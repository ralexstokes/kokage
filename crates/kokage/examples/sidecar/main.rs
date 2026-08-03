//! # Embedded sidecar
//!
//! `sidecar` is an assertion-driven acceptance script for embedding Kokage in
//! a host process that owns `main`, the Tokio runtime, initialization, and
//! teardown. Its task-first root contains four plain supervised services and
//! one actor subtree as siblings:
//!
//! ```text
//! sidecar (ordered)
//! ├── config-watcher       plain task, cooperative
//! ├── cache-refresher      plain task, cooperative
//! ├── log-rotator          plain task, immediate abort
//! ├── actor-services       nested actor subtree
//! │   └── audit            typed actor/mailbox
//! └── health-prober        plain task, strict bounded cooperation
//! ```
//!
//! The script deliberately fails the last ordered child once. The host first
//! observes that the declaration-ordered prefix remains live, then explicitly
//! rolls it back. It subsequently completes two successful embed/run/stop
//! cycles with ordinary host work between them, proving that the library
//! composes with a process it does not own.
//!
//! Shutdown assertions require the log rotator's immediate
//! [`Shutdown::abort`] classification. From the host's shutdown boundary they
//! also prove that the stubborn health prober reaches its configured
//! [`Shutdown::graceful_for`] bound before the timeout error and
//! `after_grace: true` exit classification. The prober's own events establish
//! cancellation-before-escalation ordering without treating its
//! scheduler-dependent wake time as the start of the grace period. Host
//! teardown begins only after the re-embedded sidecar has stopped.
//!
//! Run the acceptance script from the repository root:
//!
//! ```sh
//! ./scripts/dev cargo run --locked -p kokage --example sidecar
//! ```

use std::{
    error::Error,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use kokage::{
    Actor, ActorRef, ActorSpec, BoxError, Context, ExitResult, RestartPolicy, RunningTree,
    ScopeRef, Shutdown, StopContext, SubtreeSpec, SupervisorError, TaskContext, TaskSpec, Tree,
    observe::{ChildStateView, ExitStatus, SupervisorStateView},
};

const START_BOUND: Duration = Duration::from_secs(2);
const STOP_BOUND: Duration = Duration::from_secs(2);
const PROBER_GRACE: Duration = Duration::from_millis(40);

type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Default)]
struct Journal(Arc<Mutex<Vec<String>>>);

impl Journal {
    fn record(&self, event: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.into());
    }

    fn events(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn position(&self, event: &str) -> usize {
        self.events()
            .iter()
            .position(|candidate| candidate == event)
            .unwrap_or_else(|| panic!("missing `{event}` in journal: {:?}", self.events()))
    }
}

struct AbortWitness {
    epoch: u8,
    journal: Journal,
}

impl Drop for AbortWitness {
    fn drop(&mut self) {
        self.journal.record(event(self.epoch, "rotator:aborted"));
    }
}

#[derive(Clone)]
struct AuditActor {
    epoch: u8,
    journal: Journal,
    reports: Arc<AtomicUsize>,
}

impl Actor for AuditActor {
    type Msg = &'static str;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.journal
            .record(event(self.epoch, "audit-actor:started"));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        assert_eq!(message, "host-health-report");
        self.reports.fetch_add(1, Ordering::SeqCst);
        self.journal
            .record(event(self.epoch, "audit-actor:reported"));
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.journal
            .record(event(self.epoch, "audit-actor:stopped"));
        Ok(())
    }
}

struct EmbeddedSidecar {
    epoch: u8,
    running: RunningTree,
    scope: ScopeRef,
    audit: ActorRef<&'static str>,
    reports: Arc<AtomicUsize>,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let journal = Journal::default();

    journal.record("host:init");
    verify_failed_startup_rolls_back(journal.clone()).await?;

    journal.record("host:initialized");
    let first = embed(1, journal.clone()).await?;
    journal.record("host:started");
    exercise(&first).await?;
    stop(first, &journal).await?;

    journal.record("host:sidecar-detached");
    journal.record("host:foreground-work-without-sidecar");

    let second = embed(2, journal.clone()).await?;
    journal.record("host:sidecar-re-embedded");
    exercise(&second).await?;
    stop(second, &journal).await?;

    verify_host_ownership(&journal);
    journal.record("host:teardown");
    assert!(journal.position("2:sidecar:stopped") < journal.position("host:teardown"));

    println!("PHASE 4 OK — host teardown began only after the re-embedded sidecar stopped");
    Ok(())
}

async fn verify_failed_startup_rolls_back(journal: Journal) -> Result<(), AnyError> {
    let failed = assemble(0, journal.clone(), true)?;
    let failed_scope = failed.scope.clone();

    let startup = tokio::time::timeout(START_BOUND, failed_scope.wait_started()).await?;
    assert!(matches!(startup, Err(SupervisorError::StartupAborted(_))));

    let start_order = [
        event(0, "config-watcher:started"),
        event(0, "cache-refresher:started"),
        event(0, "rotator:started"),
        event(0, "audit-actor:started"),
        event(0, "health-prober:start-failed"),
    ];
    for adjacent in start_order.windows(2) {
        assert!(
            journal.position(&adjacent[0]) < journal.position(&adjacent[1]),
            "ordered startup must hand readiness from `{}` to `{}`",
            adjacent[0],
            adjacent[1]
        );
    }

    let terminal_prefix_events = [
        event(0, "config-watcher:stopped"),
        event(0, "cache-refresher:stopped"),
        event(0, "rotator:aborted"),
        event(0, "audit-actor:stopped"),
    ];
    let before_rollback = journal.events();
    assert!(!before_rollback.contains(&event(0, "health-prober:started")));
    assert!(
        terminal_prefix_events
            .iter()
            .all(|terminal| !before_rollback.contains(terminal))
    );

    let snapshot = failed_scope.snapshot();
    assert_eq!(snapshot.state, SupervisorStateView::Running);
    for id in ["config-watcher", "cache-refresher", "log-rotator"] {
        let child = snapshot.child(id).expect("started prefix remains visible");
        assert!(
            child.state.is_running(),
            "`{id}` must remain running until the host requests rollback: {child:?}"
        );
    }
    let actor_services = snapshot
        .child("actor-services")
        .expect("started actor subtree remains visible");
    assert!(actor_services.state.is_running());
    let actor_scope = failed_scope
        .subtree("actor-services")
        .expect("started actor subtree retains its stable scope handle");
    let actors = actor_scope.snapshot();
    assert_eq!(actors.state, SupervisorStateView::Running);
    assert!(
        actors
            .child("audit")
            .is_some_and(|audit| audit.state.is_running()),
        "audit actor must remain running until explicit host rollback: {actors:?}"
    );

    let failed_prober = snapshot
        .child("health-prober")
        .expect("failed prober remains observable");
    assert!(matches!(
        &failed_prober.state,
        ChildStateView::StartupAborted {
            exit: ExitStatus::Failed { message, cancelled: false },
        }
            if message == "scripted health initialization failure"
    ));

    let rollback_requested = event(0, "sidecar:rollback-requested");
    journal.record(rollback_requested.clone());
    tokio::time::timeout(STOP_BOUND, failed.running.shutdown()).await??;
    let rolled_back = event(0, "sidecar:rolled-back");
    journal.record(rolled_back.clone());

    for terminal in &terminal_prefix_events {
        assert!(journal.position(&rollback_requested) < journal.position(terminal));
        assert!(journal.position(terminal) < journal.position(&rolled_back));
    }

    let snapshot = failed_scope.snapshot();
    assert_eq!(snapshot.state, SupervisorStateView::Stopped);
    for id in [
        "config-watcher",
        "cache-refresher",
        "log-rotator",
        "actor-services",
    ] {
        let child = snapshot
            .child(id)
            .expect("rolled-back prefix remains visible");
        assert!(
            matches!(&child.state, ChildStateView::Stopped { started: true, .. }),
            "`{id}` must become terminal during explicit host rollback: {child:?}"
        );
    }
    let actors = actor_scope.snapshot();
    assert_eq!(actors.state, SupervisorStateView::Stopped);
    assert!(matches!(
        actors.child("audit").map(|audit| &audit.state),
        Some(ChildStateView::Stopped { started: true, .. })
    ));

    println!("PHASE 0 OK — ordered startup failure rolled back every started sibling");
    Ok(())
}

async fn embed(epoch: u8, journal: Journal) -> Result<EmbeddedSidecar, AnyError> {
    journal.record(event(epoch, "sidecar:embedding"));
    let sidecar = assemble(epoch, journal.clone(), false)?;
    tokio::time::timeout(START_BOUND, sidecar.scope.wait_started()).await??;
    journal.record(event(epoch, "sidecar:started"));

    let root = sidecar.scope.snapshot();
    assert_eq!(
        root.children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        [
            "config-watcher",
            "cache-refresher",
            "log-rotator",
            "actor-services",
            "health-prober",
        ]
    );
    let actor_services = root
        .child("actor-services")
        .and_then(|child| child.supervisor.as_deref())
        .expect("one actor subtree is a sibling of the raw tasks");
    assert_eq!(
        actor_services
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["audit"]
    );

    println!(
        "PHASE {} OK — host embedded a task-first root with one actor subtree",
        epoch
    );
    Ok(sidecar)
}

fn assemble(
    epoch: u8,
    journal: Journal,
    fail_startup: bool,
) -> Result<EmbeddedSidecar, kokage::BuildError> {
    let reports = Arc::new(AtomicUsize::new(0));
    let mut actors = Tree::new().default_child_restart(RestartPolicy::never());
    let audit = actors.add_actor_spec(ActorSpec::new("audit", {
        let journal = journal.clone();
        let reports = Arc::clone(&reports);
        move || AuditActor {
            epoch,
            journal: journal.clone(),
            reports: Arc::clone(&reports),
        }
    }));

    let mut root = Tree::new().default_child_restart(RestartPolicy::never());
    root.add_task_spec(cooperative_task(epoch, "config-watcher", journal.clone()));
    root.add_task_spec(cooperative_task(epoch, "cache-refresher", journal.clone()));
    root.add_task_spec(rotator(epoch, journal.clone()));
    root.add_subtree_spec(
        "actor-services",
        SubtreeSpec::from(actors).restart(RestartPolicy::never()),
    );
    root.add_task_spec(prober(epoch, journal.clone(), fail_startup));

    let running = root.spawn()?;
    let scope = running.scope();
    Ok(EmbeddedSidecar {
        epoch,
        running,
        scope,
        audit,
        reports,
    })
}

fn cooperative_task(epoch: u8, name: &'static str, journal: Journal) -> TaskSpec {
    TaskSpec::new(name, move |ctx| {
        let journal = journal.clone();
        async move {
            journal.record(event(epoch, &format!("{name}:started")));
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            journal.record(event(epoch, &format!("{name}:stopped")));
            Ok(())
        }
    })
    .restart(RestartPolicy::never())
    .shutdown(Shutdown::graceful_for(Duration::from_millis(200)))
    .manual_readiness(START_BOUND)
}

fn rotator(epoch: u8, journal: Journal) -> TaskSpec {
    TaskSpec::new("log-rotator", move |ctx| {
        let journal = journal.clone();
        async move {
            let _witness = AbortWitness {
                epoch,
                journal: journal.clone(),
            };
            journal.record(event(epoch, "rotator:started"));
            ctx.mark_ready();
            std::future::pending::<()>().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::never())
    .shutdown(Shutdown::abort())
    .manual_readiness(START_BOUND)
}

fn prober(epoch: u8, journal: Journal, fail_startup: bool) -> TaskSpec {
    TaskSpec::new("health-prober", move |ctx: TaskContext| {
        let journal = journal.clone();
        async move {
            if fail_startup {
                journal.record(event(epoch, "health-prober:start-failed"));
                return Err(std::io::Error::other("scripted health initialization failure").into());
            }

            journal.record(event(epoch, "health-prober:started"));
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            journal.record(event(epoch, "health-prober:shutdown-requested"));

            ctx.abort_token().cancelled().await;
            journal.record(event(epoch, "health-prober:grace-expired"));
            Ok(())
        }
    })
    .restart(RestartPolicy::never())
    .shutdown(Shutdown::graceful_for(PROBER_GRACE))
    .manual_readiness(START_BOUND)
}

async fn exercise(sidecar: &EmbeddedSidecar) -> Result<(), AnyError> {
    sidecar.audit.send("host-health-report").await?;
    tokio::time::timeout(START_BOUND, async {
        while sidecar.reports.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(sidecar.reports.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn stop(sidecar: EmbeddedSidecar, journal: &Journal) -> Result<(), AnyError> {
    let epoch = sidecar.epoch;
    let shutdown_started = Instant::now();
    let shutdown = tokio::time::timeout(STOP_BOUND, sidecar.running.shutdown()).await?;
    let shutdown_elapsed = shutdown_started.elapsed();
    assert!(matches!(
        shutdown,
        Err(SupervisorError::ShutdownTimedOut(ref child)) if child == "health-prober"
    ));
    assert!(
        shutdown_elapsed >= PROBER_GRACE,
        "host-side shutdown must reach the stubborn prober's configured bound: {shutdown_elapsed:?}"
    );

    let snapshot = sidecar.scope.snapshot();
    let prober_exit = snapshot
        .child("health-prober")
        .and_then(|child| child.state.last_exit())
        .expect("strict prober exit remains observable");
    assert!(matches!(
        prober_exit,
        ExitStatus::Aborted {
            after_grace: true,
            cancelled: true,
        }
    ));
    let rotator_exit = snapshot
        .child("log-rotator")
        .and_then(|child| child.state.last_exit())
        .expect("abort-mode rotator exit remains observable");
    assert!(matches!(
        rotator_exit,
        ExitStatus::Aborted {
            after_grace: false,
            cancelled: true,
        }
    ));

    journal.record(event(epoch, "sidecar:stopped"));
    assert!(
        journal.position(&event(epoch, "health-prober:shutdown-requested"))
            < journal.position(&event(epoch, "health-prober:grace-expired"))
    );
    assert!(
        journal.position(&event(epoch, "audit-actor:stopped"))
            < journal.position(&event(epoch, "sidecar:stopped"))
    );
    assert!(
        journal.position(&event(epoch, "rotator:aborted"))
            < journal.position(&event(epoch, "sidecar:stopped"))
    );

    println!(
        "PHASE {} STOP OK — abort was immediate and strict cooperative grace expired",
        epoch
    );
    Ok(())
}

fn verify_host_ownership(journal: &Journal) {
    assert!(journal.position("host:init") < journal.position("0:config-watcher:started"));
    assert!(journal.position("0:sidecar:rolled-back") < journal.position("host:initialized"));
    assert!(journal.position("host:initialized") < journal.position("1:sidecar:started"));
    assert!(journal.position("1:sidecar:started") < journal.position("host:started"));
    assert!(journal.position("1:sidecar:stopped") < journal.position("host:sidecar-detached"));
    assert!(
        journal.position("host:sidecar-detached")
            < journal.position("host:foreground-work-without-sidecar")
    );
    assert!(
        journal.position("host:foreground-work-without-sidecar")
            < journal.position("2:sidecar:embedding")
    );
    assert!(journal.position("2:sidecar:started") < journal.position("2:sidecar:stopped"));
    assert!(journal.position("1:audit-actor:reported") < journal.position("1:audit-actor:stopped"));
    assert!(journal.position("2:audit-actor:reported") < journal.position("2:audit-actor:stopped"));
}

fn event(epoch: u8, detail: &str) -> String {
    format!("{epoch}:{detail}")
}
