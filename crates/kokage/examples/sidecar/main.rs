//! A host-owned process that embeds Kokage around its background machinery.
//!
//! The host owns `main`, initialization, teardown, and the Tokio runtime. Its
//! sidecar root is task-first: config watching, cache refresh, health probing,
//! and log rotation are plain supervised futures with no actor runtime or
//! mailbox. One small actor subtree sits beside those tasks to prove that an
//! application can mix both execution models without making the host itself
//! an actor system.
//!
//! The executable is an assertion-driven acceptance script. It first rolls
//! back a deliberately failed ordered startup, then embeds, detaches, and
//! re-embeds the sidecar while the host remains alive. Shutdown proves both
//! immediate abort and strict grace-expiry behavior.

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
    observe::ExitStatus,
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

#[derive(Clone, Default)]
struct GraceObservations(Arc<Mutex<Vec<Duration>>>);

impl GraceObservations {
    fn push(&self, elapsed: Duration) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(elapsed);
    }

    fn values(&self) -> Vec<Duration> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
    let grace = GraceObservations::default();

    journal.record("host:init");
    verify_failed_startup_rolls_back(journal.clone()).await?;

    journal.record("host:initialized");
    let first = embed(1, journal.clone(), grace.clone()).await?;
    journal.record("host:started");
    exercise(&first).await?;
    stop(first, &journal).await?;

    journal.record("host:sidecar-detached");
    journal.record("host:foreground-work-without-sidecar");

    let second = embed(2, journal.clone(), grace.clone()).await?;
    journal.record("host:sidecar-re-embedded");
    exercise(&second).await?;
    stop(second, &journal).await?;

    verify_host_ownership(&journal, &grace);
    journal.record("host:teardown");
    assert!(journal.position("2:sidecar:stopped") < journal.position("host:teardown"));

    println!("PHASE 4 OK — host teardown began only after the re-embedded sidecar stopped");
    Ok(())
}

async fn verify_failed_startup_rolls_back(journal: Journal) -> Result<(), AnyError> {
    let grace = GraceObservations::default();
    let failed = assemble(0, journal.clone(), grace, true)?;

    let startup = tokio::time::timeout(START_BOUND, failed.scope.wait_started()).await?;
    assert!(matches!(startup, Err(SupervisorError::StartupAborted(_))));

    let snapshot = failed.scope.snapshot();
    let failed_prober = snapshot
        .child("health-prober")
        .expect("failed prober remains observable");
    assert!(matches!(
        failed_prober.state.last_exit(),
        Some(ExitStatus::Failed { message, cancelled: false })
            if message == "scripted health initialization failure"
    ));

    tokio::time::timeout(STOP_BOUND, failed.running.shutdown()).await??;
    journal.record(event(0, "sidecar:rolled-back"));

    let events = journal.events();
    assert!(events.contains(&event(0, "config-watcher:started")));
    assert!(events.contains(&event(0, "cache-refresher:started")));
    assert!(events.contains(&event(0, "audit-actor:started")));
    assert!(events.contains(&event(0, "health-prober:start-failed")));
    assert!(events.contains(&event(0, "config-watcher:stopped")));
    assert!(events.contains(&event(0, "cache-refresher:stopped")));
    assert!(events.contains(&event(0, "audit-actor:stopped")));
    assert!(events.contains(&event(0, "rotator:aborted")));

    println!("PHASE 0 OK — ordered startup failure rolled back every started sibling");
    Ok(())
}

async fn embed(
    epoch: u8,
    journal: Journal,
    grace: GraceObservations,
) -> Result<EmbeddedSidecar, AnyError> {
    journal.record(event(epoch, "sidecar:embedding"));
    let sidecar = assemble(epoch, journal.clone(), grace, false)?;
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
    grace: GraceObservations,
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
    root.add_task_spec(prober(epoch, journal.clone(), grace, fail_startup));

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

fn prober(epoch: u8, journal: Journal, grace: GraceObservations, fail_startup: bool) -> TaskSpec {
    TaskSpec::new("health-prober", move |ctx: TaskContext| {
        let journal = journal.clone();
        let grace = grace.clone();
        async move {
            if fail_startup {
                journal.record(event(epoch, "health-prober:start-failed"));
                return Err(std::io::Error::other("scripted health initialization failure").into());
            }

            journal.record(event(epoch, "health-prober:started"));
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            journal.record(event(epoch, "health-prober:shutdown-requested"));

            let grace_started = Instant::now();
            ctx.abort_token().cancelled().await;
            let elapsed = grace_started.elapsed();
            grace.push(elapsed);
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
    let shutdown = tokio::time::timeout(STOP_BOUND, sidecar.running.shutdown()).await?;
    assert!(matches!(
        shutdown,
        Err(SupervisorError::ShutdownTimedOut(ref child)) if child == "health-prober"
    ));

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

fn verify_host_ownership(journal: &Journal, grace: &GraceObservations) {
    let observed = grace.values();
    assert_eq!(observed.len(), 2);
    assert!(
        observed.iter().all(|elapsed| *elapsed >= PROBER_GRACE),
        "strict cooperative shutdown must preserve each configured grace: {observed:?}"
    );

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
