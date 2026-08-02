//! A finite remote build over supervised actors and tasks.
//!
//! This is an assertion-driven application example for the task-oriented
//! modes not exercised by `trading_engine` or `agent_control`: a plain task
//! with manual readiness and supervised restart, a finite scheduler observed
//! through `TaskRef`, consuming `FnOnce` workers inserted with `spawn_once`,
//! exact-handle retirement of a wedged worker, automatic removal on terminal
//! exit, declaration-time outlines, and explicit batch completion teardown.
//!
//! ```text
//! build-farm (ordered, one-for-one)
//! ├── progress         actor, keyed latest-wins mailbox
//! ├── cas              actor, durable store shared across runs
//! ├── lease-renewer    task, readiness-gated and restarted with backoff
//! ├── workers          dynamic scope
//! │   └── <target>-attempt-<n>  finite one-shot tasks, auto-removed
//! └── scheduler        finite task; its terminal TaskRef ends the batch
//! ```
//!
//! The cold build loses one worker to a normal failure, retires one wedged
//! worker through its exact `TaskRef`, and keeps scheduling while the lease
//! task restarts. The warm build starts a fresh supervision tree over the same
//! durable CAS and performs no worker submissions.

mod actors;
mod lease;
mod messages;
mod model;
mod scheduler;
mod shared;

use std::{error::Error, sync::Arc, time::Duration};

use kokage::{
    ActorRef, ActorSpec, DynamicScopeRef, DynamicTree, Mailbox, RestartPolicy, ScopeRef, Shutdown,
    Strategy, TaskRef, TaskSpec, Tree,
    observe::{ChildOutline, ExitStatus, ScopeKind, SupervisionOutline},
};

use actors::{Cas, Progress};
use lease::Lease;
use messages::{CasMsg, Phase, ProgressMsg, StoreReport};
use model::BuildPlan;
use scheduler::Scheduler;
use shared::{AttemptBook, BuildJournal, BuildReport, CasStore, ProgressBook, TargetState};

const CALL_BOUND: Duration = Duration::from_secs(1);
const BUILD_BOUND: Duration = Duration::from_secs(10);
const PROBE_TARGET: &str = "display-probe";

type AnyError = Box<dyn Error + Send + Sync>;

struct Durable {
    store: Arc<CasStore>,
    attempts: Arc<AttemptBook>,
    plan: Arc<BuildPlan>,
}

struct Farm {
    running: kokage::RunningTree,
    root: ScopeRef,
    workers: DynamicScopeRef,
    scheduler: TaskRef,
    cas: ActorRef<CasMsg>,
    progress: ActorRef<ProgressMsg>,
    progress_book: Arc<ProgressBook>,
    journal: Arc<BuildJournal>,
    lease: Arc<Lease>,
}

struct FarmResult {
    report: BuildReport,
    store: StoreReport,
    lease_acquisitions: u64,
    lease_renewals: u64,
    progress_conflated: u64,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;

    let durable = Durable {
        store: CasStore::new(),
        attempts: AttemptBook::new(),
        plan: Arc::new(BuildPlan::demo()),
    };

    let cold = run_farm(&durable, true, true).await?;
    verify_cold(&durable, &cold);
    let warm = run_farm(&durable, false, false).await?;
    verify_warm(&durable, &warm);

    println!("PHASE 5 OK — warm rebuild reused durable state across two finite runtimes");
    Ok(())
}

async fn run_farm(
    durable: &Durable,
    fail_first_lease: bool,
    verify_declaration: bool,
) -> Result<FarmResult, AnyError> {
    let farm = assemble(durable, fail_first_lease, verify_declaration)?;
    let mut worker_snapshots = farm.workers.subscribe_snapshots();
    farm.root.wait_started().await?;
    farm.scheduler.wait_started().await?;

    // Conflation is state-shaped traffic: every target has at most one unread
    // update. The probe is deliberately not a call/control message.
    for update in 0..512 {
        farm.progress.try_send(ProgressMsg {
            target: PROBE_TARGET,
            phase: if update % 2 == 0 {
                Phase::Queued
            } else {
                Phase::Running
            },
        })?;
    }

    let terminal = tokio::time::timeout(BUILD_BOUND, farm.scheduler.wait()).await??;
    assert!(matches!(
        terminal,
        ExitStatus::Completed { cancelled: false }
    ));
    tokio::time::timeout(
        BUILD_BOUND,
        worker_snapshots.wait_for(|snapshot| snapshot.children.is_empty()),
    )
    .await??;
    tokio::time::timeout(BUILD_BOUND, async {
        while !farm.progress_book.snapshot().contains_key(PROBE_TARGET) {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let report = farm
        .journal
        .report()
        .ok_or("scheduler records a report before clean completion")?;
    let store = farm
        .cas
        .call(|reply| CasMsg::Report { reply }, CALL_BOUND)
        .await?;
    let progress_conflated = farm.progress.stats().messages_conflated;
    assert!(progress_conflated > 0);
    assert!(farm.workers.snapshot().children.is_empty());

    let result = FarmResult {
        report,
        store,
        lease_acquisitions: farm.lease.acquisitions(),
        lease_renewals: farm.lease.renewals(),
        progress_conflated,
    };
    farm.running.shutdown().await?;
    Ok(result)
}

fn assemble(
    durable: &Durable,
    fail_first_lease: bool,
    verify_declaration: bool,
) -> Result<Farm, AnyError> {
    let progress_book = ProgressBook::new();
    let journal = BuildJournal::new();
    let lease = Lease::new();

    let mut tree = Tree::new()
        .strategy(Strategy::OneForOne)
        .default_actor_mailbox_capacity(32);
    let progress = tree.add_actor_spec(
        ActorSpec::new("progress", {
            let progress_book = Arc::clone(&progress_book);
            move || Progress::new(Arc::clone(&progress_book))
        })
        .mailbox(Mailbox::latest_by_key(32, |message: &ProgressMsg| {
            message.target
        })),
    );
    let cas = tree.add_actor("cas", {
        let store = Arc::clone(&durable.store);
        move || Cas::new(Arc::clone(&store))
    });
    tree.add_task_spec(lease::renewer(Arc::clone(&lease), fail_first_lease));

    let workers_tree = DynamicTree::new()
        .default_child_shutdown(Shutdown::graceful_for(Duration::from_millis(250)));
    let workers = workers_tree.scope();
    tree.add_subtree("workers", workers_tree);

    let scheduler = tree.add_task_spec(
        TaskSpec::new("scheduler", {
            let plan = Arc::clone(&durable.plan);
            let cas = cas.clone();
            let progress = progress.clone();
            let workers = workers.clone();
            let lease = Arc::clone(&lease);
            let attempts = Arc::clone(&durable.attempts);
            let journal = Arc::clone(&journal);
            move |_| {
                scheduler::run(Scheduler {
                    plan: Arc::clone(&plan),
                    cas: cas.clone(),
                    progress: progress.clone(),
                    workers: workers.clone(),
                    lease: Arc::clone(&lease),
                    attempts: Arc::clone(&attempts),
                    journal: Arc::clone(&journal),
                })
            }
        })
        .restart(RestartPolicy::never()),
    );

    if verify_declaration {
        verify_outline(&tree.outline())?;
    }
    let running = tree.spawn()?;
    let root = running.scope();
    Ok(Farm {
        running,
        root,
        workers,
        scheduler,
        cas,
        progress,
        progress_book,
        journal,
        lease,
    })
}

fn verify_outline(outline: &SupervisionOutline) -> Result<(), AnyError> {
    assert_eq!(
        outline.child_ids(),
        ["progress", "cas", "lease-renewer", "workers", "scheduler"]
    );
    assert!(matches!(
        outline.child("lease-renewer"),
        Some(ChildOutline::Task { restart, .. }) if !matches!(restart, RestartPolicy::Never)
    ));
    assert!(matches!(
        outline.child("workers"),
        Some(ChildOutline::Scope { outline, .. })
            if outline.kind == ScopeKind::Dynamic
                && outline.default_child_shutdown
                    == Shutdown::graceful_for(Duration::from_millis(250))
    ));
    assert!(matches!(
        outline.child("scheduler"),
        Some(ChildOutline::Task {
            restart: RestartPolicy::Never,
            ..
        })
    ));
    let encoded = serde_json::to_string(outline)?;
    let decoded: SupervisionOutline = serde_json::from_str(&encoded)?;
    assert_eq!(*outline, decoded);
    println!("PHASE 0 OK — mixed actor/task topology validated and serialized before spawn");
    Ok(())
}

fn verify_cold(durable: &Durable, cold: &FarmResult) {
    assert_complete(&cold.report, false);
    assert_eq!(cold.report.failed_attempts, 1, "{:?}", cold.report);
    assert_eq!(cold.report.retired_workers, 1, "{:?}", cold.report);
    assert!(cold.report.peak_workers >= 3, "{:?}", cold.report);
    assert!(cold.report.lease_waits > 0, "{:?}", cold.report);
    assert_eq!(cold.lease_acquisitions, 2);
    assert!(cold.lease_renewals >= 1);
    assert_eq!(cold.store.entries, durable.plan.actions().len());
    assert_eq!(durable.attempts.snapshot().get("network"), Some(&2));
    assert_eq!(durable.attempts.snapshot().get("docs"), Some(&2));
    println!("PHASE 1 OK — cold build recovered one failure and retired one wedged TaskRef");
    println!(
        "PHASE 2 OK — lease task restarted with backoff while {} worker tasks ran concurrently",
        cold.report.peak_workers
    );
    println!(
        "PHASE 3 OK — latest-wins progress replaced {} unread updates",
        cold.progress_conflated
    );
}

fn verify_warm(durable: &Durable, warm: &FarmResult) {
    assert_complete(&warm.report, true);
    assert_eq!(warm.report.submissions, 0);
    assert_eq!(warm.report.cache_hits, durable.plan.actions().len() as u64);
    assert_eq!(warm.report.failed_attempts, 0);
    assert_eq!(warm.report.retired_workers, 0);
    assert_eq!(warm.lease_acquisitions, 1);
    assert_eq!(warm.store.entries, durable.plan.actions().len());
    assert!(warm.store.hits >= durable.plan.actions().len() as u64);
    assert_eq!(durable.attempts.snapshot().get("network"), Some(&2));
    assert_eq!(durable.attempts.snapshot().get("docs"), Some(&2));
    println!("PHASE 4 OK — TaskRef completion drove explicit finite-tree teardown");
}

fn assert_complete(report: &BuildReport, cached: bool) {
    assert!(report.complete, "{report:?}");
    assert!(
        report
            .targets
            .values()
            .all(|state| matches!(state, TargetState::Built { cached: hit, .. } if *hit == cached))
    );
}
