//! A remote build farm: a supervised actor graph whose lifetime is bounded by
//! the work it was asked to do.
//!
//! The two other application examples in this crate model long-running
//! services. This one models a *batch*. A build is submitted, executed across a
//! pool of workers that is sized at runtime, and then the farm stops on its own
//! — the scheduler's clean exit is what shuts the runtime down. That inverts
//! several defaults, and most of what is interesting here follows from it.
//!
//! # Modules
//!
//! * `plan` — the build graph, content addressing, and the simulated compile
//!   (real CPU on the blocking pool, with scripted stalls and crashes).
//! * `messages` — one message enum per actor, plus the report types the
//!   acceptance script asserts against.
//! * `shared` — state that outlives an incarnation: the attempt log that bounds
//!   retries, the artifact store that outlives the whole runtime, and the
//!   journal actors write their closing summaries to.
//! * `cas` — the content-addressed store, whose `CasFactory` is derived.
//! * `progress` — the display: a keyed conflating mailbox that absorbs redraw
//!   storms.
//! * `worker` — the executor. `#[derive(ActorFactory)]` names the configuration
//!   the pool clones into every worker it spawns.
//! * `pool` — the pool leader. It owns a dynamic scope through
//!   [`MessageContext::children`](tokio_otp::MessageContext::children) and pipelines
//!   every effect off its handle loop.
//! * `scheduler` — the frontier walk, and the child whose completion ends the
//!   run.
//! * `lease` — a supervised child that is deliberately not an actor.
//!
//! # Topology
//!
//! ```text
//! build-farm (ordered, one-for-one)
//! ├── progress        keyed conflating mailbox
//! ├── cas             durable store behind a derived factory
//! ├── lease-renewer   plain ChildSpec, wait_for_ready, restart Always
//! ├── build-pool      ActorWithScope, one-for-all
//! │   ├── pool        leader
//! │   └── children    dynamic scope: worker-0, worker-1, ... (one-for-one)
//! └── scheduler       restart OnFailure — its completion stops the farm
//! ```
//!
//! # Data flow
//!
//! ```text
//!                      lease (Arc, no mailbox)
//!                          |
//!                          v
//!   +-----------+  Submit  +------+ offload: Execute +--------+
//!   | Scheduler |--------->| Pool |---------------->| Worker |
//!   +-----------+          +------+                 +--------+
//!         ^                    |                      |    |
//!         | offload: Finished  |    add/remove_actor   |    | Lookup / Store
//!         +--------------------+   (owned dynamic      |    v
//!                                   scope)             |  +-----+
//!                                                      |  | Cas |
//!                                       Update         |  +-----+
//!                                  +-------------------+
//!                                  v
//!                             +----------+
//!                             | Progress |
//!                             +----------+
//! ```
//!
//! The scheduler → pool edge is an awaited send, so a full queue applies
//! backpressure. Every pool → anything edge is pipelined through
//! [`LiveContext::offload`](tokio_otp::LiveContext::offload), which is what
//! keeps the pair from deadlocking on each other's bounded mailboxes and what
//! keeps one stalled compile from freezing the pool.
//!
//! `main` runs phases 0–6: the declared shape before anything runs, readiness,
//! runtime pool growth, display conflation, completion-driven shutdown of a
//! cold build with a poison target and a wedged worker, a warm rebuild that is
//! a pure cache hit, and the durable evidence both runs left behind.

mod cas;
mod lease;
mod messages;
mod plan;
mod pool;
mod progress;
mod scheduler;
mod shared;
mod worker;

use std::{
    collections::BTreeMap,
    error::Error,
    future::Future,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use tokio_otp::{
    ActorSpec, ActorStats, ChildOutline, CompletionGuard, Graph, RunnableActor, Runtime,
    SupervisionOutline, SupervisionTree, prelude::*,
};

use cas::CasFactory;
use lease::{LEASE_CHILD_ID, Lease};
use messages::{
    BuildStatus, CALL_DEADLINE, CasMsg, CasSnapshot, Phase, PoolMsg, PoolReport, ProgressMsg,
    ProgressStats, SchedulerMsg, TargetProgress, TargetState,
};
use plan::{BuildPlan, TargetId};
use pool::{PoolLimits, PoolManager};
use progress::{Progress, progress_key};
use scheduler::Scheduler;
use shared::{AttemptLog, BuildJournal, CasStore};
use worker::WorkerFactory;

type AnyError = Box<dyn Error + Send + Sync>;

const POOL_ID: &str = "build-pool";
const POOL_LEADER_ID: &str = "pool";
/// The id the lowering gives a leader's owned scope inside its node.
const POOL_SCOPE_ID: &str = "children";
const SCHEDULER_ID: &str = "scheduler";
const PROGRESS_ID: &str = "progress";
const CAS_ID: &str = "cas";

const LIMITS: PoolLimits = PoolLimits { min: 1, max: 4 };
const MAX_ATTEMPTS: u32 = 2;
const LEASE_PERIOD: Duration = Duration::from_millis(25);
/// Renewals the first lease incarnation performs before its scripted failure.
const LEASE_DROP_AFTER: u64 = 2;
/// A target key used only to saturate the display; never part of the plan.
const DISPLAY_PROBE: TargetId = "display-probe";

const PHASE_TIMEOUT: Duration = Duration::from_secs(10);
const BUILD_TIMEOUT: Duration = Duration::from_secs(30);

/// State the farm keeps between runs, standing in for the parts of a build
/// service that live on disk rather than in the process.
struct Durable {
    store: Arc<CasStore>,
    attempts: Arc<AttemptLog>,
    labels: Arc<AtomicU64>,
    plan: Arc<BuildPlan>,
}

/// Typed refs into one assembled farm.
struct Refs {
    scheduler: ActorRef<SchedulerMsg>,
    pool: ActorRef<PoolMsg>,
    progress: ActorRef<ProgressMsg>,
    cas: ActorRef<CasMsg>,
}

/// An assembled but not yet running farm.
struct Blueprint {
    runtime: Runtime,
    outline: SupervisionOutline,
    refs: Refs,
    journal: Arc<BuildJournal>,
    lease: Arc<Lease>,
}

/// A running farm.
struct Farm {
    handle: RuntimeHandle,
    refs: Refs,
    journal: Arc<BuildJournal>,
    lease: Arc<Lease>,
    /// Retained deliberately: dropping it cancels the completion watch and the
    /// build would run to the end and then sit there.
    _finished: CompletionGuard,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    // The poison target's panic is scripted, so the supervisor's ERROR-level
    // report of it is expected output rather than a problem.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;

    let durable = Durable {
        store: CasStore::new(),
        attempts: AttemptLog::new(MAX_ATTEMPTS),
        labels: Arc::new(AtomicU64::new(0)),
        plan: Arc::new(BuildPlan::demo()),
    };

    let cold = assemble(&durable, LEASE_DROP_AFTER)?;
    phase_0(&cold)?;
    let cold = spawn(cold);
    phase_1(&cold).await?;
    phase_2(&cold).await?;
    phase_3(&cold).await?;
    phase_4(&cold).await?;

    let warm = spawn(assemble(&durable, u64::MAX)?);
    phase_5(&warm).await?;
    phase_6(&durable, &cold, &warm)?;
    Ok(())
}

/// Builds the graph, places its actors in a supervision tree, and arms the
/// completion watch.
fn assemble(durable: &Durable, lease_drop_after: u64) -> Result<Blueprint, AnyError> {
    let journal = BuildJournal::new();
    let lease = Lease::new();

    let mut builder = GraphBuilder::new();
    builder.name("build-farm");
    builder.mailbox_capacity(32);

    let progress = builder.actor_with_options(
        PROGRESS_ID,
        {
            let journal = Arc::clone(&journal);
            move || Progress::new(Arc::clone(&journal))
        },
        ActorOptions::new().mailbox(MailboxMode::conflate_by_key(progress_key)),
    );
    let cas = builder.actor_with_options(
        CAS_ID,
        CasFactory {
            store: Arc::clone(&durable.store),
        },
        ActorOptions::new().message_size(),
    );
    // The scheduler submits to the pool and the pool reports back, so neither
    // ref exists before the other is declared.
    let (pool_slot, pool) = builder.slot::<PoolMsg>(POOL_LEADER_ID);
    let (scheduler_slot, scheduler) = builder.slot::<SchedulerMsg>(SCHEDULER_ID);

    let worker_factory = WorkerFactory {
        cas: cas.clone(),
        progress: progress.clone(),
        attempts: Arc::clone(&durable.attempts),
        journal: Arc::clone(&journal),
    };
    builder.define(pool_slot, {
        let scheduler = scheduler.clone();
        let progress = progress.clone();
        let journal = Arc::clone(&journal);
        let labels = Arc::clone(&durable.labels);
        move || {
            PoolManager::new(
                scheduler.clone(),
                progress.clone(),
                worker_factory.clone(),
                LIMITS,
                Arc::clone(&journal),
                Arc::clone(&labels),
            )
        }
    });
    builder.define(scheduler_slot, {
        let plan = Arc::clone(&durable.plan);
        let pool = pool.clone();
        let progress = progress.clone();
        let lease = Arc::clone(&lease);
        let journal = Arc::clone(&journal);
        move || {
            Scheduler::new(
                Arc::clone(&plan),
                pool.clone(),
                progress.clone(),
                Arc::clone(&lease),
                Arc::clone(&journal),
            )
        }
    });
    let graph = builder.build()?;

    // One graph, two tree depths: three actors at the root and the pool leader
    // one level down. `GraphBuilder` hands back `ActorRef`s for wiring, but
    // placing an actor by hand needs its `RunnableActor`, and the only way to
    // that value is a scan of `graph.actors()` by label.
    let tree = SupervisionTree::new()
        .strategy(Strategy::OneForOne)
        .actor(runnable(&graph, PROGRESS_ID))
        .actor(runnable(&graph, CAS_ID))
        .task(lease::renewer(
            Arc::clone(&lease),
            LEASE_PERIOD,
            lease_drop_after,
        ))
        .actor_with_scope_strategy(
            POOL_ID,
            // The leader holds the roster for a scope it created, so it must
            // not outlive that scope: one-for-all recycles both together.
            ActorSpec::new(runnable(&graph, POOL_LEADER_ID)).restart(RestartPolicy::Always),
            SupervisionTree::dynamic()
                .restart_intensity(RestartIntensity::new(8, Duration::from_secs(30))),
            Strategy::OneForAll,
        )
        // OTP's `transient` is load-bearing here: the scheduler's clean stop must
        // count as completion, while a crash still restarts it.
        .actor(ActorSpec::new(runnable(&graph, SCHEDULER_ID)).restart(RestartPolicy::OnFailure));

    Ok(Blueprint {
        outline: tree.outline()?,
        runtime: tree.build()?,
        refs: Refs {
            scheduler,
            pool,
            progress,
            cas,
        },
        journal,
        lease,
    })
}

/// Arms the completion watch on the pre-spawn handle, then starts the farm.
fn spawn(blueprint: Blueprint) -> Farm {
    let Blueprint {
        runtime,
        refs,
        journal,
        lease,
        ..
    } = blueprint;
    // Armed before spawning: a build that finished instantly would otherwise
    // complete before anything was watching.
    let pending = runtime.handle();
    let finished = pending.shutdown_on_completion([SCHEDULER_ID]);
    let handle = runtime.spawn();
    drop(pending);

    Farm {
        handle,
        refs,
        journal,
        lease,
        _finished: finished,
    }
}

/// The declared tree, checked before a single task exists.
fn phase_0(blueprint: &Blueprint) -> Result<(), AnyError> {
    let outline = &blueprint.outline;
    assert_eq!(
        outline.child_ids(),
        [PROGRESS_ID, CAS_ID, LEASE_CHILD_ID, POOL_ID, SCHEDULER_ID],
        "ordered startup follows declaration order"
    );
    assert!(
        matches!(
            outline.child(LEASE_CHILD_ID),
            Some(ChildOutline::Child { restart, .. }) if *restart == RestartPolicy::Always
        ),
        "the lease renewer is a plain task child, not an actor"
    );
    assert!(
        matches!(
            outline.child(SCHEDULER_ID),
            Some(ChildOutline::Actor { restart, .. }) if *restart == RestartPolicy::OnFailure
        ),
        "only an on-failure scheduler can complete"
    );
    let Some(ChildOutline::ActorWithScope {
        leader,
        children,
        strategy,
        ..
    }) = outline.child(POOL_ID)
    else {
        return Err("the pool must be declared as an actor with an owned scope".into());
    };
    assert_eq!(leader.id(), POOL_LEADER_ID);
    assert_eq!(*strategy, Strategy::OneForAll);
    assert_eq!(children.kind, ScopeKind::Dynamic);
    assert!(
        children.children.is_empty(),
        "workers are membership written at runtime, not declaration"
    );
    println!("PHASE 0 OK — declared shape validated before spawn");
    Ok(())
}

/// Readiness, and the ordering guarantee the lease depends on.
async fn phase_1(farm: &Farm) -> Result<(), AnyError> {
    tokio::time::timeout(PHASE_TIMEOUT, farm.handle.wait_started()).await??;
    assert!(
        farm.lease.is_held(),
        "the renewer is declared before the scheduler and waits for ready, so \
         the lease is taken before the first dispatch"
    );
    let snapshot = farm.handle.snapshot();
    for id in [PROGRESS_ID, CAS_ID, LEASE_CHILD_ID, POOL_ID, SCHEDULER_ID] {
        assert!(snapshot.child(id).is_some(), "{id} is a running child");
    }
    // An actor-with-scope node lowers to a nested supervisor holding the
    // leader and a scope literally named `children`. That generated id is not
    // something the builder API names, so reaching the worker scope from
    // outside means knowing the lowering.
    let pool_scope = farm
        .handle
        .subtree(POOL_ID)
        .ok_or("the pool node is a runtime subtree")?;
    let workers = pool_scope
        .subtree(POOL_SCOPE_ID)
        .ok_or("the leader's owned scope is nested one level further")?;
    assert_eq!(workers.snapshot().strategy, Strategy::OneForOne);
    assert_eq!(
        pool_scope.snapshot().strategy,
        Strategy::OneForAll,
        "the leader and its scope share fate"
    );
    println!("PHASE 1 OK — readiness-gated startup with the lease already held");
    Ok(())
}

/// The pool writing membership into the scope it owns.
async fn phase_2(farm: &Farm) -> Result<(), AnyError> {
    // `peak_workers`, not the live roster: the pool retires idle workers as
    // soon as its queue drains, so the moment it was at full width is easy to
    // poll straight past.
    await_until(|| async {
        pool_report(farm)
            .await
            .is_some_and(|report| report.peak_workers >= 2)
    })
    .await?;
    let report = pool_report(farm).await.ok_or("the pool is a live actor")?;
    let workers: Vec<String> = farm
        .handle
        .actor_stats()
        .into_iter()
        .filter(|stats| stats.actor_id.starts_with("worker-"))
        .map(|stats| stats.actor_id)
        .collect();
    assert!(
        !workers.is_empty(),
        "dynamically added workers appear in recursive actor stats: {workers:?}"
    );

    // Live interrogation, while the build is still running. Everything after
    // phase 4 has to come from the journal instead, because completion takes
    // the actors with it.
    let status: BuildStatus = farm
        .refs
        .scheduler
        .call(CALL_DEADLINE, |reply| SchedulerMsg::Status { reply })
        .await?;
    assert!(
        status.submitted >= 1,
        "the frontier walk started without prompting: {status:?}"
    );
    let cas: CasSnapshot = farm
        .refs
        .cas
        .call(CALL_DEADLINE, |reply| CasMsg::Report { reply })
        .await?;
    assert!(
        cas.served_by_incarnation >= 1 && cas.store.misses >= 1,
        "a cold store is all misses: {cas:?}"
    );
    println!(
        "PHASE 2 OK — pool grew its owned scope to {} workers ({} live) with {} actions submitted",
        report.peak_workers,
        workers.len(),
        status.submitted
    );
    Ok(())
}

/// A redraw storm collapsing in the display's keyed conflating mailbox.
async fn phase_3(farm: &Farm) -> Result<(), AnyError> {
    const BURST: u16 = 256;

    await_until(|| async {
        // A tight synchronous burst on one key: `try_send` never waits for
        // capacity on a conflating mailbox, so everything the actor has not
        // yet taken is replaced rather than queued or rejected.
        for percent in 0..BURST {
            let _ = farm
                .refs
                .progress
                .try_send(ProgressMsg::Update(TargetProgress {
                    target: DISPLAY_PROBE,
                    phase: Phase::Running((percent % 101) as u8),
                }));
        }
        mailbox(farm, PROGRESS_ID).is_some_and(|stats| stats.messages_conflated > 0)
    })
    .await?;

    let mailbox = mailbox(farm, PROGRESS_ID).ok_or("the display is a live actor")?;
    assert_eq!(
        mailbox.sends_rejected, 0,
        "a conflating mailbox replaces instead of rejecting, however deep the burst"
    );
    assert!(
        mailbox.messages_conflated > 0,
        "updates the display had not read yet were replaced: {mailbox:?}"
    );

    let stats: ProgressStats = farm
        .refs
        .progress
        .call(CALL_DEADLINE, |reply| ProgressMsg::Stats { reply })
        .await?;
    // The two control keys are what let these replies through at all: an
    // unkeyed conflating mailbox would have let a progress update displace them.
    let table: BTreeMap<TargetId, Phase> = farm
        .refs
        .progress
        .call(CALL_DEADLINE, |reply| ProgressMsg::Render { reply })
        .await?;
    assert!(
        table.contains_key(DISPLAY_PROBE),
        "the burst survived as exactly one latest-wins entry: {table:?}"
    );
    println!(
        "PHASE 3 OK — display absorbed a redraw storm ({} conflated, {} applied, 0 rejected)",
        mailbox.messages_conflated, stats.applied
    );
    Ok(())
}

/// The cold build: a wedged worker, a poison target, and a farm that stops
/// itself.
async fn phase_4(farm: &Farm) -> Result<(), AnyError> {
    tokio::time::timeout(BUILD_TIMEOUT, farm.handle.wait()).await??;

    let summary = farm
        .journal
        .summary()
        .ok_or("the scheduler must have stopped cleanly and written its summary")?;
    assert!(summary.finished, "every target reached a terminal state");
    assert_eq!(
        summary.targets_in(&TargetState::Failed { attempts: 2 }),
        ["ui-lib"],
        "the poison target is failed once the shared attempt log is spent"
    );
    assert_eq!(
        summary.targets_in(&TargetState::Skipped),
        ["app-bundle"],
        "only the dependent of the failed target is skipped"
    );
    assert_eq!(built(&summary, false).len(), 7, "seven targets compiled");
    assert!(
        built(&summary, true).is_empty(),
        "a cold build has nothing to hit in the store"
    );

    let pool = farm
        .journal
        .pool()
        .ok_or("the pool leader must have written its final report")?;
    assert!(
        pool.peak_workers >= 2,
        "the pool grew past its minimum under fan-out: {pool:?}"
    );
    assert_eq!(pool.stalled, 1, "net-lib misses its dispatch deadline once");
    assert_eq!(pool.retired, 1, "the wedged worker is retired, not reused");
    assert_eq!(
        pool.lost, 2,
        "the poison target loses one dispatch per surviving attempt"
    );
    assert!(
        pool.worker_restarts >= 1,
        "poison crashes are ordinary one-for-one restarts: {pool:?}"
    );
    assert!(
        summary.submitted > u64::try_from(built(&summary, false).len())?,
        "requeued work is resubmitted, not silently dropped"
    );

    assert_eq!(
        farm.lease.acquisitions(),
        2,
        "the scripted lease failure is a supervised restart of a non-actor child"
    );
    assert!(!farm.lease.is_held(), "the lease is released at shutdown");
    assert!(
        farm.lease.renewals() >= LEASE_DROP_AFTER,
        "the first incarnation renewed before its scripted failure"
    );
    assert!(
        summary.lease_stalls >= 1,
        "the scheduler observed the restart window and declined to dispatch"
    );

    let display = farm.journal.display();
    assert_eq!(display.get("ui-lib"), Some(&Phase::Failed));
    assert_eq!(display.get("app-bundle"), Some(&Phase::Skipped));
    assert_eq!(display.get("cli-bin"), Some(&Phase::Built));

    // `on_stop` does not run after a panic, so a worker that compiled something
    // and *then* hit the poison target takes its incarnation-local counters
    // with it. That is the cost of accounting in per-incarnation state: the
    // durable store checked in phase 6 is the authority on what was compiled,
    // and these records are only a lower bound.
    let workers = farm.journal.workers();
    assert!(
        !workers.is_empty(),
        "workers that stopped cleanly wrote an exit record"
    );
    assert!(
        farm.journal.total_built() <= 7,
        "no worker can claim more compiles than the plan has targets: {workers:?}"
    );
    println!(
        "PHASE 4 OK — completion-driven shutdown after {} submissions, {} lease stalls, \
         {} worker exit records",
        summary.submitted,
        summary.lease_stalls,
        workers.len()
    );
    Ok(())
}

/// The warm rebuild: same plan, same store, no compiler runs at all.
async fn phase_5(farm: &Farm) -> Result<(), AnyError> {
    tokio::time::timeout(BUILD_TIMEOUT, farm.handle.wait()).await??;

    let summary = farm
        .journal
        .summary()
        .ok_or("the warm scheduler must have stopped cleanly")?;
    assert!(summary.finished);
    assert_eq!(
        built(&summary, true).len(),
        7,
        "every previously built target is served from the store: {summary:?}"
    );
    assert!(
        built(&summary, false).is_empty(),
        "a warm build compiles nothing"
    );
    assert_eq!(
        farm.journal.total_built(),
        0,
        "no worker touched the blocking pool"
    );
    assert_eq!(
        summary.targets_in(&TargetState::Failed { attempts: 2 }),
        ["ui-lib"],
        "the durable attempt log refuses the poison target without executing it"
    );
    let pool = farm.journal.pool().ok_or("warm pool report")?;
    assert_eq!(pool.stalled, 0);
    assert_eq!(pool.worker_restarts, 0, "nothing crashed the second time");
    assert_eq!(
        farm.lease.acquisitions(),
        1,
        "the unscripted lease is taken once and never lost"
    );
    println!("PHASE 5 OK — warm rebuild resolved entirely from the store");
    Ok(())
}

/// What survived both runs.
fn phase_6(durable: &Durable, cold: &Farm, warm: &Farm) -> Result<(), AnyError> {
    let store = durable.store.report();
    assert_eq!(store.entries, 7, "one artifact per compiled target");
    assert_eq!(store.writes, 7, "the warm build added nothing");
    assert!(
        store.hits >= 7,
        "the warm build's lookups all hit: {store:?}"
    );

    let attempts = durable.attempts.snapshot();
    assert_eq!(
        attempts.get("ui-lib"),
        Some(&durable.attempts.max_attempts()),
        "the poison target spent exactly its allowance, across two runs"
    );
    assert_eq!(
        attempts.get("net-lib"),
        Some(&2),
        "the wedged target needed a second attempt on a healthy worker"
    );
    assert!(
        !attempts.contains_key("app-bundle"),
        "a skipped target is never dispatched"
    );

    println!("cold pool: {:#?}", cold.journal.pool());
    println!("warm pool: {:#?}", warm.journal.pool());
    println!("final display: {:#?}", warm.journal.display());
    println!("attempt log: {attempts:#?}");
    println!("store: {store:#?}");
    println!("PHASE 6 OK — durable state carried the second build");
    Ok(())
}

fn runnable(graph: &Graph, label: &str) -> RunnableActor {
    graph
        .actors()
        .iter()
        .find(|actor| actor.label() == label)
        .unwrap_or_else(|| panic!("{label} is registered in the graph"))
        .clone()
}

fn built(summary: &BuildStatus, cached: bool) -> Vec<TargetId> {
    summary
        .states
        .iter()
        .filter(
            |(_, state)| matches!(state, TargetState::Built { cached: hit, .. } if *hit == cached),
        )
        .map(|(target, _)| *target)
        .collect()
}

fn mailbox(farm: &Farm, actor_id: &str) -> Option<ActorStats> {
    farm.handle
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == actor_id)
}

async fn pool_report(farm: &Farm) -> Option<PoolReport> {
    farm.refs
        .pool
        .call(CALL_DEADLINE, |reply| PoolMsg::Report { reply })
        .await
        .ok()
}

async fn await_until<F, Fut>(mut predicate: F) -> Result<(), AnyError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(PHASE_TIMEOUT, async {
        loop {
            if predicate().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}
