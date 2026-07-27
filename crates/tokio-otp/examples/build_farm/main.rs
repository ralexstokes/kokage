//! A remote build farm whose supervised runtime ends with one finite build.
//!
//! This example complements the long-running application examples with a
//! batch-shaped application. A dependency scheduler feeds a leader-owned,
//! runtime-sized worker scope; workers consult a durable content-addressed
//! store; and a plain supervised task renews the build lease. The scheduler's
//! clean stop is the signal that shuts down the whole tree.
//!
//! `#[derive(Supervision)]` wires the statically known actors and their cyclic
//! references. Tree placement stays explicit because the root also contains a
//! plain [`ChildSpec`](tokio_otp::ChildSpec) and an actor-owned dynamic scope.
//!
//! ```text
//! build-farm (ordered, one-for-one)
//! ├── progress         keyed conflating mailbox
//! ├── cas              durable content-addressed store
//! ├── lease-renewer    plain ChildSpec, readiness gated
//! ├── build-pool       actor-with-scope, one-for-all
//! │   ├── pool         leader
//! │   └── children     dynamic worker-0, worker-1, ...
//! └── scheduler        OnFailure; clean completion stops the farm
//! ```

mod cas;
mod lease;
mod messages;
mod model;
mod pool;
mod progress;
mod scheduler;
mod shared;
mod worker;

use std::{
    error::Error,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use tokio_otp::{
    ActorSpec, ChildOutline, SupervisionOutline,
    prelude::{
        ActorOptions, CompletionGuard, GraphBuilder, MailboxMode, RestartIntensity, RestartPolicy,
        Runtime, RuntimeHandle, ScopeKind, Strategy, Supervision, SupervisionTree,
    },
};

use cas::{Cas, CasFactory};
use lease::{LEASE_ID, Lease};
use messages::{
    BuildStatus, CALL_DEADLINE, Phase, PoolMsg, ProgressMsg, SchedulerMsg, TargetState,
};
use model::{BuildPlan, TargetId};
use pool::{POOL_LEADER_ID, POOL_NODE_ID, Pool};
use progress::{Progress, progress_key};
use scheduler::{SCHEDULER_ID, Scheduler};
use shared::{AttemptBook, BuildJournal, CasStore};
use worker::WorkerFactory;

const PROGRESS_ID: &str = "progress";
const CAS_ID: &str = "cas";
const PROBE_TARGET: TargetId = "display-probe";
const BUILD_TIMEOUT: Duration = Duration::from_secs(20);

type AnyError = Box<dyn Error + Send + Sync>;

struct Durable {
    store: Arc<CasStore>,
    attempts: Arc<AttemptBook>,
    labels: Arc<AtomicU64>,
    plan: Arc<BuildPlan>,
}

/// The statically known actor graph. Its supervision placement stays manual:
/// the root also contains a plain task and an actor-owned dynamic scope, two
/// shapes the derive deliberately does not model.
#[derive(Supervision)]
struct BuildFarmActors {
    #[supervision(options = ActorOptions::new()
        .mailbox(MailboxMode::conflate_by_key(progress_key)))]
    progress: Progress,
    cas: Cas,
    pool: Pool,
    scheduler: Scheduler,
}

struct Blueprint {
    runtime: Runtime,
    outline: SupervisionOutline,
    refs: BuildFarmActorsRefs,
    journal: Arc<BuildJournal>,
    lease: Arc<Lease>,
}

struct Farm {
    handle: RuntimeHandle,
    refs: BuildFarmActorsRefs,
    journal: Arc<BuildJournal>,
    lease: Arc<Lease>,
    _completion: CompletionGuard,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    // One worker panic is intentional and supervised.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;

    let durable = Durable {
        store: CasStore::new(),
        attempts: AttemptBook::new(),
        labels: Arc::new(AtomicU64::new(0)),
        plan: Arc::new(BuildPlan::demo()),
    };

    let cold = assemble(&durable, true)?;
    verify_outline(&cold)?;
    let cold = spawn(cold);
    exercise_live_farm(&cold).await?;
    wait_for_build(&cold).await?;
    verify_cold_build(&durable, &cold)?;

    let warm = spawn(assemble(&durable, false)?);
    wait_for_build(&warm).await?;
    verify_warm_build(&durable, &warm)?;

    println!("PHASE 5 OK — durable cache and attempt history survived both runtimes");
    Ok(())
}

fn assemble(durable: &Durable, fail_first_lease: bool) -> Result<Blueprint, AnyError> {
    let journal = BuildJournal::new();
    let lease = Lease::new();

    let mut builder = GraphBuilder::new();
    builder.name("build-farm");
    builder.mailbox_capacity(32);

    let (graph, refs) = BuildFarmActors::graph_with(builder, |refs| {
        let worker_factory = WorkerFactory {
            cas: refs.cas.clone(),
            progress: refs.progress.clone(),
            attempts: Arc::clone(&durable.attempts),
        };
        BuildFarmActorsFactories {
            progress: {
                let journal = Arc::clone(&journal);
                move || Progress::new(Arc::clone(&journal))
            },
            cas: CasFactory {
                store: Arc::clone(&durable.store),
            },
            pool: {
                let scheduler = refs.scheduler.clone();
                let progress = refs.progress.clone();
                let journal = Arc::clone(&journal);
                let labels = Arc::clone(&durable.labels);
                move || {
                    Pool::new(
                        scheduler.clone(),
                        progress.clone(),
                        worker_factory.clone(),
                        Arc::clone(&journal),
                        Arc::clone(&labels),
                    )
                }
            },
            scheduler: {
                let plan = Arc::clone(&durable.plan);
                let pool = refs.pool.clone();
                let lease = Arc::clone(&lease);
                let journal = Arc::clone(&journal);
                move || {
                    Scheduler::new(
                        Arc::clone(&plan),
                        pool.clone(),
                        Arc::clone(&lease),
                        Arc::clone(&journal),
                    )
                }
            },
        }
    })?;

    let actor = |label| {
        graph
            .actor(label)
            .unwrap_or_else(|| panic!("{label} is declared by BuildFarmActors"))
            .clone()
    };

    let tree = SupervisionTree::new()
        .strategy(Strategy::OneForOne)
        .actor(actor(PROGRESS_ID))
        .actor(actor(CAS_ID))
        .task(lease::renewer(Arc::clone(&lease), fail_first_lease))
        .actor_with_scope_strategy(
            POOL_NODE_ID,
            ActorSpec::new(actor(POOL_LEADER_ID)).restart(RestartPolicy::Always),
            SupervisionTree::dynamic()
                .restart_intensity(RestartIntensity::new(8, Duration::from_secs(30))),
            Strategy::OneForAll,
        )
        .actor(ActorSpec::new(actor(SCHEDULER_ID)).restart(RestartPolicy::OnFailure));

    Ok(Blueprint {
        outline: tree.outline()?,
        runtime: tree.build()?,
        refs,
        journal,
        lease,
    })
}

fn spawn(blueprint: Blueprint) -> Farm {
    let pending = blueprint.runtime.handle();
    let completion = pending.shutdown_on_completion([SCHEDULER_ID]);
    let handle = blueprint.runtime.spawn();
    Farm {
        handle,
        refs: blueprint.refs,
        journal: blueprint.journal,
        lease: blueprint.lease,
        _completion: completion,
    }
}

fn verify_outline(blueprint: &Blueprint) -> Result<(), AnyError> {
    assert_eq!(
        blueprint.outline.child_ids(),
        [PROGRESS_ID, CAS_ID, LEASE_ID, POOL_NODE_ID, SCHEDULER_ID]
    );
    assert!(matches!(
        blueprint.outline.child(LEASE_ID),
        Some(ChildOutline::Child { restart, .. }) if *restart == RestartPolicy::Always
    ));
    let Some(ChildOutline::ActorWithScope {
        children, strategy, ..
    }) = blueprint.outline.child(POOL_NODE_ID)
    else {
        return Err("pool must be an actor with an owned scope".into());
    };
    assert_eq!(*strategy, Strategy::OneForAll);
    assert_eq!(children.kind, ScopeKind::Dynamic);
    assert!(matches!(
        blueprint.outline.child(SCHEDULER_ID),
        Some(ChildOutline::Actor { restart, .. }) if *restart == RestartPolicy::OnFailure
    ));
    println!("PHASE 0 OK — declared batch topology validated before spawn");
    Ok(())
}

async fn exercise_live_farm(farm: &Farm) -> Result<(), AnyError> {
    tokio::time::timeout(Duration::from_secs(3), farm.handle.wait_started()).await??;
    assert!(farm.lease.is_held());

    for update in 0..512 {
        let _ = farm.refs.progress.try_send(ProgressMsg::Update {
            target: PROBE_TARGET,
            phase: if update % 2 == 0 {
                Phase::Queued
            } else {
                Phase::Running
            },
        });
    }
    let rendered = farm
        .refs
        .progress
        .call(CALL_DEADLINE, |reply| ProgressMsg::Snapshot { reply })
        .await?;
    assert!(rendered.contains_key(PROBE_TARGET));
    let progress_stats = farm
        .handle
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == PROGRESS_ID)
        .ok_or("progress actor stats are available while the farm is live")?;
    assert!(progress_stats.messages_conflated > 0);

    let status = farm
        .refs
        .scheduler
        .call(CALL_DEADLINE, |reply| SchedulerMsg::Snapshot { reply })
        .await?;
    assert!(!status.targets.is_empty());
    let pool = farm
        .refs
        .pool
        .call(CALL_DEADLINE, |reply| PoolMsg::Report { reply })
        .await?;
    assert!(pool.added_workers <= 3);
    println!(
        "PHASE 1 OK — readiness held the lease and progress updates conflated ({} replaced)",
        progress_stats.messages_conflated
    );
    Ok(())
}

async fn wait_for_build(farm: &Farm) -> Result<(), AnyError> {
    tokio::time::timeout(BUILD_TIMEOUT, farm.handle.wait()).await??;
    Ok(())
}

fn verify_cold_build(durable: &Durable, farm: &Farm) -> Result<(), AnyError> {
    let status = farm
        .journal
        .status()
        .ok_or("scheduler writes its terminal status during shutdown")?;
    assert_complete(&status, false);
    assert!(status.lease_stalls > 0);

    let pool = farm
        .journal
        .pool()
        .ok_or("pool writes its final report during shutdown")?;
    assert!(pool.peak_workers >= 2, "{pool:?}");
    assert_eq!(pool.lost_dispatches, 1, "{pool:?}");
    assert!(pool.removed_workers > 0, "{pool:?}");
    assert_eq!(farm.lease.acquisitions(), 2);
    assert!(farm.lease.renewals() >= 1);

    let attempts = durable.attempts.snapshot();
    assert_eq!(attempts.get("network"), Some(&2));
    assert_eq!(durable.store.report().entries, durable.plan.actions().len());
    assert!(
        farm.journal
            .progress()
            .values()
            .filter(|phase| **phase == Phase::Built)
            .count()
            >= durable.plan.actions().len()
    );
    println!(
        "PHASE 2 OK — cold build completed after one worker crash and {} lease stalls",
        status.lease_stalls
    );
    println!(
        "PHASE 3 OK — dynamic pool peaked at {} workers and retired {}",
        pool.peak_workers, pool.removed_workers
    );
    Ok(())
}

fn verify_warm_build(durable: &Durable, farm: &Farm) -> Result<(), AnyError> {
    let status = farm
        .journal
        .status()
        .ok_or("warm scheduler writes its terminal status")?;
    assert_complete(&status, true);
    assert_eq!(farm.lease.acquisitions(), 1);

    let attempts = durable.attempts.snapshot();
    assert_eq!(attempts.get("network"), Some(&2));
    assert!(attempts.values().all(|attempts| *attempts <= 2));
    let store = durable.store.report();
    assert_eq!(store.entries, durable.plan.actions().len());
    assert_eq!(store.writes, durable.plan.actions().len() as u64);
    assert!(store.hits >= durable.plan.actions().len() as u64);
    println!("PHASE 4 OK — warm rebuild was served entirely from the durable CAS");
    Ok(())
}

fn assert_complete(status: &BuildStatus, cached: bool) {
    assert!(status.complete, "{status:?}");
    assert!(
        status
            .targets
            .values()
            .all(|state| matches!(state, TargetState::Built { cached: hit, .. } if *hit == cached))
    );
}
