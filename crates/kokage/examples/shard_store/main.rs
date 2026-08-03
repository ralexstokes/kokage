//! A state-owning shard store under deliberate topology change.
//!
//! This deterministic acceptance script exercises a mode unlike failure-led
//! supervision: the application chooses to replace live subtrees while their
//! durable state and traffic remain correct.
//!
//! ```text
//! shard-store (ordered root)
//! ├── directory          application registry; atomically rebound by router
//! ├── shards             dynamic scope
//! │   ├── shard-...      ordered subtree per key range
//! │   │   └── store      state owner; restart-stable durable image
//! │   └── shard-...
//! └── membership-router  sole topology writer; buffers moving ranges
//! ```
//!
//! The script writes and reads through the directory, splits one shard with a
//! blue/green handoff, and rolls a config replacement across both successors.
//! One source actor crashes after persisting its handoff image; its stable ref
//! reaches the recovered incarnation, while the planned successors receive
//! fresh refs through an atomic directory cutover. Exact effect ids, values,
//! routes, lineages, generations, restart counters, and actor-stat paths are
//! asserted before shutdown. Fresh trees then make the same executable inject
//! pre-commit failures and post-commit reply loss, prove accepted-request
//! quiescence and the crash-window fence, and reconcile or roll back each
//! outcome before traffic resumes.

mod directory;
mod model;
mod router;
mod shard;

use std::{collections::BTreeMap, error::Error, io, sync::Once, time::Duration};

use directory::{Directory, DirectoryMsg, Endpoint};
use kokage::{ActorRef, DynamicScopeRef, DynamicTree, ScopeRef, Strategy, Tree};
use model::{
    DirectorySnapshot, DurableImage, Key, KeyRange, PlannedChange, ReadReceipt, ShardConfig,
    TransitionReport, Value, Write, WriteReceipt,
};
use router::{FailurePoint, FaultInjector, MembershipRouter, RouterMsg, TransitionGate};
use shard::ShardMsg;

const CALL_BOUND: Duration = Duration::from_secs(25);
const SCRIPT_BOUND: Duration = Duration::from_secs(60);

type AnyError = Box<dyn Error + Send + Sync>;

struct Acceptance {
    running: kokage::RunningTree,
    root: ScopeRef,
    shards: DynamicScopeRef,
    directory: ActorRef<DirectoryMsg>,
    router: ActorRef<RouterMsg>,
    gate: TransitionGate,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    install_deliberate_panic_filter();

    tokio::time::timeout(SCRIPT_BOUND, run())
        .await
        .expect("the shard-store acceptance script must remain bounded")?;
    Ok(())
}

fn install_deliberate_panic_filter() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let default_panic_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let deliberate = info
                .payload()
                .downcast_ref::<String>()
                .is_some_and(|message| message.starts_with("scripted shard handoff crash"));
            if !deliberate {
                default_panic_hook(info);
            }
        }));
    });
}

async fn run() -> Result<(), AnyError> {
    let acceptance = build()?;
    acceptance.root.wait_started().await?;

    let initial = bootstrap(&acceptance.router).await?;
    assert_eq!(initial.revision, 1);
    assert_eq!(initial.planned_rebinds, 0);
    assert_eq!(initial.routes.len(), 1);
    assert_eq!(initial.routes[0].range, KeyRange::new(0, 100));
    assert_eq!(acceptance.shards.snapshot().children.len(), 1);
    println!("PHASE 1 OK — router mounted one state-owning shard and registered its live ref");

    for command in [
        Write {
            effect: 1,
            key: 10,
            delta: 5,
        },
        Write {
            effect: 2,
            key: 60,
            delta: 7,
        },
        Write {
            effect: 3,
            key: 10,
            delta: 3,
        },
        Write {
            effect: 4,
            key: 80,
            delta: -2,
        },
    ] {
        assert!(write(&acceptance.router, command).await?.applied);
    }
    let duplicate = write(
        &acceptance.router,
        Write {
            effect: 1,
            key: 10,
            delta: 999,
        },
    )
    .await?;
    assert!(!duplicate.applied);
    assert_eq!(
        duplicate.value,
        Value {
            total: 8,
            version: 2
        }
    );
    let conflicting_replay = write(
        &acceptance.router,
        Write {
            effect: 1,
            key: 20,
            delta: 999,
        },
    )
    .await
    .expect_err("an effect id cannot be replayed against a different key");
    assert!(
        conflicting_replay
            .to_string()
            .contains("effect 1 was already applied to key 10")
    );
    let initial_image = durable_image(&endpoint(&acceptance.directory, 10).await?).await?;
    assert!(!initial_image.values.contains_key(&20));
    assert_eq!(read(&acceptance.router, 60).await?.value.total, 7);
    println!("PHASE 2 OK — effect ids are idempotent for one key and reject cross-key replay");

    let source_id = initial.routes[0].shard_id.clone();
    let split_ticket = acceptance.gate.arm();
    let split_router = acceptance.router.clone();
    let split = tokio::spawn(async move { split(&split_router, source_id, 50).await });
    acceptance.gate.wait_entered(split_ticket).await;

    let buffered_before = acceptance.gate.buffered();
    let write_left = spawn_write(
        acceptance.router.clone(),
        Write {
            effect: 5,
            key: 20,
            delta: 4,
        },
    );
    let write_right = spawn_write(
        acceptance.router.clone(),
        Write {
            effect: 6,
            key: 70,
            delta: 6,
        },
    );
    let read_during_split = spawn_read(acceptance.router.clone(), 10);
    acceptance.gate.wait_buffered(buffered_before + 3).await;
    acceptance.gate.release();

    let split_report = split.await??;
    assert_eq!(split_report.change, PlannedChange::Split);
    assert_eq!(split_report.successors.len(), 2);
    assert_eq!(split_report.moved_keys, 3);
    assert_eq!(split_report.durable_effects, 4);
    assert_eq!(split_report.buffered_requests, 3);
    assert!(!split_report.recovered_crash);
    assert!(!split_report.cutover_reconciled);
    assert!(!split_report.retirement_reconciled);
    assert!(!split_report.retirement_pending);
    assert_eq!(split_report.source_restart.child_restarts, 0);
    assert!(write_left.await??.applied);
    assert!(write_right.await??.applied);
    assert_eq!(read_during_split.await??.value.total, 8);

    let after_split = directory_snapshot(&acceptance.directory).await?;
    assert_eq!(after_split.revision, 2);
    assert_eq!(after_split.planned_rebinds, 1);
    assert_eq!(
        after_split
            .routes
            .iter()
            .map(|route| route.range)
            .collect::<Vec<_>>(),
        [KeyRange::new(0, 50), KeyRange::new(50, 100)]
    );
    assert_eq!(acceptance.shards.snapshot().children.len(), 2);
    println!(
        "PHASE 3 OK — blue/green split buffered three requests, handed off state, and rebound two refs"
    );

    // Roll the same config revision one shard at a time. The first planned
    // replacement stays at generation zero. The second deliberately crashes
    // only its old actor after the handoff image is durable.
    let first_reload_source = after_split.routes[0].shard_id.clone();
    let first_reload = reload(
        &acceptance.router,
        first_reload_source,
        ShardConfig::reloaded(),
        false,
    )
    .await?;
    assert_planned_reload(&first_reload, false);

    assert!(
        write(
            &acceptance.router,
            Write {
                effect: 7,
                key: 70,
                delta: 7,
            }
        )
        .await?
        .applied
    );
    assert_eq!(read(&acceptance.router, 10).await?.value.total, 8);

    let midway = directory_snapshot(&acceptance.directory).await?;
    let crash_source = midway
        .routes
        .iter()
        .find(|route| route.range.contains(60))
        .expect("right shard remains during rolling reload")
        .shard_id
        .clone();
    let crash_ticket = acceptance.gate.arm();
    let crash_router = acceptance.router.clone();
    let crashed_reload = tokio::spawn(async move {
        reload(&crash_router, crash_source, ShardConfig::reloaded(), true).await
    });
    acceptance.gate.wait_entered(crash_ticket).await;

    // The other range remains available while this member is held mid-roll.
    let unaffected = write(
        &acceptance.router,
        Write {
            effect: 8,
            key: 10,
            delta: 9,
        },
    )
    .await?;
    assert!(unaffected.applied);

    let buffered_before = acceptance.gate.buffered();
    let recovered_write = spawn_write(
        acceptance.router.clone(),
        Write {
            effect: 9,
            key: 60,
            delta: 8,
        },
    );
    let recovered_read = spawn_read(acceptance.router.clone(), 70);
    acceptance.gate.wait_buffered(buffered_before + 2).await;
    acceptance.gate.release();

    let crashed_reload = crashed_reload.await??;
    assert_planned_reload(&crashed_reload, true);
    assert_eq!(crashed_reload.buffered_requests, 2);
    assert_eq!(crashed_reload.source_restart.generation, 1);
    assert_eq!(crashed_reload.source_restart.child_restarts, 1);
    assert_eq!(crashed_reload.source_restart.scope_restarts, 1);
    assert_eq!(crashed_reload.source_restart.actor_starts, 2);
    assert!(recovered_write.await??.applied);
    assert_eq!(recovered_read.await??.value.total, 13);
    println!(
        "PHASE 4 OK — rolling reload stayed live; crash restart is generation/count 1 while planned replacements are fresh memberships"
    );

    verify_final_state(&acceptance).await?;
    println!(
        "PHASE 5 OK — nine durable effects survived with zero loss/duplication across three planned cutovers"
    );

    acceptance.running.shutdown().await?;

    accepted_requests_quiesce_before_handoff().await?;
    println!("PHASE 6 OK — accepted requests quiesced before handoff while later work buffered");

    crash_window_write_is_rejected_then_safely_retried().await?;
    println!("PHASE 7 OK — the durable fence rejected a crash-window write before safe retry");

    precommit_failures_abort_and_cleanup_before_replay().await?;
    println!("PHASE 8 OK — pre-commit failures aborted, cleaned up, and replayed buffered work");

    committed_unknown_outcomes_are_reconciled().await?;
    println!("PHASE 9 OK — committed cutover and retirement reply loss reconciled exactly");
    Ok(())
}

fn build() -> Result<Acceptance, AnyError> {
    let acceptance = build_with_faults(FaultInjector::default())?;
    println!("PHASE 0 OK — flagship topology validated and serialized before spawn");
    Ok(acceptance)
}

fn build_with_faults(faults: FaultInjector) -> Result<Acceptance, AnyError> {
    let gate = TransitionGate::default();
    let shards_tree = DynamicTree::new();
    let shards = shards_tree.scope();

    let mut tree = Tree::new().strategy(Strategy::OneForOne);
    let directory = tree.add_actor("directory", Directory::default);
    tree.add_subtree("shards", shards_tree);
    let router = tree.add_actor("membership-router", {
        let shards = shards.clone();
        let directory = directory.clone();
        let gate = gate.clone();
        let faults = faults.clone();
        move || {
            MembershipRouter::with_faults(
                shards.clone(),
                directory.clone(),
                gate.clone(),
                faults.clone(),
            )
        }
    });

    let outline = tree.outline();
    assert_eq!(
        outline.child_ids(),
        ["directory", "shards", "membership-router"]
    );
    let json = serde_json::to_string(&outline)?;
    assert!(json.contains("membership-router"));

    let running = tree.spawn()?;
    let root = running.scope();
    Ok(Acceptance {
        running,
        root,
        shards,
        directory,
        router,
        gate,
    })
}

async fn bootstrap(router: &ActorRef<RouterMsg>) -> Result<DirectorySnapshot, AnyError> {
    app(router
        .call(
            |reply| RouterMsg::Bootstrap {
                range: KeyRange::new(0, 100),
                config: ShardConfig::initial(),
                reply,
            },
            CALL_BOUND,
        )
        .await?)
}

async fn write(router: &ActorRef<RouterMsg>, command: Write) -> Result<WriteReceipt, AnyError> {
    app(router
        .call(|reply| RouterMsg::Write { command, reply }, CALL_BOUND)
        .await?)
}

async fn read(router: &ActorRef<RouterMsg>, key: Key) -> Result<ReadReceipt, AnyError> {
    app(router
        .call(|reply| RouterMsg::Read { key, reply }, CALL_BOUND)
        .await?)
}

async fn split(
    router: &ActorRef<RouterMsg>,
    source_id: String,
    at: Key,
) -> Result<TransitionReport, AnyError> {
    app(router
        .call(
            |reply| RouterMsg::Split {
                source_id,
                at,
                reply,
            },
            CALL_BOUND,
        )
        .await?)
}

async fn reload(
    router: &ActorRef<RouterMsg>,
    source_id: String,
    config: ShardConfig,
    crash_during_handoff: bool,
) -> Result<TransitionReport, AnyError> {
    app(router
        .call(
            |reply| RouterMsg::Reload {
                source_id,
                config,
                crash_during_handoff,
                reply,
            },
            CALL_BOUND,
        )
        .await?)
}

fn spawn_write(
    router: ActorRef<RouterMsg>,
    command: Write,
) -> tokio::task::JoinHandle<Result<WriteReceipt, AnyError>> {
    tokio::spawn(async move { write(&router, command).await })
}

fn spawn_read(
    router: ActorRef<RouterMsg>,
    key: Key,
) -> tokio::task::JoinHandle<Result<ReadReceipt, AnyError>> {
    tokio::spawn(async move { read(&router, key).await })
}

async fn directory_snapshot(
    directory: &ActorRef<DirectoryMsg>,
) -> Result<DirectorySnapshot, AnyError> {
    Ok(directory
        .call(|reply| DirectoryMsg::Snapshot { reply }, CALL_BOUND)
        .await?)
}

async fn endpoint(directory: &ActorRef<DirectoryMsg>, key: Key) -> Result<Endpoint, AnyError> {
    directory
        .call(|reply| DirectoryMsg::Resolve { key, reply }, CALL_BOUND)
        .await?
        .ok_or_else(|| io::Error::other(format!("no endpoint for key {key}")).into())
}

async fn durable_image(endpoint: &Endpoint) -> Result<DurableImage, AnyError> {
    Ok(endpoint
        .shard
        .call(|reply| ShardMsg::Snapshot { reply }, CALL_BOUND)
        .await?)
}

fn assert_planned_reload(report: &TransitionReport, recovered_crash: bool) {
    assert_eq!(report.change, PlannedChange::ConfigReload);
    assert_eq!(report.sources.len(), 1);
    assert_eq!(report.successors.len(), 1);
    assert_eq!(report.recovered_crash, recovered_crash);
    assert!(!report.cutover_reconciled);
    assert!(!report.retirement_reconciled);
    assert!(!report.retirement_pending);
    if !recovered_crash {
        assert_eq!(report.source_restart.generation, 0);
        assert_eq!(report.source_restart.child_restarts, 0);
        assert_eq!(report.source_restart.scope_restarts, 0);
        assert_eq!(report.source_restart.actor_starts, 1);
    }
}

async fn verify_final_state(acceptance: &Acceptance) -> Result<(), AnyError> {
    let directory = directory_snapshot(&acceptance.directory).await?;
    assert_eq!(directory.revision, 4);
    assert_eq!(directory.planned_rebinds, 3);
    assert_eq!(directory.routes.len(), 2);
    assert!(
        directory
            .routes
            .iter()
            .all(|route| route.config == ShardConfig::reloaded())
    );

    let left = durable_image(&endpoint(&acceptance.directory, 10).await?).await?;
    let right = durable_image(&endpoint(&acceptance.directory, 60).await?).await?;
    let mut values = left.values.clone();
    assert!(values.keys().all(|key| !right.values.contains_key(key)));
    values.extend(right.values.clone());
    assert_eq!(
        values,
        BTreeMap::from([
            (
                10,
                Value {
                    total: 17,
                    version: 3
                }
            ),
            (
                20,
                Value {
                    total: 4,
                    version: 1
                }
            ),
            (
                60,
                Value {
                    total: 15,
                    version: 2
                }
            ),
            (
                70,
                Value {
                    total: 13,
                    version: 2
                }
            ),
            (
                80,
                Value {
                    total: -2,
                    version: 1
                }
            ),
        ])
    );

    let mut effects = left.applied.clone();
    for (effect, key) in right.applied {
        assert!(
            effects.insert(effect, key).is_none(),
            "effect was duplicated"
        );
    }
    assert_eq!(
        effects,
        BTreeMap::from([
            (1, 10),
            (2, 60),
            (3, 10),
            (4, 80),
            (5, 20),
            (6, 70),
            (7, 70),
            (8, 10),
            (9, 60),
        ])
    );

    let shard_snapshot = acceptance.shards.snapshot();
    assert_eq!(shard_snapshot.total_restarts, 0);
    assert_eq!(shard_snapshot.children.len(), 2);
    for subtree in &shard_snapshot.children {
        assert_eq!(subtree.generation, 0);
        assert_eq!(subtree.restart_count, 0);
        let inner = subtree.supervisor.as_deref().expect("shard is a subtree");
        assert_eq!(inner.total_restarts, 0);
        let store = inner.child("store").expect("shard contains store actor");
        assert_eq!(store.generation, 0);
        assert_eq!(store.restart_count, 0);
    }
    let encoded = serde_json::to_string(&shard_snapshot)?;
    assert!(encoded.contains("total_restarts"));

    let stats = acceptance.root.actor_stats();
    assert_eq!(stats.len(), 4);
    assert_eq!(
        stats
            .iter()
            .map(|stat| stat.stats.actor_id.as_str())
            .collect::<Vec<_>>(),
        ["directory", "membership-router", "store", "store"]
    );
    assert_eq!(
        stats
            .iter()
            .filter(|stat| stat.stats.actor_id == "store")
            .map(|stat| stat.scope_path.len())
            .collect::<Vec<_>>(),
        [2, 2]
    );
    let store_scope_lineages: Vec<_> = stats
        .iter()
        .filter(|stat| stat.stats.actor_id == "store")
        .map(|stat| {
            stat.scope_path
                .last()
                .expect("store has a shard-subtree path")
                .lineage
        })
        .collect();
    assert!(store_scope_lineages.iter().all(|lineage| *lineage > 0));
    assert_ne!(store_scope_lineages[0], store_scope_lineages[1]);

    for (key, expected) in [(10, 17), (20, 4), (60, 15), (70, 13), (80, -2)] {
        assert_eq!(read(&acceptance.router, key).await?.value.total, expected);
    }
    Ok(())
}

fn app<T>(result: Result<T, String>) -> Result<T, AnyError> {
    result.map_err(|error| io::Error::other(error).into())
}

async fn started(faults: FaultInjector) -> Result<Acceptance, AnyError> {
    let acceptance = build_with_faults(faults)?;
    acceptance.root.wait_started().await?;
    Ok(acceptance)
}

async fn split_bootstrapped(acceptance: &Acceptance) -> Result<DirectorySnapshot, AnyError> {
    let initial = bootstrap(&acceptance.router).await?;
    split(&acceptance.router, initial.routes[0].shard_id.clone(), 50).await?;
    directory_snapshot(&acceptance.directory).await
}

async fn accepted_requests_quiesce_before_handoff() -> Result<(), AnyError> {
    let acceptance = started(FaultInjector::default()).await?;
    let initial = bootstrap(&acceptance.router).await?;

    let request_ticket = acceptance.gate.hold_requests(2);
    let early_write = spawn_write(
        acceptance.router.clone(),
        Write {
            effect: 41,
            key: 10,
            delta: 4,
        },
    );
    let early_read = spawn_read(acceptance.router.clone(), 60);
    acceptance.gate.wait_requests_entered(request_ticket).await;

    let transition_ticket = acceptance.gate.arm();
    let pending_before = acceptance.gate.pending();
    let buffered_before = acceptance.gate.buffered();
    let router = acceptance.router.clone();
    let source = initial.routes[0].shard_id.clone();
    let transition = tokio::spawn(async move { split(&router, source, 50).await });
    acceptance.gate.wait_pending(pending_before + 1).await;

    assert!(
        !acceptance.gate.has_entered(transition_ticket),
        "handoff must not start while accepted requests are in flight"
    );
    let buffered_write = spawn_write(
        acceptance.router.clone(),
        Write {
            effect: 42,
            key: 20,
            delta: 6,
        },
    );
    acceptance.gate.wait_buffered(buffered_before + 1).await;
    assert!(!acceptance.gate.has_entered(transition_ticket));

    acceptance.gate.release_requests();
    assert!(early_write.await??.applied);
    let _ = early_read.await??;
    acceptance.gate.wait_entered(transition_ticket).await;
    acceptance.gate.release();

    let report = transition.await??;
    assert_eq!(report.buffered_requests, 1);
    assert!(buffered_write.await??.applied);
    let image = durable_image(&endpoint(&acceptance.directory, 10).await?).await?;
    assert_eq!(image.applied.get(&41), Some(&10));
    assert_eq!(image.applied.get(&42), Some(&20));
    acceptance.running.shutdown().await?;
    Ok(())
}

async fn crash_window_write_is_rejected_then_safely_retried() -> Result<(), AnyError> {
    install_deliberate_panic_filter();
    let acceptance = started(FaultInjector::default()).await?;
    let after_split = split_bootstrapped(&acceptance).await?;
    let source = after_split
        .routes
        .iter()
        .find(|route| route.range.contains(60))
        .expect("right shard exists")
        .shard_id
        .clone();
    let old_endpoint = endpoint(&acceptance.directory, 60).await?;

    let ticket = acceptance.gate.arm_recovery();
    let router = acceptance.router.clone();
    let transition =
        tokio::spawn(async move { reload(&router, source, ShardConfig::reloaded(), true).await });
    acceptance.gate.wait_recovery_entered(ticket).await;

    let command = Write {
        effect: 99,
        key: 60,
        delta: 11,
    };
    let rejected = old_endpoint
        .shard
        .call(|reply| ShardMsg::Write { command, reply }, CALL_BOUND)
        .await?;
    assert!(
        rejected
            .expect_err("the durable handoff fence rejects crash-window writes")
            .contains("is fenced for active handoff")
    );

    acceptance.gate.release_recovery();
    let report = transition.await??;
    assert!(report.recovered_crash);
    let image = durable_image(&endpoint(&acceptance.directory, 60).await?).await?;
    assert!(!image.applied.contains_key(&99));

    assert!(write(&acceptance.router, command).await?.applied);
    assert!(!write(&acceptance.router, command).await?.applied);
    acceptance.running.shutdown().await?;
    Ok(())
}

async fn precommit_failures_abort_and_cleanup_before_replay() -> Result<(), AnyError> {
    for point in [
        FailurePoint::FirstMount,
        FailurePoint::SecondMount,
        FailurePoint::BeforeCutover,
    ] {
        let faults = FaultInjector::default();
        let acceptance = started(faults.clone()).await?;
        let initial = bootstrap(&acceptance.router).await?;
        assert!(
            write(
                &acceptance.router,
                Write {
                    effect: 1,
                    key: 10,
                    delta: 2,
                },
            )
            .await?
            .applied
        );

        faults.arm(point);
        let ticket = acceptance.gate.arm();
        let buffered_before = acceptance.gate.buffered();
        let router = acceptance.router.clone();
        let source = initial.routes[0].shard_id.clone();
        let transition = tokio::spawn(async move { split(&router, source, 50).await });
        acceptance.gate.wait_entered(ticket).await;
        let buffered = spawn_write(
            acceptance.router.clone(),
            Write {
                effect: 2,
                key: 10,
                delta: 3,
            },
        );
        acceptance.gate.wait_buffered(buffered_before + 1).await;
        acceptance.gate.release();

        let failure = transition.await?;
        assert!(failure.is_err(), "{point:?} must fail before commit");
        assert!(buffered.await??.applied);
        assert_eq!(read(&acceptance.router, 10).await?.value.total, 5);
        let directory = directory_snapshot(&acceptance.directory).await?;
        assert_eq!(directory.revision, 1);
        assert_eq!(directory.routes, initial.routes);
        assert_eq!(acceptance.shards.snapshot().children.len(), 1);
        acceptance.running.shutdown().await?;
    }
    Ok(())
}

async fn committed_unknown_outcomes_are_reconciled() -> Result<(), AnyError> {
    for point in [
        FailurePoint::CutoverReplyLost,
        FailurePoint::BeforeRetire,
        FailurePoint::RetireReplyLost,
    ] {
        let faults = FaultInjector::default();
        let acceptance = started(faults.clone()).await?;
        let initial = bootstrap(&acceptance.router).await?;
        faults.arm(point);
        let report = split(&acceptance.router, initial.routes[0].shard_id.clone(), 50).await?;
        assert_eq!(
            report.cutover_reconciled,
            point == FailurePoint::CutoverReplyLost
        );
        assert_eq!(
            report.retirement_reconciled,
            matches!(
                point,
                FailurePoint::BeforeRetire | FailurePoint::RetireReplyLost
            )
        );
        assert!(!report.retirement_pending);
        assert_eq!(acceptance.shards.snapshot().children.len(), 2);

        let directory = directory_snapshot(&acceptance.directory).await?;
        let source = directory.routes[0].shard_id.clone();
        let reload = reload(&acceptance.router, source, ShardConfig::reloaded(), false).await?;
        assert!(!reload.retirement_pending);
        acceptance.running.shutdown().await?;
    }
    Ok(())
}
