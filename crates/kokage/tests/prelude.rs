mod support;

use support::TreeBuilder;

use std::time::Duration;

use kokage::{
    ActorStatus, Restart, Strategy,
    host::ChildSpec,
    observe::{LifecycleEvent, LifecycleEventKind},
    prelude::*,
};
use tokio::{sync::mpsc, time::timeout};

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use kokage::prelude::{
            Actor, ActorRef, ActorResult, ActorSlot, ActorSpec, Context, OrderedTree, Reply,
            StopContext, SupervisorSnapshot, SupervisorSnapshotReceiver,
        };
    }

    mod advanced_root {
        use kokage::{
            ActorFactory, Backoff, BackoffParts, BlockingCancelled, BuildError, CancellationToken,
            ControlError, DownReason, DynamicRestrictedScope, DynamicRuntimeHandle, DynamicTree,
            Guard, MailboxMode, MonitorEvent, OffloadDeadline, Restart, RestartMode,
            RestrictedScope, Runtime, RuntimeHandle, SealedActorSlot, SealedActorSpec, Shutdown,
            ShutdownMode, Strategy, SupervisorError, TimerKey, TreeNode,
        };
    }

    mod host {
        use kokage::host::{
            ActorRunError, BoxError, ChildContext, ChildResult, ChildSpec, DEFAULT_SHUTDOWN_BOUND,
            RawActor, RawContext, RunnableActor,
        };
    }

    mod observe {
        use kokage::observe::{
            ActorStats, ChildExitView, ChildMembershipView, ChildOutline, ChildSnapshot,
            ChildStateView, CompletionError, CompletionOutcome, LifecycleEvent, LifecycleEventKind,
            LifecyclePathSegment, LifecycleWatch, ScopeKind, SupervisionOutline,
            SupervisorPathSegment, SupervisorSnapshot, SupervisorStateView,
        };
    }
}

#[test]
fn prelude_constructs_acyclic_and_cyclic_actor_declarations() {
    let spec = ActorSpec::new("direct", || BlockingWorker {
        observed: mpsc::unbounded_channel().0,
    });
    let slot = ActorSlot::new("cyclic");
    let (slot, _cyclic_ref) = slot.actor_ref();
    let cyclic = slot.define(|| BlockingWorker {
        observed: mpsc::unbounded_channel().0,
    });

    let _tree = OrderedTree::new().actor(spec).actor(cyclic);
}

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

async fn named_task(ctx: kokage::host::ChildContext) -> kokage::host::ChildResult {
    ctx.shutdown_token().cancelled().await;
    Ok(())
}

#[test]
fn host_task_surface_supports_a_named_factory_from_the_single_crate() {
    let _child = kokage::host::ChildSpec::task("worker", named_task);
}

#[test]
fn actor_supervisor_path_segments_are_nameable() {
    fn path_len(path: &[kokage::observe::SupervisorPathSegment]) -> usize {
        path.len()
    }

    let path: Vec<kokage::observe::SupervisorPathSegment> = Vec::new();
    assert_eq!(path_len(&path), 0);
}

#[test]
fn policy_values_expose_their_declared_behavior() {
    fn strategy_name(strategy: Strategy) -> &'static str {
        match strategy {
            Strategy::OneForOne => "one-for-one",
            Strategy::OneForAll => "one-for-all",
            Strategy::RestForOne => "rest-for-one",
        }
    }

    fn restart_name(policy: Restart) -> &'static str {
        match policy.mode() {
            kokage::RestartMode::Always => "always",
            kokage::RestartMode::OnFailure => "on-failure",
            kokage::RestartMode::Never => "never",
        }
    }

    fn drain_name(policy: kokage::Shutdown) -> &'static str {
        match policy.mode() {
            kokage::ShutdownMode::Drain => "drain",
            kokage::ShutdownMode::Discard => "discard",
            kokage::ShutdownMode::Abort => "abort",
        }
    }

    fn backoff_name(backoff: kokage::Backoff) -> &'static str {
        match backoff.parts() {
            kokage::BackoffParts::None => "none",
            kokage::BackoffParts::Fixed(_) => "fixed",
            kokage::BackoffParts::Exponential { jitter: false, .. } => "exponential",
            kokage::BackoffParts::Exponential { jitter: true, .. } => "jittered-exponential",
        }
    }

    fn actor_status_name(status: ActorStatus) -> &'static str {
        match status {
            ActorStatus::Running => "running",
            ActorStatus::Draining => "draining",
            ActorStatus::Stopping => "stopping",
        }
    }

    fn scope_name(kind: kokage::observe::ScopeKind) -> &'static str {
        match kind {
            kokage::observe::ScopeKind::Ordered => "ordered",
            kokage::observe::ScopeKind::Dynamic => "dynamic",
        }
    }

    assert_eq!(strategy_name(Strategy::default()), "one-for-one");
    assert_eq!(restart_name(Restart::default()), "on-failure");
    assert_eq!(
        restart_name(Restart::always().limit(3, Duration::from_secs(1))),
        "always"
    );
    assert_eq!(drain_name(kokage::Shutdown::default()), "drain");
    assert_eq!(drain_name(kokage::Shutdown::abort()), "abort");
    assert_eq!(backoff_name(kokage::Backoff::none()), "none");
    assert_eq!(
        backoff_name(kokage::Backoff::exponential_with_jitter(
            Duration::from_millis(10),
            2,
            Duration::from_secs(1),
        )),
        "jittered-exponential"
    );
    assert_eq!(actor_status_name(ActorStatus::Running), "running");
    assert_eq!(scope_name(kokage::observe::ScopeKind::default()), "ordered");
}

#[derive(Clone)]
struct BlockingWorker {
    observed: mpsc::UnboundedSender<String>,
}

impl Actor for BlockingWorker {
    type Msg = ();

    async fn handle(&mut self, _message: (), ctx: &mut Context<'_, Self>) -> ActorResult {
        let observed = self.observed.clone();
        let actor_id = ctx.id().to_owned();
        ctx.run_blocking(move |token| {
            assert!(!token.is_cancelled());
            observed.send(actor_id).expect("test receiver dropped");
        })
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn umbrella_prelude_supports_blocking_and_supervisor_helpers() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = TreeBuilder::new();
    let worker = graph.actor(ActorSpec::new("worker", move || BlockingWorker {
        observed: observed_tx.clone(),
    }));

    let handle = graph
        .build()
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");
    let mut events = handle.handle().watch_lifecycle();
    worker.send(()).await.expect("worker accepts message");
    let observed = timeout(EVENT_TIMEOUT, observed_rx.recv())
        .await
        .expect("timed out waiting for blocking task")
        .expect("blocking task reported completion");
    assert_eq!(observed, "worker");

    let started = timeout(EVENT_TIMEOUT, async {
        loop {
            let event = events.next().await.expect("lifecycle remains open");
            if matches!(
                event,
                LifecycleEvent {
                    kind: LifecycleEventKind::ChildStarted {
                        ref child_id,
                        generation: 0,
                        ..
                    },
                    ..
                } if child_id == "worker"
            ) {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for started event");
    assert!(matches!(
        started,
        LifecycleEvent {
            kind: LifecycleEventKind::ChildStarted {
                ref child_id,
                generation: 0,
                ..
            },
            ..
        } if child_id == "worker"
    ));

    let snapshot = handle.handle().snapshot();
    assert!(
        snapshot
            .child("worker")
            .expect("worker child should exist")
            .state
            .is_running()
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[test]
fn task_policy_sets_remain_nameable_from_the_single_crate() {
    fn strategy_name(strategy: Strategy) -> &'static str {
        match strategy {
            Strategy::OneForOne => "one-for-one",
            Strategy::OneForAll => "one-for-all",
            Strategy::RestForOne => "rest-for-one",
        }
    }

    fn scope_name(kind: kokage::observe::ScopeKind) -> &'static str {
        match kind {
            kokage::observe::ScopeKind::Ordered => "ordered",
            kokage::observe::ScopeKind::Dynamic => "dynamic",
        }
    }

    assert_eq!(strategy_name(Strategy::default()), "one-for-one");
    assert_eq!(scope_name(kokage::observe::ScopeKind::default()), "ordered");
}

#[tokio::test]
async fn prelude_observes_raw_task_events_and_snapshots() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let tree = OrderedTree::new().task(ChildSpec::task("worker", move |ctx| {
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    }));
    let handle = tree.handle();
    let mut events = handle.watch_lifecycle();
    let runtime = tree.spawn().expect("task tree spawns");

    assert_eq!(
        timeout(EVENT_TIMEOUT, started_rx.recv())
            .await
            .expect("timed out waiting for task")
            .expect("task reported startup"),
        0
    );
    timeout(EVENT_TIMEOUT, async {
        loop {
            let event = events.next().await.expect("lifecycle remains open");
            if matches!(
                event.kind,
                LifecycleEventKind::ChildStarted {
                    ref child_id,
                    generation: 0,
                    ..
                } if child_id == "worker"
            ) {
                break;
            }
        }
    })
    .await
    .expect("timed out waiting for task startup event");
    assert!(
        handle
            .snapshot()
            .child("worker")
            .expect("task child exists")
            .state
            .is_running()
    );

    runtime
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn prelude_snapshots_walk_nested_task_children() {
    let (leaf_started_tx, mut leaf_started_rx) = mpsc::unbounded_channel();
    let nested = OrderedTree::new().task(ChildSpec::task("leaf", move |ctx| {
        let leaf_started_tx = leaf_started_tx.clone();
        async move {
            leaf_started_tx.send(()).expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    }));
    let tree = OrderedTree::new()
        .task(ChildSpec::task("anchor", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .subtree("nested", nested);
    let handle = tree.handle();
    let runtime = tree.spawn().expect("nested task tree spawns");

    timeout(EVENT_TIMEOUT, leaf_started_rx.recv())
        .await
        .expect("timed out waiting for nested task")
        .expect("nested task reported startup");
    let snapshot = handle.snapshot();
    assert!(snapshot.descendant(["nested", "leaf"]).is_some());

    runtime
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");
}

#[test]
fn task_policy_types_cover_common_configuration() {
    assert_eq!(kokage::Shutdown::abort(), kokage::Shutdown::abort());
    assert_eq!(
        Restart::on_failure().limit(3, Duration::from_secs(10)),
        Restart::on_failure().limit(3, Duration::from_secs(10))
    );
    assert_eq!(
        Restart::on_failure()
            .limit(2, Duration::from_secs(5))
            .backoff(kokage::Backoff::fixed(Duration::from_millis(50))),
        Restart::on_failure()
            .limit(2, Duration::from_secs(5))
            .backoff(kokage::Backoff::fixed(Duration::from_millis(50)))
    );
}
