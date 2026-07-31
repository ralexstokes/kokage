use std::time::Duration;

use kokage::{
    ActorStatus, RestartMode, RestartPolicy,
    observe::{LifecycleEvent, LifecycleEventKind},
    prelude::*,
};
use tokio::{sync::mpsc, time::timeout};

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use kokage::prelude::{
            Actor, ActorRef, ActorSpec, Context, DynamicTree, ExitResult, Guard, MailboxMode,
            MailboxShutdown, MonitorEvent, OrderedTree, Reply, RestartMode, Shutdown, StopContext,
            Strategy, SupervisorSnapshot, SupervisorSnapshotReceiver, TaskSpec, TimerKey,
        };
    }

    mod advanced_root {
        use kokage::{
            ActorFactory, ActorSlot, Backoff, BlockingCancelled, BoxError, BuildError, CallError,
            CancellationToken, ControlError, ExitStatus, OffloadDeadline, RestartPolicy,
            RunningTree, ScopeRef, SendError, SendErrorKind, SubtreeSpec, SupervisorError,
            TaskContext,
        };
    }

    mod raw {
        use kokage::raw::{
            ActorHost, ActorRunError, DEFAULT_SHUTDOWN_BOUND, IncarnationExit, RawActor, RawContext,
        };
    }

    mod observe {
        use kokage::observe::{
            ActorStats, ChildMembershipView, ChildSnapshot, ChildStateView, CompletionError,
            ExitStatus, LifecycleEvent, LifecycleEventKind, LifecycleObservation, LifecycleWatch,
            ScopeKind, ScopePathSegment, ScopedActorStats, SupervisorSnapshot, SupervisorStateView,
        };
        #[cfg(feature = "serde")]
        use kokage::observe::{ChildOutline, SupervisionOutline};
    }
}

#[test]
fn prelude_constructs_actor_and_task_declarations() {
    let spec = ActorSpec::new("direct", || BlockingWorker {
        observed: mpsc::unbounded_channel().0,
    });
    let task = TaskSpec::new("task", |_| async { Ok(()) });

    let mut tree = OrderedTree::new();
    tree.add_actor(spec);
    tree.add_task(task);
}

#[test]
fn root_actor_slot_constructs_a_cyclic_declaration() {
    let slot = kokage::ActorSlot::new("cyclic");
    let _cyclic_ref = slot.actor_ref();
    let cyclic = slot.define(|| BlockingWorker {
        observed: mpsc::unbounded_channel().0,
    });

    let mut tree = OrderedTree::new();
    tree.add_actor(cyclic);
}

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

fn child_id_is(event: &LifecycleEvent, child_id: &str) -> bool {
    match &event.kind {
        LifecycleEventKind::ChildAdded {
            child_id: observed, ..
        }
        | LifecycleEventKind::ChildStarted {
            child_id: observed, ..
        }
        | LifecycleEventKind::ChildExited {
            child_id: observed, ..
        }
        | LifecycleEventKind::ChildRemoved {
            child_id: observed, ..
        }
        | LifecycleEventKind::ChildRestartScheduled {
            child_id: observed, ..
        } => observed == child_id,
        _ => false,
    }
}

async fn named_task(ctx: kokage::TaskContext) -> kokage::ExitResult {
    ctx.shutdown_token().cancelled().await;
    Ok(())
}

#[test]
fn root_task_surface_supports_a_named_factory_from_the_single_crate() {
    let _child = kokage::TaskSpec::new("worker", named_task);
}

#[test]
fn actor_stats_and_lifecycle_events_share_scope_path_segments() {
    #[allow(dead_code)]
    fn assign_shared_path(
        stats: &mut kokage::observe::ScopedActorStats,
        event: &mut kokage::observe::LifecycleEvent,
        path: Vec<kokage::observe::ScopePathSegment>,
    ) {
        stats.scope_path = path.clone();
        event.scope_path = path;
    }
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

    fn shutdown_name(policy: kokage::Shutdown) -> &'static str {
        match policy {
            kokage::Shutdown::Graceful { .. } => "graceful",
            kokage::Shutdown::Abort => "abort",
            _ => "unknown",
        }
    }

    fn mailbox_shutdown_name(policy: MailboxShutdown) -> &'static str {
        match policy {
            MailboxShutdown::Drain => "drain",
            MailboxShutdown::Discard => "discard",
            _ => "unknown",
        }
    }

    fn backoff_name(backoff: kokage::Backoff) -> &'static str {
        match backoff {
            kokage::Backoff::None => "none",
            kokage::Backoff::Fixed(_) => "fixed",
            kokage::Backoff::Exponential { jitter: false, .. } => "exponential",
            kokage::Backoff::Exponential { jitter: true, .. } => "jittered-exponential",
            _ => "unknown",
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
    assert_eq!(RestartMode::default(), RestartMode::OnFailure);
    assert_eq!(RestartPolicy::default(), RestartPolicy::on_failure());
    assert_eq!(shutdown_name(kokage::Shutdown::default()), "graceful");
    assert_eq!(shutdown_name(kokage::Shutdown::abort()), "abort");
    assert_eq!(mailbox_shutdown_name(MailboxShutdown::default()), "drain");
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

    async fn handle(&mut self, _message: (), ctx: &mut Context<'_, Self>) -> ExitResult {
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
    let mut graph = OrderedTree::new();
    let worker = graph.add_actor(ActorSpec::new("worker", move || BlockingWorker {
        observed: observed_tx.clone(),
    }));

    let handle = graph
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");
    let mut events = handle.scope().watch_lifecycle();
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
                event.kind,
                LifecycleEventKind::ChildStarted { generation: 0, .. }
            ) && child_id_is(&event, "worker")
            {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for started event");
    assert!(matches!(
        started.kind,
        LifecycleEventKind::ChildStarted { generation: 0, .. }
    ));
    assert!(child_id_is(&started, "worker"));

    let snapshot = handle.scope().snapshot();
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
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("worker", move |ctx| {
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    }));
    let handle = tree.scope();
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
                LifecycleEventKind::ChildStarted { generation: 0, .. }
            ) && child_id_is(&event, "worker")
            {
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
    let mut nested = OrderedTree::new();
    nested.add_task(TaskSpec::new("leaf", move |ctx| {
        let leaf_started_tx = leaf_started_tx.clone();
        async move {
            leaf_started_tx.send(()).expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    }));
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("anchor", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    tree.add_subtree("nested", nested);
    let handle = tree.scope();
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
        RestartPolicy::on_failure().limit(3, Duration::from_secs(10)),
        RestartPolicy::on_failure().limit(3, Duration::from_secs(10))
    );
    assert_eq!(
        RestartPolicy::on_failure()
            .limit(2, Duration::from_secs(5))
            .backoff(kokage::Backoff::fixed(Duration::from_millis(50))),
        RestartPolicy::on_failure()
            .limit(2, Duration::from_secs(5))
            .backoff(kokage::Backoff::fixed(Duration::from_millis(50)))
    );
}
