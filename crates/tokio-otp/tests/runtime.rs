use std::{
    future::IntoFuture,
    io,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_otp::{
    Actor, ActorContext, ActorFactory, ActorRef, ActorResult, AddSubtreeError, BoxError,
    DynamicActorOptions, GraphBuilder, RawActor, Reply, Runtime, SendError, SupervisedActors,
    prelude::Continue,
};
use tokio_supervisor::{
    ChildSpec, ChildStateView, ControlError, ExitStatusView, RestartIntensity, RestartPolicy,
    ShutdownPolicy, Strategy, SupervisorBuilder, SupervisorError, SupervisorStateView,
};

struct Drain<M>(PhantomData<fn(M)>);

impl<M> Drain<M> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Clone for Drain<M> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Drain<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: ActorContext<M>) -> ActorResult {
        while ctx.recv().await.is_some() {}
        Ok(Continue)
    }
}

#[derive(Clone)]
struct Observe {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for Observe {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(Continue)
    }
}

#[derive(Clone)]
struct ObserveOnce {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for ObserveOnce {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        let message = ctx.recv().await.expect("message received before shutdown");
        self.observed.send(message).expect("receiver alive");
        Ok(Continue)
    }
}

fn build_runtime<F>(factory: F) -> (Runtime, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor("worker", factory);
    let graph = builder.build().expect("valid graph");

    let runtime = SupervisedActors::new(graph)
        .build_runtime(SupervisorBuilder::new().strategy(Strategy::OneForOne))
        .expect("runtime builds");

    (runtime, actor_ref)
}

#[tokio::test]
async fn runtime_spawn_combines_actor_refs_and_supervisor_control() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || Observe {
        observed: observed_tx.clone(),
    });

    let handle = runtime.spawn();
    let supervisor_handle = handle.supervisor_handle();

    assert_eq!(
        supervisor_handle.snapshot().state,
        SupervisorStateView::Running
    );
    assert_eq!(handle.snapshot().children.len(), 1);
    worker_ref
        .send("hello".to_owned())
        .await
        .expect("message sent");

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "hello");

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn runtime_handle_enumerates_actor_stats() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || Observe {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    worker_ref
        .send("counted".to_owned())
        .await
        .expect("message sent");
    observed_rx.recv().await.expect("message received");

    let stats = handle.actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].actor_id, worker_ref.id());
    assert_eq!(stats[0].supervisor_path, Some(Vec::new()));
    assert_eq!(
        stats[0].membership_epoch,
        Some(
            handle
                .snapshot()
                .child(worker_ref.id())
                .expect("worker snapshot available")
                .membership_epoch
        )
    );
    assert_eq!(stats[0].messages_accepted, 1);
    assert_eq!(stats[0].messages_received, 1);

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn runtime_builder_composes_subtrees_with_recursive_actor_stats() {
    let mut root_graph = GraphBuilder::new();
    let root_ref = root_graph.actor("root-worker", Drain::<()>::new);
    let mut nested_graph = GraphBuilder::new();
    let nested_ref = nested_graph.actor("nested-worker", Drain::<()>::new);
    let mut leaf_graph = GraphBuilder::new();
    let leaf_ref = leaf_graph.actor("leaf-worker", Drain::<()>::new);

    let runtime = Runtime::builder()
        .graph(root_graph.build().expect("valid root graph"))
        .subtree(
            "workers",
            Runtime::builder()
                .graph(nested_graph.build().expect("valid nested graph"))
                .subtree(
                    "leaf",
                    Runtime::builder().graph(leaf_graph.build().expect("valid leaf graph")),
                ),
        )
        .build()
        .expect("nested runtime builds");
    let handle = runtime.spawn();
    handle.wait_started().await.expect("runtime started");

    root_ref.send(()).await.expect("root message sent");
    nested_ref.send(()).await.expect("nested message sent");
    leaf_ref.send(()).await.expect("leaf message sent");

    let actor_ids = handle
        .actor_stats()
        .into_iter()
        .map(|stats| stats.actor_id)
        .collect::<Vec<_>>();
    assert_eq!(actor_ids, ["root-worker", "nested-worker", "leaf-worker"]);

    let subtree = handle.subtree("workers").expect("actor-aware subtree");
    let nested_epoch = subtree
        .snapshot()
        .child("nested-worker")
        .expect("nested actor snapshot available")
        .membership_epoch;
    assert_eq!(
        handle
            .actor_stats()
            .into_iter()
            .find(|stats| stats.actor_id == "nested-worker")
            .expect("nested actor stats available")
            .membership_epoch,
        Some(nested_epoch)
    );
    assert_eq!(
        subtree
            .actor_stats()
            .into_iter()
            .map(|stats| stats.actor_id)
            .collect::<Vec<_>>(),
        [nested_ref.id(), leaf_ref.id()]
    );
    assert_eq!(
        subtree
            .subtree("leaf")
            .expect("recursive actor-aware subtree")
            .actor_stats()
            .into_iter()
            .map(|stats| stats.actor_id)
            .collect::<Vec<_>>(),
        [leaf_ref.id()]
    );

    handle
        .add_supervisor(
            "raw",
            SupervisorBuilder::new()
                .build()
                .expect("raw supervisor builds"),
        )
        .await
        .expect("raw supervisor added");
    assert!(handle.subtree("raw").is_none());

    let dynamic = subtree
        .add_actor(
            "dynamic-worker",
            Drain::<()>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("nested actor added");
    dynamic.send(()).await.expect("dynamic message sent");
    let dynamic_epoch = subtree
        .snapshot()
        .child("dynamic-worker")
        .expect("dynamic actor snapshot available")
        .membership_epoch;
    assert!(
        handle.actor_stats().iter().any(|stats| {
            stats.actor_id == "dynamic-worker" && stats.membership_epoch == Some(dynamic_epoch)
        }),
        "parent stats recursively include actors added through a subtree handle"
    );

    subtree
        .supervisor_handle()
        .remove_child("dynamic-worker")
        .await
        .expect("nested actor removed through raw handle");
    assert!(
        handle
            .actor_stats()
            .iter()
            .all(|stats| stats.actor_id != "dynamic-worker")
    );

    handle
        .supervisor_handle()
        .remove_child("workers")
        .await
        .expect("subtree removed through raw handle");
    let stats = handle.actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].actor_id, root_ref.id());
    assert!(handle.subtree("workers").is_none());

    handle
        .remove_child("raw")
        .await
        .expect("raw supervisor removed");

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn dynamic_subtree_preserves_static_and_dynamic_actor_metadata() {
    let mut graph = GraphBuilder::new();
    let static_ref = graph.actor("static-worker", Drain::<()>::new);
    let root = Runtime::builder().build().expect("runtime builds").spawn();

    let subtree = root
        .add_subtree(
            "workers",
            Runtime::builder().graph(graph.build().expect("graph builds")),
        )
        .await
        .expect("subtree added");
    subtree.wait_started().await.expect("subtree started");
    let dynamic_ref = subtree
        .add_actor(
            "dynamic-worker",
            Drain::<()>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("dynamic actor added");

    static_ref.send(()).await.expect("static actor receives");
    dynamic_ref.send(()).await.expect("dynamic actor receives");
    assert!(root.subtree("workers").is_some());
    assert_eq!(
        root.actor_stats()
            .into_iter()
            .map(|stats| stats.actor_id)
            .collect::<Vec<_>>(),
        [static_ref.id(), dynamic_ref.id()]
    );
    assert!(
        root.actor_stats()
            .iter()
            .all(|stats| stats.membership_epoch.is_some()),
        "builder-created and dynamically added actors both have bound epochs"
    );

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_subtrees_can_nest_and_removal_terminates_retained_handles() {
    let root = Runtime::builder().build().expect("runtime builds").spawn();
    let middle = root
        .add_subtree("middle", Runtime::builder())
        .await
        .expect("middle subtree added");
    let leaf = middle
        .add_subtree("leaf", Runtime::builder())
        .await
        .expect("leaf subtree added");
    let actor = leaf
        .add_actor("worker", Drain::<()>::new, DynamicActorOptions::default())
        .await
        .expect("nested actor added");
    actor.send(()).await.expect("nested actor receives");

    assert_eq!(root.actor_stats().len(), 1);
    assert!(middle.subtree("leaf").is_some());
    root.remove_child("middle")
        .await
        .expect("middle subtree removed");
    assert!(root.subtree("middle").is_none());
    assert!(root.actor_stats().is_empty());
    assert!(matches!(
        leaf.add_actor("late", Drain::<()>::new, DynamicActorOptions::default())
            .await,
        Err(ControlError::Unavailable)
    ));

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn duplicate_dynamic_subtree_id_leaves_registry_unchanged() {
    let root = Runtime::builder().build().expect("runtime builds").spawn();
    let first = root
        .add_subtree("workers", Runtime::builder())
        .await
        .expect("first subtree added");
    first
        .add_actor("worker", Drain::<()>::new, DynamicActorOptions::default())
        .await
        .expect("actor added");

    let error = root
        .add_subtree("workers", Runtime::builder())
        .await
        .expect_err("duplicate subtree rejected");
    assert_eq!(
        error,
        AddSubtreeError::Control(ControlError::DuplicateChildId("workers".to_owned()))
    );
    assert_eq!(root.actor_stats().len(), 1);
    assert!(root.subtree("workers").is_some());

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn recursive_stats_distinguish_duplicate_actor_ids_in_sibling_subtrees() {
    let mut left_graph = GraphBuilder::new();
    left_graph.actor("worker", Drain::<()>::new);
    let mut right_graph = GraphBuilder::new();
    right_graph.actor("worker", Drain::<()>::new);

    let handle = Runtime::builder()
        .subtree(
            "left",
            Runtime::builder().graph(left_graph.build().expect("left graph builds")),
        )
        .subtree(
            "right",
            Runtime::builder().graph(right_graph.build().expect("right graph builds")),
        )
        .build()
        .expect("runtime builds")
        .spawn();
    handle.wait_started().await.expect("runtime started");

    let stats = handle.actor_stats();
    assert_eq!(stats.len(), 2);
    assert!(stats.iter().all(|stats| stats.actor_id == "worker"));
    assert!(stats.iter().all(|stats| stats.membership_epoch == Some(0)));

    let paths = stats
        .iter()
        .map(|stats| {
            let path = stats
                .supervisor_path
                .as_ref()
                .expect("runtime stats carry a supervisor path");
            assert_eq!(path.len(), 1);
            (
                path[0].id.as_str(),
                path[0].membership_epoch,
                path[0].generation,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, [("left", 0, 0), ("right", 1, 0)]);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_same_id_replacement_cannot_inherit_tracked_actor_stats() {
    let handle = Runtime::builder().build().expect("runtime builds").spawn();
    let tracked = handle
        .add_actor("worker", Drain::<()>::new, DynamicActorOptions::default())
        .await
        .expect("tracked actor added");
    tracked.send(()).await.expect("tracked actor receives");
    let tracked_epoch = handle
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == "worker")
        .and_then(|stats| stats.membership_epoch)
        .expect("tracked membership epoch available");

    let sampler_handle = handle.clone();
    let sampler = tokio::spawn(async move {
        for _ in 0..1_000 {
            for stats in sampler_handle.actor_stats() {
                if stats.actor_id == "worker" {
                    assert_eq!(
                        stats.membership_epoch,
                        Some(tracked_epoch),
                        "a replacement membership must never receive the old actor's counters"
                    );
                }
            }
            tokio::task::yield_now().await;
        }
    });

    handle
        .supervisor_handle()
        .remove_child("worker")
        .await
        .expect("tracked actor removed through raw handle");
    handle
        .supervisor_handle()
        .add_child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("untracked replacement added");

    sampler.await.expect("sampler completed");
    assert!(
        handle
            .actor_stats()
            .iter()
            .all(|stats| stats.actor_id != "worker"),
        "the stale registry entry is filtered by its retained membership epoch"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn recursive_stats_prune_dynamic_actors_lost_on_subtree_restart() {
    let mut nested_graph = GraphBuilder::new();
    let static_ref = nested_graph.actor("static-worker", || FailOnMessage);
    let runtime = Runtime::builder()
        .subtree(
            "workers",
            Runtime::builder()
                .graph(nested_graph.build().expect("valid nested graph"))
                .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("nested runtime builds");
    let handle = runtime.spawn();
    handle.wait_started().await.expect("runtime started");

    let subtree = handle.subtree("workers").expect("actor-aware subtree");
    let dynamic_ref = subtree
        .add_actor(
            "dynamic-worker",
            Drain::<()>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("dynamic actor added");
    assert!(
        handle
            .actor_stats()
            .iter()
            .any(|stats| stats.actor_id == "dynamic-worker")
    );

    let restart = handle
        .monitor_restart("workers")
        .expect("subtree child is known");
    static_ref.send(()).await.expect("failure triggered");
    timeout(Duration::from_secs(1), restart.into_future())
        .await
        .expect("subtree restarted within timeout")
        .expect("subtree restart succeeded");

    let stats = handle.actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].actor_id, static_ref.id());
    assert!(matches!(
        dynamic_ref.send(()).await,
        Err(SendError::ActorTerminated { .. })
    ));

    subtree
        .supervisor_handle()
        .add_child(ChildSpec::new("dynamic-worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("same-id raw child added in replacement subtree incarnation");
    assert!(
        handle
            .actor_stats()
            .iter()
            .all(|stats| stats.actor_id != "dynamic-worker"),
        "the old dynamic registry entry must not attach to a reused local epoch"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn dynamic_subtree_restart_recreates_only_builder_membership() {
    let mut graph = GraphBuilder::new();
    let static_ref = graph.actor("static-worker", || FailOnMessage);
    let root = Runtime::builder().build().expect("runtime builds").spawn();
    let subtree = root
        .add_subtree(
            "workers",
            Runtime::builder()
                .graph(graph.build().expect("graph builds"))
                .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .await
        .expect("subtree added");
    subtree.wait_started().await.expect("subtree started");
    let dynamic_ref = subtree
        .add_actor(
            "dynamic-worker",
            Drain::<()>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("dynamic actor added");
    assert_eq!(root.actor_stats().len(), 2);

    let restart = root
        .monitor_restart("workers")
        .expect("subtree is monitored");
    static_ref.send(()).await.expect("failure triggered");
    timeout(Duration::from_secs(1), restart.into_future())
        .await
        .expect("subtree restarted within timeout")
        .expect("subtree restart succeeded");

    let stats = root.actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].actor_id, static_ref.id());
    assert!(matches!(
        dynamic_ref.send(()).await,
        Err(SendError::ActorTerminated { .. })
    ));

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn parent_restart_drops_dynamic_subtree_and_allows_same_id_replay() {
    let mut parent_graph = GraphBuilder::new();
    let fuse = parent_graph.actor("fuse", || FailOnMessage);
    let root = Runtime::builder()
        .subtree(
            "parent",
            Runtime::builder()
                .graph(parent_graph.build().expect("graph builds"))
                .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("runtime builds")
        .spawn();
    root.wait_started().await.expect("runtime started");
    let parent = root.subtree("parent").expect("parent subtree available");
    let dynamic = parent
        .add_subtree("dynamic", Runtime::builder())
        .await
        .expect("dynamic subtree added");
    dynamic
        .add_actor("worker", Drain::<()>::new, DynamicActorOptions::default())
        .await
        .expect("dynamic actor added");
    assert!(
        root.actor_stats()
            .iter()
            .any(|stats| stats.actor_id == "worker")
    );

    let restart = root
        .monitor_restart("parent")
        .expect("parent subtree is monitored");
    fuse.send(()).await.expect("parent failure triggered");
    timeout(Duration::from_secs(1), restart.into_future())
        .await
        .expect("parent restarted within timeout")
        .expect("parent restart succeeded");

    assert!(
        root.actor_stats()
            .iter()
            .all(|stats| stats.actor_id != "worker")
    );
    assert!(matches!(
        dynamic
            .add_actor("late", Drain::<()>::new, DynamicActorOptions::default())
            .await,
        Err(ControlError::Unavailable)
    ));
    let restarted_parent = root.subtree("parent").expect("parent rebound");
    assert!(restarted_parent.subtree("dynamic").is_none());
    let replayed = restarted_parent
        .add_subtree("dynamic", Runtime::builder())
        .await
        .expect("same id can be replayed");
    replayed
        .add_actor(
            "replacement",
            Drain::<()>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("replacement actor added");
    assert!(
        root.actor_stats()
            .iter()
            .any(|stats| stats.actor_id == "replacement")
    );

    root.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct FailAfterObserve {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for FailAfterObserve {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        match ctx.recv().await {
            Some(message) => {
                self.observed.send(message).expect("receiver alive");
                Err("deliberate failure".into())
            }
            None => Ok(Continue),
        }
    }
}

#[tokio::test]
async fn actor_stats_accumulate_across_supervised_restarts() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || FailAfterObserve {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    worker_ref
        .send("first".to_owned())
        .await
        .expect("first message sent");
    observed_rx.recv().await.expect("first message observed");

    // The incarnation fails after observing; `send` waits for the restarted
    // incarnation's mailbox to bind before delivering.
    timeout(Duration::from_secs(5), worker_ref.send("second".to_owned()))
        .await
        .expect("rebind within timeout")
        .expect("second message sent");
    observed_rx.recv().await.expect("second message observed");

    let restarted = handle
        .snapshot()
        .child("worker")
        .map_or(0, |child| child.restart_count);
    assert!(restarted >= 1, "worker should have restarted");

    // Counters accumulate across incarnations; mailbox fields describe only
    // the current binding (and are zero in the window between incarnations),
    // so only the counters are asserted here.
    let stats = worker_ref.stats();
    assert_eq!(stats.messages_received, 2);
    assert_eq!(stats.messages_accepted, 2);
    assert_eq!(stats.sends_rejected, 0);

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn runtime_into_supervisor_spawn_accepts_ref_cloned_before_startup() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || ObserveOnce {
        observed: observed_tx.clone(),
    });

    let handle = runtime.into_supervisor().spawn();
    let mut snapshots = handle.subscribe_snapshots();
    let sender = tokio::spawn(async move {
        worker_ref
            .send("run-path".to_owned())
            .await
            .expect("message sent through cloned ref");
    });

    sender.await.expect("sender task joined");

    let completed = snapshots
        .wait_for(|snapshot| {
            snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Stopped)
        })
        .await
        .expect("completion snapshot remains available")
        .clone();
    assert!(matches!(
        completed
            .child("worker")
            .expect("worker remains visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Completed)
    ));

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "run-path");
    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn runtime_spawn_wait_drives_to_completion_with_control_surface() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || ObserveOnce {
        observed: observed_tx.clone(),
    });

    let handle = runtime.spawn();
    let control = handle.clone();

    control
        .subscribe_snapshots()
        .wait_for(|snapshot| {
            snapshot
                .children
                .iter()
                .all(|child| child.state == ChildStateView::Running)
        })
        .await
        .expect("runtime reported running");
    let _events = control.subscribe();
    assert_eq!(control.snapshot().children.len(), 1);

    worker_ref
        .send("spawn-wait-path".to_owned())
        .await
        .expect("message sent through cloned ref");

    let mut snapshots = control.subscribe_snapshots();
    let completed = snapshots
        .wait_for(|snapshot| {
            snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Stopped)
        })
        .await
        .expect("completion snapshot remains available")
        .clone();
    assert!(matches!(
        completed
            .child("worker")
            .expect("worker remains visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Completed)
    ));

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "spawn-wait-path");
    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn runtime_builder_wires_graph_into_supervised_runtime() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let worker_ref = builder.actor("worker", move || Observe {
        observed: observed_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();

    worker_ref
        .send("built".to_owned())
        .await
        .expect("message sent");

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "built");

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn snapshot_wait_reports_all_children_running_after_spawn() {
    let mut builder = GraphBuilder::new();
    builder.actor("one", Drain::<()>::new);
    builder.actor("two", Drain::<()>::new);
    let graph = builder.build().expect("valid graph");

    let runtime = SupervisedActors::new(graph)
        .build_runtime(SupervisorBuilder::new().strategy(Strategy::OneForOne))
        .expect("runtime builds");
    let handle = runtime.spawn();

    let mut snapshots = handle.subscribe_snapshots();
    let all_running = snapshots.wait_for(|snapshot| {
        snapshot.children.len() == 2
            && snapshot
                .children
                .iter()
                .all(|child| child.state == ChildStateView::Running)
    });
    timeout(Duration::from_secs(1), all_running)
        .await
        .expect("runtime reported running")
        .expect("snapshot channel stays open");
    assert_eq!(handle.snapshot().children.len(), 2);

    handle
        .shutdown_and_wait()
        .await
        .expect("runtime shut down cleanly");
}

#[derive(Clone)]
struct FailOnMessage;

impl RawActor for FailOnMessage {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<()>) -> ActorResult {
        if ctx.recv().await.is_some() {
            return Err::<_, BoxError>(Box::new(io::Error::other("boom")));
        }

        Ok(Continue)
    }
}

#[tokio::test]
async fn runtime_handle_monitor_restart_delegates_to_supervisor() {
    let mut builder = GraphBuilder::new();
    let worker_ref = builder.actor("worker", || FailOnMessage);
    let graph = builder.build().expect("valid graph");

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .restart(RestartPolicy::OnFailure)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();

    let restart = handle
        .monitor_restart("worker")
        .expect("worker child should be known");
    worker_ref.send(()).await.expect("message sent");

    let generation = timeout(Duration::from_secs(1), restart.into_future())
        .await
        .expect("restart monitor should resolve")
        .expect("restart monitor should succeed");
    assert_eq!(generation, 1);

    handle
        .shutdown_and_wait()
        .await
        .expect("runtime shut down cleanly");
}

#[derive(Clone)]
struct AlwaysFails;

impl RawActor for AlwaysFails {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        Err::<_, BoxError>(Box::new(io::Error::other("boom")))
    }
}

#[tokio::test]
async fn send_fails_after_restart_intensity_is_exhausted() {
    let mut builder = GraphBuilder::new();
    let worker_ref = builder.actor("worker", || AlwaysFails);
    let graph = builder.build().expect("valid graph");

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .restart(RestartPolicy::Always)
        .restart_intensity(RestartIntensity::new(1, Duration::from_secs(60)))
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();

    // The crash loop exhausts the restart budget and the supervisor gives up.
    let _ = timeout(Duration::from_secs(2), handle.wait())
        .await
        .expect("supervisor gave up");

    // A rebind will never come; send must not wait for one.
    let result = timeout(Duration::from_millis(500), worker_ref.send(()))
        .await
        .expect("send resolved after the supervisor gave up");
    assert!(matches!(result, Err(SendError::ActorTerminated { .. })));
}

enum CounterMsg {
    Add(u32),
    Total(Reply<u32>),
    Crash,
}

#[derive(Clone)]
struct ResettingCounter {
    total: u32,
    on_starts: Arc<AtomicUsize>,
}

impl Actor for ResettingCounter {
    type Msg = CounterMsg;

    async fn on_start(&mut self, _ctx: &ActorContext<CounterMsg>) -> ActorResult {
        self.on_starts.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: CounterMsg,
        _ctx: &ActorContext<CounterMsg>,
    ) -> ActorResult {
        match message {
            CounterMsg::Add(n) => {
                self.total += n;
                Ok(Continue)
            }
            CounterMsg::Total(reply) => {
                reply.send(self.total);
                Ok(Continue)
            }
            CounterMsg::Crash => Err("deliberate crash".into()),
        }
    }
}

/// A supervised restart invokes the factory again, so the new incarnation
/// starts from fresh ordinary state and `on_start` runs once per incarnation.
#[tokio::test]
async fn supervised_restart_constructs_fresh_actor_state() {
    let on_starts = Arc::new(AtomicUsize::new(0));
    let mut builder = GraphBuilder::new();
    let counter = builder.actor("counter", {
        let on_starts = on_starts.clone();
        move || ResettingCounter {
            total: 0,
            on_starts: on_starts.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let runtime = Runtime::builder()
        .graph(graph)
        .restart(RestartPolicy::Always)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();

    counter.send(CounterMsg::Add(5)).await.expect("add sent");
    assert_eq!(
        counter
            .call(Duration::from_secs(1), CounterMsg::Total)
            .await
            .expect("total replied"),
        5
    );

    let restart = handle
        .monitor_restart("counter")
        .expect("counter child should be known");
    counter.send(CounterMsg::Crash).await.expect("crash sent");
    timeout(Duration::from_secs(2), restart.into_future())
        .await
        .expect("restart observed")
        .expect("restart monitor succeeded");

    assert_eq!(
        counter
            .call(Duration::from_secs(2), CounterMsg::Total)
            .await
            .expect("total replied after restart"),
        0,
        "restart receives freshly constructed state"
    );
    assert_eq!(
        on_starts.load(Ordering::SeqCst),
        2,
        "on_start runs once per incarnation"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[test]
fn runtime_builder_allows_an_empty_runtime() {
    Runtime::builder().build().expect("empty runtime builds");
}

#[tokio::test]
async fn runtime_into_supervisor_round_trips_supervisor() {
    let (runtime, _worker_ref) = build_runtime(Drain::<()>::new);

    let supervisor = runtime.into_supervisor();
    let runtime = Runtime::new(supervisor);
    let handle = runtime.spawn();

    timeout(Duration::from_secs(1), handle.shutdown_and_wait())
        .await
        .expect("shutdown completed")
        .expect("supervisor shut down cleanly");
}

#[derive(Clone)]
struct PendingActor {
    started: Arc<Notify>,
}

impl RawActor for PendingActor {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn actor_deadline_can_complete_before_strict_supervisor_deadline() {
    let started = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    builder.actor_shutdown_timeout(Duration::from_millis(20));
    builder.actor("worker", {
        let started = started.clone();
        move || PendingActor {
            started: started.clone(),
        }
    });
    let runtime = SupervisedActors::new(builder.build().expect("valid graph"))
        .shutdown(ShutdownPolicy::cooperative_strict(Duration::from_secs(1)))
        .build_runtime(SupervisorBuilder::new())
        .expect("runtime builds");
    let handle = runtime.spawn();

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("actor started");
    handle
        .shutdown_and_wait()
        .await
        .expect("inner actor timeout completed the outer child cleanly");
}

#[tokio::test]
async fn strict_supervisor_deadline_can_abort_before_actor_deadline() {
    let started = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    builder.actor_shutdown_timeout(Duration::from_secs(1));
    builder.actor("worker", {
        let started = started.clone();
        move || PendingActor {
            started: started.clone(),
        }
    });
    let runtime = SupervisedActors::new(builder.build().expect("valid graph"))
        .shutdown(ShutdownPolicy::cooperative_strict(Duration::from_millis(
            20,
        )))
        .build_runtime(SupervisorBuilder::new())
        .expect("runtime builds");
    let handle = runtime.spawn();

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("actor started");
    assert!(matches!(
        handle.shutdown_and_wait().await,
        Err(SupervisorError::ShutdownTimedOut(actor_id)) if actor_id == "worker"
    ));
}

#[tokio::test]
async fn aborting_supervisor_deadline_can_complete_before_actor_deadline() {
    let started = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    builder.actor_shutdown_timeout(Duration::from_secs(1));
    builder.actor("worker", {
        let started = started.clone();
        move || PendingActor {
            started: started.clone(),
        }
    });
    let runtime = SupervisedActors::new(builder.build().expect("valid graph"))
        .shutdown(ShutdownPolicy::cooperative_then_abort(
            Duration::from_millis(20),
        ))
        .build_runtime(SupervisorBuilder::new())
        .expect("runtime builds");
    let handle = runtime.spawn();

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("actor started");
    handle
        .shutdown_and_wait()
        .await
        .expect("outer abort is a successful supervisor shutdown");
}

#[tokio::test]
async fn handle_actor_stats_track_graph_and_runtime_added_actors() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, worker_ref) = build_runtime(move || Observe {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    worker_ref
        .send("count me".to_owned())
        .await
        .expect("message sent");
    observed_rx.recv().await.expect("message observed");

    // Graph actors are visible in the runtime's stats without any dynamic
    // actor having been added.
    let stats = handle.actor_stats();
    let worker = stats
        .iter()
        .find(|stats| stats.actor_id == "worker")
        .expect("graph actor reported in runtime stats");
    assert_eq!(worker.messages_accepted, 1);

    let extra = handle
        .add_actor("extra", Drain::<()>::new, DynamicActorOptions::default())
        .await
        .expect("actor added");
    extra.send(()).await.expect("message sent");

    let stats = handle.actor_stats();
    assert_eq!(stats.len(), 2);
    let extra_stats = stats
        .iter()
        .find(|stats| stats.actor_id == "extra")
        .expect("runtime-added actor reported in runtime stats");
    assert_eq!(extra_stats.messages_accepted, 1);

    handle.remove_child("extra").await.expect("actor removed");
    let stats = handle.actor_stats();
    assert!(
        stats.iter().all(|stats| stats.actor_id != "extra"),
        "removed actor no longer reported"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}
