mod support;

use support::TreeBuilder;

use std::{
    io,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorFactory, ActorRef, ActorSlot, ActorSpec, BoxError, BuildError, Context,
    ControlError, DynamicTree, ExitResult, Reply, RestartPolicy, ScopeRef, SendError, Shutdown,
    Strategy, SupervisorError, TaskSpec, Tree,
    observe::{
        ChildEventKind, LifecycleEventKind, SupervisorSnapshotReceiver, SupervisorStateView,
    },
    raw::{RawActor, RawContext},
};
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
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

    async fn run(&mut self, ctx: &mut RawContext<M>) -> ExitResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[derive(Clone)]
struct Observe {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for Observe {
    type Msg = String;

    async fn run(&mut self, ctx: &mut RawContext<String>) -> ExitResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ObserveOnce {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for ObserveOnce {
    type Msg = String;

    async fn run(&mut self, ctx: &mut RawContext<String>) -> ExitResult {
        let message = ctx.recv().await.expect("message received before shutdown");
        self.observed.send(message).expect("receiver alive");
        Ok(())
    }
}

fn build_runtime<F>(factory: F) -> (Tree, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = TreeBuilder::new();
    let actor_ref_slot = ActorSlot::new("worker");
    let actor_ref = actor_ref_slot.actor_ref();
    builder.define(actor_ref_slot, factory);
    let graph = builder.build();

    let tree = graph.strategy(Strategy::OneForOne);

    (tree, actor_ref)
}

fn restart_observer(handle: &ScopeRef, id: &str) -> (SupervisorSnapshotReceiver, u64) {
    let snapshots = handle.subscribe_snapshots();
    let baseline = handle
        .snapshot()
        .child(id)
        .unwrap_or_else(|| panic!("{id} is a direct child"))
        .generation;
    (snapshots, baseline)
}

async fn await_restart(mut snapshots: SupervisorSnapshotReceiver, id: &str, baseline: u64) -> u64 {
    snapshots
        .wait_for_child(id, |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .expect("runtime remains live")
        .generation
}

#[tokio::test]
async fn runtime_spawn_combines_actor_refs_and_supervisor_control() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, worker_ref) = build_runtime(move || Observe {
        observed: observed_tx.clone(),
    });

    let handle = tree.spawn().expect("runtime builds");

    assert_eq!(
        handle.scope().snapshot().state,
        SupervisorStateView::Running
    );
    assert_eq!(handle.scope().snapshot().children.len(), 1);
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
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn runtime_handle_enumerates_actor_stats() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, worker_ref) = build_runtime(move || Observe {
        observed: observed_tx.clone(),
    });
    let handle = tree.spawn().expect("runtime builds");

    worker_ref
        .send("counted".to_owned())
        .await
        .expect("message sent");
    observed_rx.recv().await.expect("message received");

    let stats = handle.scope().actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].stats.actor_id, worker_ref.id());
    assert_eq!(stats[0].scope_path, Vec::new());
    assert_eq!(
        stats[0].lineage,
        handle
            .scope()
            .snapshot()
            .child(worker_ref.id())
            .expect("worker snapshot available")
            .lineage
    );
    assert_eq!(stats[0].stats.messages_accepted, 1);
    assert_eq!(stats[0].stats.messages_received, 1);

    handle
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn supervision_tree_composes_subtrees_with_recursive_actor_stats() {
    let mut root_graph = TreeBuilder::new();
    let root_ref_slot = ActorSlot::new("root-worker");
    let root_ref = root_ref_slot.actor_ref();
    root_graph.define(root_ref_slot, Drain::<()>::new);
    let mut nested_graph = TreeBuilder::new();
    let nested_ref_slot = ActorSlot::new("nested-worker");
    let nested_ref = nested_ref_slot.actor_ref();
    nested_graph.define(nested_ref_slot, Drain::<()>::new);
    let mut leaf_graph = TreeBuilder::new();
    let leaf_ref_slot = ActorSlot::new("leaf-worker");
    let leaf_ref = leaf_ref_slot.actor_ref();
    leaf_graph.define(leaf_ref_slot, Drain::<()>::new);

    let mut root_graph = root_graph.build();
    let mut nested_graph = nested_graph.build();
    let leaf_graph = leaf_graph.build();
    nested_graph.add_subtree("dynamic", DynamicTree::new());
    nested_graph.add_subtree("leaf", leaf_graph);
    root_graph.add_subtree("workers", nested_graph);
    root_graph.add_subtree("raw-members", DynamicTree::new());
    let handle = root_graph.spawn().expect("nested runtime builds");
    handle
        .scope()
        .wait_started()
        .await
        .expect("runtime started");

    root_ref.send(()).await.expect("root message sent");
    nested_ref.send(()).await.expect("nested message sent");
    leaf_ref.send(()).await.expect("leaf message sent");

    let actor_ids = handle
        .scope()
        .actor_stats()
        .into_iter()
        .map(|stats| stats.stats.actor_id)
        .collect::<Vec<_>>();
    assert_eq!(actor_ids, ["root-worker", "nested-worker", "leaf-worker"]);

    let subtree = handle
        .scope()
        .subtree("workers")
        .expect("actor-aware subtree");
    let nested_lineage = subtree
        .snapshot()
        .child("nested-worker")
        .expect("nested actor snapshot available")
        .lineage;
    assert_eq!(
        handle
            .scope()
            .actor_stats()
            .into_iter()
            .find(|stats| stats.stats.actor_id == "nested-worker")
            .expect("nested actor stats available")
            .lineage,
        nested_lineage
    );
    assert_eq!(
        subtree
            .actor_stats()
            .into_iter()
            .map(|stats| stats.stats.actor_id)
            .collect::<Vec<_>>(),
        [nested_ref.id(), leaf_ref.id()]
    );
    assert_eq!(
        subtree
            .subtree("leaf")
            .expect("recursive actor-aware subtree")
            .actor_stats()
            .into_iter()
            .map(|stats| stats.stats.actor_id)
            .collect::<Vec<_>>(),
        [leaf_ref.id()]
    );

    let raw_members = handle
        .scope()
        .subtree("raw-members")
        .and_then(|scope| scope.dynamic())
        .expect("dynamic raw-members subtree");
    raw_members
        .add_task_spec(TaskSpec::new("raw", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("raw task child added");
    assert!(raw_members.subtree("raw").is_none());

    let dynamic_scope = subtree
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree");
    let dynamic = dynamic_scope
        .add_actor_spec(ActorSpec::new("dynamic-worker", Drain::<()>::new))
        .await
        .expect("nested actor added");
    dynamic.send(()).await.expect("dynamic message sent");
    let dynamic_lineage = dynamic_scope
        .snapshot()
        .child("dynamic-worker")
        .expect("dynamic actor snapshot available")
        .lineage;
    assert!(
        handle.scope().actor_stats().iter().any(|stats| {
            stats.stats.actor_id == "dynamic-worker" && stats.lineage == dynamic_lineage
        }),
        "parent stats recursively include actors added through a subtree handle"
    );

    dynamic_scope
        .remove(&dynamic)
        .await
        .expect("nested actor removed through runtime handle");
    assert!(
        handle
            .scope()
            .actor_stats()
            .iter()
            .all(|stats| stats.stats.actor_id != "dynamic-worker")
    );

    raw_members
        .remove_named("raw")
        .await
        .expect("raw supervisor removed");

    handle
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn dynamic_subtree_preserves_static_and_dynamic_actor_metadata() {
    let mut graph = TreeBuilder::new();
    let static_ref_slot = ActorSlot::new("static-worker");
    let static_ref = static_ref_slot.actor_ref();
    graph.define(static_ref_slot, Drain::<()>::new);
    let root = DynamicTree::new().spawn().expect("runtime builds");

    let mut graph = graph.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let subtree = support::dynamic_root(&root)
        .add_subtree("workers", graph)
        .await
        .expect("subtree added");
    subtree.wait_started().await.expect("subtree started");
    let dynamic = subtree
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree");
    let dynamic_ref = dynamic
        .add_actor_spec(ActorSpec::new("dynamic-worker", Drain::<()>::new))
        .await
        .expect("dynamic actor added");

    static_ref.send(()).await.expect("static actor receives");
    dynamic_ref.send(()).await.expect("dynamic actor receives");
    assert!(root.scope().subtree("workers").is_some());
    assert_eq!(
        root.scope()
            .actor_stats()
            .into_iter()
            .map(|stats| stats.stats.actor_id)
            .collect::<Vec<_>>(),
        [static_ref.id(), dynamic_ref.id()]
    );
    assert_eq!(root.scope().actor_stats().len(), 2);

    root.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_subtrees_can_nest_and_removal_terminates_retained_handles() {
    let root = DynamicTree::new().spawn().expect("runtime builds");
    let middle_tree = DynamicTree::new();
    let middle = middle_tree.scope();
    support::dynamic_root(&root)
        .add_subtree("middle", middle_tree)
        .await
        .expect("middle subtree added");
    let leaf_tree = DynamicTree::new();
    let leaf = leaf_tree.scope();
    middle
        .add_subtree("leaf", leaf_tree)
        .await
        .expect("leaf subtree added");
    let actor = leaf
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("nested actor added");
    actor.send(()).await.expect("nested actor receives");

    assert_eq!(root.scope().actor_stats().len(), 1);
    assert!(middle.subtree("leaf").is_some());
    support::dynamic_root(&root)
        .remove_named("middle")
        .await
        .expect("middle subtree removed");
    assert!(root.scope().subtree("middle").is_none());
    assert!(root.scope().actor_stats().is_empty());
    assert!(middle.subtree("leaf").is_none());
    assert!(middle.actor_stats().is_empty());
    assert!(leaf.actor_stats().is_empty());
    assert!(matches!(
        leaf.add_actor_spec(ActorSpec::new("late", Drain::<()>::new))
            .await,
        Err(ControlError::Unavailable)
    ));

    root.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn subtree_validation_phases_report_rejected() {
    let root = DynamicTree::new().spawn().expect("runtime builds");

    let mut invalid = Tree::new();
    invalid.add_task_spec(TaskSpec::new("duplicate", |_| async { Ok(()) }));
    invalid.add_task_spec(TaskSpec::new("duplicate", |_| async { Ok(()) }));
    assert_eq!(
        support::dynamic_root(&root)
            .add_subtree("invalid", invalid)
            .await
            .expect_err("invalid subtree fails before insertion"),
        ControlError::Rejected(BuildError::DuplicateChildId("duplicate".to_owned()))
    );

    let first_tree = DynamicTree::new();
    let first = first_tree.scope();
    support::dynamic_root(&root)
        .add_subtree("workers", first_tree)
        .await
        .expect("first subtree added");
    first
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("actor added");

    let error = support::dynamic_root(&root)
        .add_subtree("workers", DynamicTree::new())
        .await
        .expect_err("duplicate subtree rejected");
    assert_eq!(
        error,
        ControlError::Rejected(BuildError::DuplicateChildId("workers".to_owned()))
    );
    assert_eq!(root.scope().actor_stats().len(), 1);
    assert!(root.scope().subtree("workers").is_some());

    root.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn recursive_stats_distinguish_duplicate_actor_ids_in_sibling_subtrees() {
    let mut left_graph = TreeBuilder::new();
    let actor_slot = ActorSlot::new("worker");
    left_graph.define(actor_slot, Drain::<()>::new);
    let mut right_graph = TreeBuilder::new();
    let actor_slot = ActorSlot::new("worker");
    right_graph.define(actor_slot, Drain::<()>::new);

    let left_graph = left_graph.build();
    let right_graph = right_graph.build();
    let mut tree = Tree::new();
    tree.add_subtree("left", left_graph);
    tree.add_subtree("right", right_graph);
    let handle = tree.spawn().expect("runtime builds");
    handle
        .scope()
        .wait_started()
        .await
        .expect("runtime started");

    let stats = handle.scope().actor_stats();
    assert_eq!(stats.len(), 2);
    assert!(stats.iter().all(|stats| stats.stats.actor_id == "worker"));
    assert!(stats.iter().all(|stats| stats.lineage == 0));

    let paths = stats
        .iter()
        .map(|stats| {
            let path = &stats.scope_path;
            assert_eq!(path.len(), 1);
            (path[0].id.as_str(), path[0].lineage, path[0].generation)
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, [("left", 0, 0), ("right", 1, 0)]);

    handle.shutdown().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_same_id_replacement_cannot_inherit_tracked_actor_stats() {
    let handle = DynamicTree::new().spawn().expect("runtime builds");
    let tracked = support::dynamic_root(&handle)
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("tracked actor added");
    tracked.send(()).await.expect("tracked actor receives");
    let tracked_lineage = handle
        .scope()
        .actor_stats()
        .into_iter()
        .find(|stats| stats.stats.actor_id == "worker")
        .map(|stats| stats.lineage)
        .expect("tracked lineage available");

    let sampler_handle = handle.scope();
    let sampler = tokio::spawn(async move {
        for _ in 0..1_000 {
            for stats in sampler_handle.actor_stats() {
                if stats.stats.actor_id == "worker" {
                    assert_eq!(
                        stats.lineage, tracked_lineage,
                        "a replacement membership must never receive the old actor's counters"
                    );
                }
            }
            tokio::task::yield_now().await;
        }
    });

    support::dynamic_root(&handle)
        .remove_named("worker")
        .await
        .expect("tracked actor removed through runtime handle");
    support::dynamic_root(&handle)
        .add_task_spec(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("untracked replacement added");

    sampler.await.expect("sampler completed");
    assert!(
        handle
            .scope()
            .actor_stats()
            .iter()
            .all(|stats| stats.stats.actor_id != "worker"),
        "the replacement membership does not carry the old actor attachment"
    );

    handle.shutdown().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recursive_stats_prune_dynamic_actors_lost_on_subtree_restart() {
    let mut nested_graph = TreeBuilder::new();
    let static_ref_slot = ActorSlot::new("static-worker");
    let static_ref = static_ref_slot.actor_ref();
    nested_graph.define(static_ref_slot, || FailOnMessage);
    let mut nested_graph = nested_graph.build();
    nested_graph.add_subtree("dynamic", DynamicTree::new());
    let nested_graph = nested_graph
        .default_child_restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(60)));
    let mut tree = Tree::new();
    tree.add_subtree("workers", nested_graph);
    let handle = tree.spawn().expect("nested runtime builds");
    handle
        .scope()
        .wait_started()
        .await
        .expect("runtime started");

    let subtree = handle
        .scope()
        .subtree("workers")
        .expect("actor-aware subtree");
    let dynamic = subtree
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree");
    let dynamic_ref = dynamic
        .add_actor_spec(ActorSpec::new("dynamic-worker", Drain::<()>::new))
        .await
        .expect("dynamic actor added");
    assert!(
        handle
            .scope()
            .actor_stats()
            .iter()
            .any(|stats| stats.stats.actor_id == "dynamic-worker")
    );

    let old_generation = handle
        .scope()
        .snapshot()
        .child("workers")
        .expect("subtree snapshot exists")
        .generation;
    let sampling = Arc::new(AtomicBool::new(true));
    let sampler_handle = handle.scope();
    let sampler_sampling = Arc::clone(&sampling);
    let sampler = tokio::spawn(async move {
        while sampler_sampling.load(Ordering::Relaxed) {
            for stats in sampler_handle.actor_stats() {
                if stats.stats.actor_id != "dynamic-worker" {
                    continue;
                }
                let path = stats.scope_path;
                assert_eq!(
                    path[0].generation, old_generation,
                    "an old incarnation's attachment cache must not be traversed through the new incarnation"
                );
            }
            tokio::task::yield_now().await;
        }
    });

    let (lifecycle, baseline) = restart_observer(&handle.scope(), "workers");
    static_ref.send(()).await.expect("failure triggered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "workers", baseline),
    )
    .await
    .expect("subtree restarted within timeout");

    let stats = handle.scope().actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].stats.actor_id, static_ref.id());
    assert!(matches!(dynamic_ref.send(()).await, Err(SendError { .. })));
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    sampling.store(false, Ordering::Relaxed);
    sampler.await.expect("restart-window sampler completed");

    dynamic
        .add_task_spec(TaskSpec::new("dynamic-worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("same-id raw child added in replacement subtree incarnation");
    assert!(
        handle
            .scope()
            .actor_stats()
            .iter()
            .all(|stats| stats.stats.actor_id != "dynamic-worker"),
        "the replacement child must not inherit the old actor attachment"
    );

    handle
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn dynamic_subtree_restart_recreates_only_builder_membership() {
    let mut graph = TreeBuilder::new();
    let static_ref_slot = ActorSlot::new("static-worker");
    let static_ref = static_ref_slot.actor_ref();
    graph.define(static_ref_slot, || FailOnMessage);
    let root = DynamicTree::new().spawn().expect("runtime builds");
    let mut graph = graph.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let graph =
        graph.default_child_restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(60)));
    let subtree = support::dynamic_root(&root)
        .add_subtree("workers", graph)
        .await
        .expect("subtree added");
    subtree.wait_started().await.expect("subtree started");
    let dynamic = subtree
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree");
    let dynamic_ref = dynamic
        .add_actor_spec(ActorSpec::new("dynamic-worker", Drain::<()>::new))
        .await
        .expect("dynamic actor added");
    assert_eq!(root.scope().actor_stats().len(), 2);

    let root_handle = root.scope();
    let (lifecycle, baseline) = restart_observer(&root_handle, "workers");
    static_ref.send(()).await.expect("failure triggered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "workers", baseline),
    )
    .await
    .expect("subtree restarted within timeout");

    let stats = root.scope().actor_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].stats.actor_id, static_ref.id());
    assert!(matches!(dynamic_ref.send(()).await, Err(SendError { .. })));

    root.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn parent_restart_drops_dynamic_members_and_allows_same_id_replay() {
    let mut parent_graph = TreeBuilder::new();
    let fuse_slot = ActorSlot::new("fuse");
    let fuse = fuse_slot.actor_ref();
    parent_graph.define(fuse_slot, || FailOnMessage);
    let mut parent_graph = parent_graph.build();
    parent_graph.add_subtree("dynamic", DynamicTree::new());
    let parent_graph = parent_graph
        .default_child_restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(60)));
    let mut tree = Tree::new();
    tree.add_subtree("parent", parent_graph);
    let root = tree.spawn().expect("runtime builds");
    root.scope().wait_started().await.expect("runtime started");
    let parent = root
        .scope()
        .subtree("parent")
        .expect("parent subtree available");
    let dynamic = parent
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree available");
    dynamic
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("dynamic actor added");
    assert!(
        root.scope()
            .actor_stats()
            .iter()
            .any(|stats| stats.stats.actor_id == "worker")
    );

    let (lifecycle, baseline) = restart_observer(&root.scope(), "parent");
    fuse.send(()).await.expect("parent failure triggered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "parent", baseline),
    )
    .await
    .expect("parent restarted within timeout");

    assert!(
        root.scope()
            .actor_stats()
            .iter()
            .all(|stats| stats.stats.actor_id != "worker")
    );
    let restarted_parent = root.scope().subtree("parent").expect("parent rebound");
    let rebound_dynamic = restarted_parent
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("dynamic subtree rebound");
    rebound_dynamic
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("same id can be replayed");
    assert!(
        root.scope()
            .actor_stats()
            .iter()
            .any(|stats| stats.stats.actor_id == "worker")
    );

    root.shutdown().await.expect("clean shutdown");
}

#[derive(Clone)]
struct FailAfterObserve {
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for FailAfterObserve {
    type Msg = String;

    async fn run(&mut self, ctx: &mut RawContext<String>) -> ExitResult {
        match ctx.recv().await {
            Some(message) => {
                self.observed.send(message).expect("receiver alive");
                Err("deliberate failure".into())
            }
            None => Ok(()),
        }
    }
}

#[tokio::test]
async fn actor_stats_accumulate_across_supervised_restarts() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, worker_ref) = build_runtime(move || FailAfterObserve {
        observed: observed_tx.clone(),
    });
    let handle = tree.spawn().expect("runtime builds");

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
        .scope()
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
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn tree_spawn_accepts_ref_cloned_before_startup() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, worker_ref) = build_runtime(move || ObserveOnce {
        observed: observed_tx.clone(),
    });

    let handle = tree.spawn().expect("runtime builds");
    let mut snapshots = handle.scope().subscribe_snapshots();
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
                .is_some_and(|child| child.state.is_terminal())
        })
        .await
        .expect("completion snapshot remains available")
        .clone();
    assert!(matches!(
        completed
            .child("worker")
            .expect("worker remains visible")
            .state
            .last_exit(),
        Some(exit) if exit.is_completed()
    ));

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "run-path");
    handle.scope().request_shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn runtime_spawn_wait_drives_to_completion_with_control_surface() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, worker_ref) = build_runtime(move || ObserveOnce {
        observed: observed_tx.clone(),
    });

    let handle = tree.spawn().expect("runtime builds");
    let control = handle.scope();

    control
        .subscribe_snapshots()
        .wait_for(|snapshot| {
            snapshot
                .children
                .iter()
                .all(|child| child.state.is_running())
        })
        .await
        .expect("runtime reported running");
    let _lifecycle = control.subscribe_lifecycle();
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
                .is_some_and(|child| child.state.is_terminal())
        })
        .await
        .expect("completion snapshot remains available")
        .clone();
    assert!(matches!(
        completed
            .child("worker")
            .expect("worker remains visible")
            .state
            .last_exit(),
        Some(exit) if exit.is_completed()
    ));

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker observed the message")
        .expect("worker is still running");
    assert_eq!(observed, "spawn-wait-path");
    handle.scope().request_shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn supervision_tree_wires_graph_into_supervised_runtime() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = TreeBuilder::new();
    let worker_ref_slot = ActorSlot::new("worker");
    let worker_ref = worker_ref_slot.actor_ref();
    builder.define(worker_ref_slot, move || Observe {
        observed: observed_tx.clone(),
    });
    let graph = builder.build();

    let handle = graph
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");

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
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn supervision_tree_mixes_actor_and_non_actor_children() {
    let mut builder = TreeBuilder::new();
    let actor_slot = ActorSlot::new("actor");
    builder.define(actor_slot, Drain::<()>::new);
    let graph = builder.build();
    let sidecar_started = Arc::new(Notify::new());

    let sidecar = TaskSpec::new("sidecar", {
        let sidecar_started = sidecar_started.clone();
        move |ctx| {
            let sidecar_started = sidecar_started.clone();
            async move {
                sidecar_started.notify_one();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    });
    let mut graph = graph;
    graph.add_task_spec(sidecar);
    let handle = graph.spawn().expect("runtime builds");

    timeout(Duration::from_secs(1), sidecar_started.notified())
        .await
        .expect("sidecar started");
    timeout(
        Duration::from_secs(1),
        handle.scope().subscribe_snapshots().wait_for(|snapshot| {
            snapshot
                .child("actor")
                .is_some_and(|child| child.state.is_running())
                && snapshot
                    .child("sidecar")
                    .is_some_and(|child| child.state.is_running())
        }),
    )
    .await
    .expect("actor and sidecar reported running")
    .expect("snapshot channel stays open");

    handle.shutdown().await.expect("runtime shut down cleanly");
}

#[tokio::test]
async fn snapshot_wait_reports_all_children_running_after_spawn() {
    let mut builder = TreeBuilder::new();
    let actor_slot = ActorSlot::new("one");
    builder.define(actor_slot, Drain::<()>::new);
    let actor_slot = ActorSlot::new("two");
    builder.define(actor_slot, Drain::<()>::new);
    let graph = builder.build();

    let handle = graph
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");

    let mut snapshots = handle.scope().subscribe_snapshots();
    let all_running = snapshots.wait_for(|snapshot| {
        snapshot.children.len() == 2
            && snapshot
                .children
                .iter()
                .all(|child| child.state.is_running())
    });
    timeout(Duration::from_secs(1), all_running)
        .await
        .expect("runtime reported running")
        .expect("snapshot channel stays open");
    assert_eq!(handle.scope().snapshot().children.len(), 2);

    handle.shutdown().await.expect("runtime shut down cleanly");
}

#[derive(Clone)]
struct FailOnMessage;

impl RawActor for FailOnMessage {
    type Msg = ();

    async fn run(&mut self, ctx: &mut RawContext<()>) -> ExitResult {
        if ctx.recv().await.is_some() {
            return Err::<_, BoxError>(Box::new(io::Error::other("boom")));
        }

        Ok(())
    }
}

#[tokio::test]
async fn snapshot_child_wait_arms_before_the_future_is_polled() {
    let mut builder = TreeBuilder::new();
    let worker_ref_slot = ActorSlot::new("worker");
    let worker_ref = worker_ref_slot.actor_ref();
    builder.define(worker_ref_slot, || FailOnMessage);
    let graph = builder.build();

    let handle = graph
        .strategy(Strategy::OneForOne)
        .default_child_restart(RestartPolicy::on_failure())
        .spawn()
        .expect("runtime builds");

    let mut snapshots = handle.scope().subscribe_snapshots();
    let baseline = handle
        .scope()
        .snapshot()
        .child("worker")
        .unwrap()
        .generation;
    worker_ref.send(()).await.expect("message sent");

    timeout(
        Duration::from_secs(1),
        snapshots.wait_for_child("worker", |child| {
            child.generation > baseline && child.state.is_running()
        }),
    )
    .await
    .expect("replacement starts before the helper future is polled")
    .expect("snapshot stream stays open");

    handle.shutdown().await.expect("runtime shut down cleanly");
}

#[derive(Clone)]
struct AlwaysFails;

impl RawActor for AlwaysFails {
    type Msg = ();

    async fn run(&mut self, _ctx: &mut RawContext<()>) -> ExitResult {
        Err::<_, BoxError>(Box::new(io::Error::other("boom")))
    }
}

#[tokio::test]
async fn send_fails_after_restart_intensity_is_exhausted() {
    let mut builder = TreeBuilder::new();
    let worker_ref_slot = ActorSlot::new("worker");
    let worker_ref = worker_ref_slot.actor_ref();
    builder.define(worker_ref_slot, || AlwaysFails);
    let graph = builder.build();

    let handle = graph
        .strategy(Strategy::OneForOne)
        .default_child_restart(RestartPolicy::always().limit(1, Duration::from_secs(60)))
        .spawn()
        .expect("runtime builds");

    // The crash loop exhausts the restart budget and the supervisor gives up.
    let _ = timeout(Duration::from_secs(2), handle.wait())
        .await
        .expect("supervisor gave up");

    // A rebind will never come; send must not wait for one.
    let result = timeout(Duration::from_millis(500), worker_ref.send(()))
        .await
        .expect("send resolved after the supervisor gave up");
    assert!(matches!(result, Err(SendError { .. })));
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.on_starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, message: CounterMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CounterMsg::Add(n) => {
                self.total += n;
                Ok(())
            }
            CounterMsg::Total(reply) => {
                reply.send(self.total);
                Ok(())
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
    let mut builder = TreeBuilder::new();
    let counter_slot = ActorSlot::new("counter");
    let counter = counter_slot.actor_ref();
    builder.define(counter_slot, {
        let on_starts = on_starts.clone();
        move || ResettingCounter {
            total: 0,
            on_starts: on_starts.clone(),
        }
    });
    let graph = builder.build();

    let handle = graph
        .default_child_restart(RestartPolicy::always())
        .spawn()
        .expect("runtime builds");

    counter.send(CounterMsg::Add(5)).await.expect("add sent");
    assert_eq!(
        counter
            .call(CounterMsg::Total, Duration::from_secs(1))
            .await
            .expect("total replied"),
        5
    );

    let (lifecycle, baseline) = restart_observer(&handle.scope(), "counter");
    counter.send(CounterMsg::Crash).await.expect("crash sent");
    timeout(
        Duration::from_secs(2),
        await_restart(lifecycle, "counter", baseline),
    )
    .await
    .expect("restart observed");

    assert_eq!(
        counter
            .call(CounterMsg::Total, Duration::from_secs(2))
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
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}

#[tokio::test]
async fn ordered_tree_spawns_and_shuts_down_without_children() {
    let running_tree = Tree::new().spawn().expect("empty ordered tree builds");
    running_tree
        .scope()
        .wait_started()
        .await
        .expect("empty ordered tree starts");
    assert!(running_tree.scope().snapshot().children.is_empty());
    running_tree
        .shutdown()
        .await
        .expect("empty ordered tree shuts down");
}

#[tokio::test]
async fn dynamic_tree_idles_empty_until_an_actor_is_added() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("empty dynamic tree builds");
    let handle = running_tree.scope();
    handle
        .wait_started()
        .await
        .expect("empty dynamic tree starts");
    assert!(handle.snapshot().children.is_empty());

    let dynamic = handle.clone();

    let actor = dynamic
        .add_actor_spec(ActorSpec::new("worker", Drain::<()>::new))
        .await
        .expect("actor is added after idle startup");
    actor.send(()).await.expect("added actor is running");
    assert!(handle.snapshot().child("worker").is_some());

    running_tree
        .shutdown()
        .await
        .expect("dynamic tree shuts down");
}

#[derive(Clone)]
struct PendingActor {
    started: Arc<Notify>,
}

enum StuckDrainMsg {
    Gate,
    Stuck,
}

#[derive(Clone)]
struct StuckDrainActor {
    handling_gate: Arc<Notify>,
    release_gate: Arc<Notify>,
}

impl Actor for StuckDrainActor {
    type Msg = StuckDrainMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            StuckDrainMsg::Gate => {
                self.handling_gate.notify_one();
                self.release_gate.notified().await;
            }
            StuckDrainMsg::Stuck => std::future::pending::<()>().await,
        }
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_drain_for_bounds_the_whole_actor_drain() {
    let handling_gate = Arc::new(Notify::new());
    let release_gate = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let worker_slot = ActorSlot::new("worker");
    let worker = worker_slot.actor_ref();
    graph.add_actor_spec(
        worker_slot
            .define({
                let handling_gate = handling_gate.clone();
                let release_gate = release_gate.clone();
                move || StuckDrainActor {
                    handling_gate: handling_gate.clone(),
                    release_gate: release_gate.clone(),
                }
            })
            .shutdown(Shutdown::graceful_for(Duration::from_millis(20))),
    );
    let handle = graph.spawn().expect("runtime builds");

    worker
        .send(StuckDrainMsg::Gate)
        .await
        .expect("gate accepted");
    handling_gate.notified().await;
    worker
        .send(StuckDrainMsg::Stuck)
        .await
        .expect("drain work queued");
    handle.scope().request_shutdown();
    let shutdown = tokio::spawn({
        let handle = handle.scope();
        async move { handle.wait_stopped().await }
    });
    tokio::task::yield_now().await;
    release_gate.notify_one();

    assert!(matches!(
        shutdown.await.expect("shutdown task joined"),
        Err(SupervisorError::ShutdownTimedOut(actor_id)) if actor_id == "worker"
    ));
    assert!(
        handle
            .scope()
            .snapshot()
            .child("worker")
            .expect("static membership remains")
            .state
            .last_exit()
            .is_some_and(|exit| exit.timed_out())
    );
}

impl RawActor for PendingActor {
    type Msg = ();

    async fn run(&mut self, _ctx: &mut RawContext<()>) -> ExitResult {
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn actor_shutdown_timeout_is_truthful_across_layers() {
    let started = Arc::new(Notify::new());
    let mut builder = TreeBuilder::new();
    let actor_slot = ActorSlot::new("worker");
    builder.define(actor_slot, {
        let started = started.clone();
        move || PendingActor {
            started: started.clone(),
        }
    });
    let handle = builder
        .build()
        .default_child_shutdown(Shutdown::graceful_for(Duration::from_millis(20)))
        .spawn()
        .expect("runtime builds");
    let mut lifecycle = handle.scope().subscribe_lifecycle();
    let scope = handle.scope();

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("actor started");
    assert!(matches!(
        handle.shutdown().await,
        Err(SupervisorError::ShutdownTimedOut(actor_id)) if actor_id == "worker"
    ));
    assert!(
        scope
            .snapshot()
            .child("worker")
            .expect("actor remains in static membership")
            .state
            .last_exit()
            .is_some_and(|exit| exit.timed_out())
    );
    while let Some(event) = lifecycle.next().await {
        if let LifecycleEventKind::Child(child) = event.kind
            && let ChildEventKind::Exited { exit, .. } = child.kind
        {
            assert!(exit.timed_out());
            return;
        }
    }
    panic!("timeout exit was not published");
}

#[tokio::test]
async fn handle_actor_stats_track_graph_and_runtime_added_actors() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = TreeBuilder::new();
    let worker_ref_slot = ActorSlot::new("worker");
    let worker_ref = worker_ref_slot.actor_ref();
    graph.define(worker_ref_slot, move || Observe {
        observed: observed_tx.clone(),
    });
    let mut graph = graph.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let handle = graph.spawn().expect("mixed runtime builds");

    worker_ref
        .send("count me".to_owned())
        .await
        .expect("message sent");
    observed_rx.recv().await.expect("message observed");

    // Declared actors are visible in the runtime's stats without any dynamic
    // actor having been added.
    let stats = handle.scope().actor_stats();
    let worker = stats
        .iter()
        .find(|stats| stats.stats.actor_id == "worker")
        .expect("graph actor reported in runtime stats");
    assert_eq!(worker.stats.messages_accepted, 1);

    let dynamic = handle
        .scope()
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic subtree");
    let extra = dynamic
        .add_actor_spec(ActorSpec::new("extra", Drain::<()>::new))
        .await
        .expect("actor added");
    extra.send(()).await.expect("message sent");

    let stats = handle.scope().actor_stats();
    assert_eq!(stats.len(), 2);
    let extra_stats = stats
        .iter()
        .find(|stats| stats.stats.actor_id == "extra")
        .expect("runtime-added actor reported in runtime stats");
    assert_eq!(extra_stats.stats.messages_accepted, 1);

    dynamic.remove_named("extra").await.expect("actor removed");
    let stats = handle.scope().actor_stats();
    assert!(
        stats.iter().all(|stats| stats.stats.actor_id != "extra"),
        "removed actor no longer reported"
    );

    handle
        .shutdown()
        .await
        .expect("supervisor shut down cleanly");
}
