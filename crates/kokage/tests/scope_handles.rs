use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorResult, ActorSpec, ControlError, DynamicTree, GraphBuilder, LiveContext,
    MessageContext, OrderedTree, RestartConfig, RestartPolicy, RuntimeHandle, ScopeKind,
    ShutdownPolicy, StartContext, StopContext, Strategy, SupervisorBuildError,
    host::{BoxError, ChildSpec},
    observe::{ChildStateView, ExitStatusView, SupervisorSnapshot},
};
use tokio::{
    sync::{mpsc, watch},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Debug)]
enum LeaderMsg {
    AddFromHandler,
    OnStartAdded(bool),
    Crash,
}

struct ScopeProbe {
    reports: mpsc::UnboundedSender<&'static str>,
    starts: Arc<AtomicUsize>,
    child_stopped: Option<Arc<AtomicBool>>,
    mutate_children_on_start: bool,
}

impl Actor for ScopeProbe {
    type Msg = LeaderMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let Some(children) = ctx.children() else {
            let supervisor = ctx.supervisor();
            assert_eq!(supervisor.snapshot().kind, ScopeKind::Ordered);
            assert!(matches!(
                supervisor.add_actor("forbidden", || Idle).await,
                Err(ControlError::UnsupportedByScopeKind { .. })
            ));
            self.reports
                .send("ordered-supervisor")
                .expect("test receiver open");
            self.reports.send("none").expect("test receiver open");
            return Ok(());
        };
        self.reports.send("some").expect("test receiver open");
        // Task insertion schedules startup rather than awaiting readiness, so
        // it remains available on the restricted startup-stage handle.
        let before_ready = children
            .add_child(ChildSpec::task("too-early", |_| async { Ok(()) }))
            .await;
        assert!(matches!(before_ready, Err(ControlError::Unavailable)));
        self.reports
            .send("unavailable-before-ready")
            .expect("test receiver open");

        if !self.mutate_children_on_start {
            return Ok(());
        }

        // The wait is detached from startup but remains owned by this actor
        // incarnation. Its mapped result returns through the actor mailbox.
        ctx.spawn_scope_wait(
            &children,
            |children| async move {
                children.wait_started().await.map_err(|_| ())?;
                children
                    .add_actor("from-on-start", || Idle)
                    .await
                    .map_err(|_| ())?;
                Ok::<_, ()>(())
            },
            |result| LeaderMsg::OnStartAdded(result.is_ok()),
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            LeaderMsg::AddFromHandler => {
                let children = ctx.children().expect("ActorWithScope leader has children");
                children.add_actor("from-handler", || Idle).await?;
                self.reports
                    .send("handler-added")
                    .expect("test receiver open");
            }
            LeaderMsg::OnStartAdded(true) => {
                self.reports
                    .send("on-start-added")
                    .expect("test receiver open");
            }
            LeaderMsg::OnStartAdded(false) => return Err("on-start child add failed".into()),
            LeaderMsg::Crash => panic!("scripted leader crash"),
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        if let Some(child_stopped) = &self.child_stopped {
            assert!(
                child_stopped.load(Ordering::SeqCst),
                "owned children stop before their leader"
            );
        }
        Ok(())
    }
}

struct StopProbe(Arc<AtomicBool>);

impl Actor for StopProbe {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct BuilderHandleOwner {
    mount: RuntimeHandle,
    report: mpsc::UnboundedSender<&'static str>,
}

impl Actor for BuilderHandleOwner {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.mount.add_actor("owned", || Idle).await?;
        self.report.send("mounted").expect("test receiver open");
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

struct RestrictedTaskAdder {
    lineage: mpsc::UnboundedSender<u64>,
}

impl Actor for RestrictedTaskAdder {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        let children = ctx.children().expect("actor owns a dynamic scope");
        let lineage = children
            .add_child(ChildSpec::task("task", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await?;
        self.lineage.send(lineage).expect("test receiver open");
        Ok(())
    }
}

fn single_use_mount(report: mpsc::UnboundedSender<&'static str>) -> OrderedTree {
    let mount_builder = DynamicTree::new();
    let mount = mount_builder.handle();
    let mut graph = GraphBuilder::new();
    let (actor_slot, _) = graph.slot("owner");
    graph.define(actor_slot, move || BuilderHandleOwner {
        mount: mount.clone(),
        report: report.clone(),
    });
    let graph = graph.build().expect("owner graph builds");
    OrderedTree::new()
        .subtree("mount", mount_builder)
        .actor(graph.actors()[0].clone())
}

async fn next_report(reports: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
    timeout(WAIT, reports.recv())
        .await
        .expect("timed out waiting for report")
        .expect("report channel closed")
}

async fn assert_snapshot_stream_closes(handle: &RuntimeHandle) {
    assert_snapshot_receiver_closes(handle.subscribe_snapshots()).await;
}

async fn assert_snapshot_receiver_closes(mut snapshots: watch::Receiver<SupervisorSnapshot>) {
    timeout(
        WAIT,
        async move { while snapshots.changed().await.is_ok() {} },
    )
    .await
    .expect("snapshot stream closes");
}

#[tokio::test]
async fn tree_handle_binds_to_the_spawned_runtime() {
    let tree = DynamicTree::new();
    let pre_spawn = tree.handle();
    let spawned = tree.spawn().expect("tree builds and spawns");

    pre_spawn
        .wait_started()
        .await
        .expect("pre-spawn scope starts");
    pre_spawn
        .add_actor("worker", || Idle)
        .await
        .expect("pre-spawn handle controls the spawned scope");
    assert!(spawned.snapshot().child("worker").is_some());

    pre_spawn
        .shutdown_and_wait()
        .await
        .expect("pre-spawn handle stops the spawned scope");
}

#[tokio::test]
async fn pre_spawn_snapshot_subscription_follows_the_spawned_identity() {
    let tree = OrderedTree::new().task(
        ChildSpec::task("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }),
        RestartPolicy::default(),
        ShutdownPolicy::default(),
    );
    let handle = tree.handle();
    let mut snapshots = handle.subscribe_snapshots();
    let declared = snapshots
        .borrow()
        .child("worker")
        .expect("worker is projected before spawn")
        .clone();
    assert!(matches!(declared.state, ChildStateView::Starting { .. }));
    assert!(!declared.state.started());

    let spawned = tree.spawn().expect("tree builds and spawns");
    timeout(
        WAIT,
        snapshots.wait_for(|snapshot| {
            snapshot.child("worker").is_some_and(|worker| {
                worker.lineage == declared.lineage
                    && worker.state.is_running()
                    && worker.state.started()
            })
        }),
    )
    .await
    .expect("pre-spawn subscription observes startup")
    .expect("same snapshot stream remains open");

    assert_eq!(handle.snapshot(), spawned.snapshot());
    spawned
        .shutdown_and_wait()
        .await
        .expect("spawned tree stops");
}

#[tokio::test]
async fn trees_terminalize_handles_when_dropped() {
    let builder = OrderedTree::new();
    let handle = builder.handle();
    let snapshots = handle.subscribe_snapshots();
    assert_eq!(handle.snapshot().kind, ScopeKind::Ordered);
    assert!(matches!(
        handle
            .add_child(ChildSpec::task("early", |_| async { Ok(()) }))
            .await,
        Err(ControlError::Unavailable)
    ));
    let builder = builder.strategy(Strategy::RestForOne);
    assert_eq!(handle.snapshot().strategy, Strategy::RestForOne);
    drop(builder);
    assert_snapshot_receiver_closes(snapshots).await;

    let builder = DynamicTree::new();
    let handle = builder.handle();
    let snapshots = handle.subscribe_snapshots();
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);
    drop(builder);
    assert_snapshot_receiver_closes(snapshots).await;

    let child = DynamicTree::new();
    let child_handle = child.handle();
    let child_snapshots = child_handle.subscribe_snapshots();
    let parent = OrderedTree::new().subtree("child", child);
    drop(parent);
    assert_snapshot_receiver_closes(child_snapshots).await;
}

#[test]
fn tree_strategy_preserves_declared_pre_spawn_snapshot() {
    let mut graph = GraphBuilder::new();
    let (actor_slot, _) = graph.slot("actor");
    graph.define(actor_slot, || Idle);
    let graph = graph.build().expect("graph builds");
    let tree = OrderedTree::new()
        .task(
            ChildSpec::task("task", |_| async { Ok(()) }),
            RestartPolicy::default(),
            ShutdownPolicy::default(),
        )
        .actor(graph.actors()[0].clone());
    let handle = tree.handle();
    let declared_before = handle
        .snapshot()
        .children
        .into_iter()
        .map(|child| child.id)
        .collect::<Vec<_>>();

    let tree = tree.strategy(Strategy::RestForOne);
    let after = handle.snapshot();

    assert_eq!(after.strategy, Strategy::RestForOne);
    assert_eq!(
        after
            .children
            .into_iter()
            .map(|child| child.id)
            .collect::<Vec<_>>(),
        declared_before
    );
    assert_eq!(declared_before, ["task", "actor"]);
    drop(tree);
}

#[tokio::test]
async fn spawn_errors_and_rejected_subtrees_terminalize_tree_handles() {
    let tree = OrderedTree::new()
        .task(
            ChildSpec::task("duplicate", |_| async { Ok(()) }),
            RestartPolicy::default(),
            ShutdownPolicy::default(),
        )
        .task(
            ChildSpec::task("duplicate", |_| async { Ok(()) }),
            RestartPolicy::default(),
            ShutdownPolicy::default(),
        );
    let failed_ordered = tree.handle();
    let failed_ordered_snapshots = failed_ordered.subscribe_snapshots();
    assert!(tree.spawn().is_err());
    assert_snapshot_receiver_closes(failed_ordered_snapshots).await;

    let builder = DynamicTree::new().restart_intensity(RestartConfig::new(1, Duration::ZERO));
    let failed_dynamic = builder.handle();
    let failed_dynamic_snapshots = failed_dynamic.subscribe_snapshots();
    assert!(builder.spawn().is_err());
    assert_snapshot_receiver_closes(failed_dynamic_snapshots).await;

    let parent = OrderedTree::new().spawn().expect("ordered parent builds");
    parent.wait_started().await.expect("ordered parent starts");
    let child = DynamicTree::new();
    let rejected = child.handle();
    let rejected_snapshots = rejected.subscribe_snapshots();
    assert!(matches!(
        parent.add_subtree("rejected", child).await,
        Err(ControlError::UnsupportedByScopeKind { .. })
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;
    parent
        .shutdown_and_wait()
        .await
        .expect("ordered parent stops");

    let parent = DynamicTree::new().spawn().expect("dynamic parent builds");
    parent.wait_started().await.expect("dynamic parent starts");

    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("duplicate-binding");
    graph.define(slot, || Idle);
    let graph = graph.build().expect("graph builds");
    let actor = graph.actors()[0].clone();
    let invalid = OrderedTree::new().actor(actor.clone()).actor(actor);
    let rejected = invalid.handle();
    let rejected_snapshots = rejected.subscribe_snapshots();
    assert!(matches!(
        parent.add_subtree("invalid", invalid).await,
        Err(ControlError::Rejected(
            SupervisorBuildError::DuplicateActorBinding(label)
        )) if label == "duplicate-binding"
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;

    parent
        .add_subtree("occupied", DynamicTree::new())
        .await
        .expect("first subtree inserts");
    let duplicate = DynamicTree::new();
    let rejected = duplicate.handle();
    let rejected_snapshots = rejected.subscribe_snapshots();
    assert!(matches!(
        parent.add_subtree("occupied", duplicate).await,
        Err(ControlError::Rejected(SupervisorBuildError::DuplicateChildId(id)))
            if id == "occupied"
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;
    parent
        .shutdown_and_wait()
        .await
        .expect("dynamic parent stops");
}

#[tokio::test]
async fn pre_spawn_mount_handle_supports_awaited_and_pipelined_subtree_adds() {
    let outer = DynamicTree::new().spawn().expect("dynamic outer builds");
    outer.wait_started().await.expect("dynamic outer starts");

    let (awaited_tx, mut awaited_rx) = mpsc::unbounded_channel();
    let awaited = outer
        .add_subtree("awaited", single_use_mount(awaited_tx))
        .await
        .expect("awaited subtree inserts");
    awaited
        .wait_started()
        .await
        .expect("awaited subtree starts");
    assert_eq!(next_report(&mut awaited_rx).await, "mounted");

    let (pipelined_tx, mut pipelined_rx) = mpsc::unbounded_channel();
    let pipelined_outer = outer.clone();
    let pipelined = tokio::spawn(async move {
        pipelined_outer
            .add_subtree("pipelined", single_use_mount(pipelined_tx))
            .await
    });
    assert_eq!(next_report(&mut pipelined_rx).await, "mounted");
    pipelined
        .await
        .expect("pipelined insertion task joins")
        .expect("pipelined subtree inserts");

    outer.shutdown_and_wait().await.expect("outer stops");
}

#[tokio::test]
async fn ordinary_actor_gets_its_scope_but_no_owned_children() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (actor_slot, _) = graph.slot("ordinary");
    graph.define(actor_slot, move || ScopeProbe {
        reports: reports_tx.clone(),
        starts: Arc::new(AtomicUsize::new(0)),
        child_stopped: None,
        mutate_children_on_start: false,
    });
    let handle = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("runtime builds");
    handle.wait_started().await.expect("actor starts");
    assert_eq!(next_report(&mut reports_rx).await, "ordered-supervisor");
    assert_eq!(next_report(&mut reports_rx).await, "none");
    handle.shutdown_and_wait().await.expect("runtime stops");
}

#[tokio::test]
async fn actor_with_dynamic_scope_injects_children_for_on_start_and_handler_mutation() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let (leader_slot, leader) = graph.slot("leader");
    graph.define(leader_slot, {
        let starts = Arc::clone(&starts);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: None,
            mutate_children_on_start: true,
        }
    });
    let graph = graph.build().expect("leader graph builds");
    let handle = OrderedTree::new()
        .actor_with_scope(
            "owned",
            graph.actors()[0].clone(),
            DynamicTree::new(),
            Strategy::RestForOne,
        )
        .spawn()
        .expect("ActorWithScope builds");

    assert_eq!(next_report(&mut reports_rx).await, "some");
    assert_eq!(
        next_report(&mut reports_rx).await,
        "unavailable-before-ready"
    );
    handle
        .wait_started()
        .await
        .expect("owned node becomes ready");
    assert_eq!(next_report(&mut reports_rx).await, "on-start-added");
    leader
        .send(LeaderMsg::AddFromHandler)
        .await
        .expect("leader receives handler command");
    assert_eq!(next_report(&mut reports_rx).await, "handler-added");
    let children = handle
        .subtree("owned")
        .and_then(|owned| owned.subtree("children"))
        .expect("owned dynamic scope is registered");
    assert!(children.snapshot().child("from-on-start").is_some());
    assert!(children.snapshot().child("from-handler").is_some());
    let snapshot = handle.snapshot();
    let owned = snapshot
        .child("owned")
        .and_then(|child| child.supervisor.as_deref())
        .expect("root.owned exists");
    assert!(owned.child("leader").is_some(), "root.owned.leader exists");
    let inner = owned
        .child("children")
        .and_then(|child| child.supervisor.as_deref())
        .expect("root.owned.children exists");
    assert!(
        inner.child("from-handler").is_some(),
        "root.owned.children.from-handler exists"
    );

    handle.shutdown_and_wait().await.expect("runtime stops");
}

#[tokio::test]
async fn restricted_scope_add_child_returns_the_inserted_lineage() {
    let (lineage_tx, mut lineage_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (adder_slot, adder) = graph.slot("adder");
    graph.define(adder_slot, move || RestrictedTaskAdder {
        lineage: lineage_tx.clone(),
    });
    let graph = graph.build().expect("adder graph builds");
    let handle = OrderedTree::new()
        .actor_with_scope(
            "owned",
            graph.actors()[0].clone(),
            DynamicTree::new(),
            Strategy::OneForOne,
        )
        .spawn()
        .expect("tree builds");
    handle.wait_started().await.expect("tree starts");

    adder.send(()).await.expect("adder receives command");
    let lineage = timeout(WAIT, lineage_rx.recv())
        .await
        .expect("timed out waiting for lineage")
        .expect("lineage channel remains open");
    let children = handle
        .subtree("owned")
        .and_then(|owned| owned.subtree("children"))
        .expect("owned dynamic scope is registered");
    assert_eq!(
        children
            .snapshot()
            .child("task")
            .expect("task is inserted")
            .lineage,
        lineage
    );

    handle.shutdown_and_wait().await.expect("tree stops");
}

#[tokio::test]
async fn actor_with_ordered_scope_starts_after_leader_and_stops_before_it() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let child_stopped = Arc::new(AtomicBool::new(false));
    let mut leaders = GraphBuilder::new();
    let (actor_slot, _) = leaders.slot("leader");
    leaders.define(actor_slot, {
        let starts = Arc::clone(&starts);
        let child_stopped = Arc::clone(&child_stopped);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: Some(Arc::clone(&child_stopped)),
            mutate_children_on_start: false,
        }
    });
    let leaders = leaders.build().expect("leader graph builds");
    let mut workers = GraphBuilder::new();
    let (actor_slot, _) = workers.slot("worker");
    workers.define(actor_slot, {
        let child_stopped = Arc::clone(&child_stopped);
        move || StopProbe(Arc::clone(&child_stopped))
    });
    let workers = workers.build().expect("worker graph builds");
    let runtime = OrderedTree::new().actor_with_scope(
        "owned",
        leaders.actors()[0].clone(),
        OrderedTree::graph(workers),
        Strategy::RestForOne,
    );
    assert_eq!(runtime.handle().snapshot().strategy, Strategy::OneForOne);
    let handle = runtime.spawn().expect("ordered ActorWithScope builds");
    assert_eq!(next_report(&mut reports_rx).await, "some");
    assert_eq!(
        next_report(&mut reports_rx).await,
        "unavailable-before-ready"
    );
    handle.wait_started().await.expect("ordered child starts");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    let snapshot = handle.snapshot();
    let inner = snapshot
        .child("owned")
        .and_then(|child| child.supervisor.as_deref())
        .and_then(|owned| owned.child("children"))
        .and_then(|child| child.supervisor.as_deref())
        .expect("root.owned.children exists");
    assert!(
        inner.child("worker").is_some(),
        "root.owned.children.worker exists"
    );
    handle.shutdown_and_wait().await.expect("runtime stops");
    assert!(child_stopped.load(Ordering::SeqCst));
}

struct RestartProbe {
    starts: Arc<AtomicUsize>,
}

impl Actor for RestartProbe {
    type Msg = LeaderMsg;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        if matches!(message, LeaderMsg::Crash) {
            panic!("scripted crash");
        }
        ctx.stop();
        Ok(())
    }
}

async fn wait_count(counter: &AtomicUsize, expected: usize) {
    timeout(WAIT, async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("restart count reached");
}

#[tokio::test]
async fn actor_with_scope_uses_explicit_rest_for_one() {
    let leader_starts = Arc::new(AtomicUsize::new(0));
    let worker_starts = Arc::new(AtomicUsize::new(0));
    let mut leaders = GraphBuilder::new();
    let (leader_slot, leader) = leaders.slot("leader");
    leaders.define(leader_slot, {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let leaders = leaders.build().expect("leaders build");
    let mut workers = GraphBuilder::new();
    let (worker_slot, worker) = workers.slot("worker");
    workers.define(worker_slot, {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let workers = workers.build().expect("workers build");
    let tree = OrderedTree::new().actor_with_scope(
        "owned",
        leaders.actors()[0].clone(),
        OrderedTree::graph(workers),
        Strategy::RestForOne,
    );
    let outline = tree.outline();
    assert!(matches!(
        outline.child("owned"),
        Some(kokage::observe::ChildOutline::ActorWithScope {
            strategy: Strategy::RestForOne,
            ..
        })
    ));
    let handle = tree.spawn().expect("tree builds");
    handle.wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("worker crashes");
    wait_count(&worker_starts, 2).await;
    assert_eq!(leader_starts.load(Ordering::SeqCst), 1);

    leader.send(LeaderMsg::Crash).await.expect("leader crashes");
    wait_count(&leader_starts, 2).await;
    wait_count(&worker_starts, 3).await;
    handle.shutdown_and_wait().await.expect("tree stops");
}

#[tokio::test]
async fn one_for_all_opt_in_recycles_leader_when_inner_scope_fails() {
    let leader_starts = Arc::new(AtomicUsize::new(0));
    let worker_starts = Arc::new(AtomicUsize::new(0));
    let mut leaders = GraphBuilder::new();
    let (actor_slot, _) = leaders.slot("leader");
    leaders.define(actor_slot, {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let leaders = leaders.build().expect("leaders build");
    let mut workers = GraphBuilder::new();
    let (worker_slot, worker) = workers.slot("worker");
    workers.define(worker_slot, {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let workers = workers.build().expect("workers build");
    let inner = OrderedTree::graph(workers)
        .restart_intensity(RestartConfig::new(1, Duration::from_secs(30)));
    let handle = OrderedTree::new()
        .actor_with_scope(
            "owned",
            leaders.actors()[0].clone(),
            inner,
            Strategy::OneForAll,
        )
        .spawn()
        .expect("tree builds");
    handle.wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("first crash");
    wait_count(&worker_starts, 2).await;
    worker.send(LeaderMsg::Crash).await.expect("second crash");
    wait_count(&leader_starts, 2).await;

    handle.shutdown_and_wait().await.expect("tree stops");
}

#[tokio::test]
async fn consuming_a_graph_into_a_tree_preserves_issued_actor_refs() {
    let mut graph = GraphBuilder::new();
    let (slot, actor_ref) = graph.slot("actor");
    graph.define(slot, || Idle);
    let graph = graph.build().expect("graph builds");

    let tree = OrderedTree::graph(graph);
    let spawned = tree.spawn().expect("tree builds and spawns");
    spawned.wait_started().await.expect("tree starts");
    actor_ref.send(()).await.expect("issued ref remains bound");

    spawned
        .shutdown_and_wait()
        .await
        .expect("tree stops cleanly");
}

#[tokio::test]
async fn duplicate_actor_bindings_are_rejected_during_tree_lowering() {
    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("actor");
    graph.define(slot, || Idle);
    let graph = graph.build().expect("graph builds");
    let actor = graph.actors()[0].clone();
    let tree = OrderedTree::new().actor(actor.clone()).actor(actor);
    let handle = tree.handle();

    assert!(matches!(
        tree.spawn(),
        Err(SupervisorBuildError::DuplicateActorBinding(label)) if label == "actor"
    ));
    assert_snapshot_stream_closes(&handle).await;
}

#[tokio::test]
async fn actor_binding_cloned_across_trees_fails_on_the_second_concurrent_run() {
    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("actor");
    graph.define(slot, || Idle);
    let graph = graph.build().expect("graph builds");
    let actor = graph.actors()[0].clone();

    let first = OrderedTree::new()
        .actor(actor.clone())
        .spawn()
        .expect("first tree lowers");
    first.wait_started().await.expect("first actor starts");

    let second = OrderedTree::new()
        .actor(ActorSpec::new(actor).restart(RestartPolicy::Never))
        .spawn()
        .expect("a separate tree lowers before the runtime conflict");
    let mut snapshots = second.subscribe_snapshots();
    let stopped = timeout(
        WAIT,
        snapshots.wait_for(|snapshot| {
            snapshot.child("actor").is_some_and(|actor| {
                actor.state.is_stopped()
                    && matches!(
                        actor.state.last_exit().map(|exit| &exit.status),
                        Some(ExitStatusView::Failed(message))
                            if message == "actor `actor` is already running"
                    )
            })
        }),
    )
    .await
    .expect("second tree reports the late runtime conflict")
    .expect("second tree snapshot stream remains available");
    assert!(matches!(
        stopped
            .child("actor")
            .expect("actor remains declared")
            .state.last_exit().map(|exit| &exit.status),
        Some(ExitStatusView::Failed(message))
            if message == "actor `actor` is already running"
    ));

    first.shutdown_and_wait().await.expect("first tree stops");
}
