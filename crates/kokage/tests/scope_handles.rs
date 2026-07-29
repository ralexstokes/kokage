use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorResult, ActorSpec, ControlError, DynamicRuntimeHandle, DynamicTree, GraphBuilder,
    LiveContext, MessageContext, OrderedTree, RestartConfig, RestrictedScope, RuntimeHandle,
    ScopeKind, StartContext, StopContext, Strategy, SupervisorBuildError,
    host::{BoxError, ChildSpec},
    observe::{ChildStateView, SupervisorSnapshotReceiver},
};
use tokio::{sync::mpsc, time::timeout};

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
            assert!(supervisor.dynamic().is_none());
            self.reports
                .send("ordered-supervisor")
                .expect("test receiver open");
            self.reports.send("none").expect("test receiver open");
            return Ok(());
        };
        self.reports.send("some").expect("test receiver open");
        let Some(dynamic) = children.dynamic() else {
            self.reports
                .send("ordered-children")
                .expect("test receiver open");
            return Ok(());
        };
        // Task insertion schedules startup rather than awaiting readiness, so
        // it remains available on the restricted startup-stage handle.
        let before_ready = dynamic
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
                    .dynamic()
                    .expect("dynamic scope")
                    .add_actor(ActorSpec::new("from-on-start", || Idle))
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
                children
                    .dynamic()
                    .expect("dynamic scope")
                    .add_actor(ActorSpec::new("from-handler", || Idle))
                    .await?;
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
    mount: DynamicRuntimeHandle,
    report: mpsc::UnboundedSender<&'static str>,
}

impl Actor for BuilderHandleOwner {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.mount
            .add_actor(ActorSpec::new("owned", || Idle))
            .await?;
        self.report.send("mounted").expect("test receiver open");
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

struct RestrictedTaskAdder {
    lineage: mpsc::UnboundedSender<u64>,
    subtree: mpsc::UnboundedSender<RestrictedScope>,
}

impl Actor for RestrictedTaskAdder {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        let children = ctx.children().expect("actor owns a dynamic scope");
        let dynamic = children.dynamic().expect("dynamic scope");
        let lineage = dynamic
            .add_child(ChildSpec::task("task", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await?;
        self.lineage.send(lineage).expect("test receiver open");
        let subtree: RestrictedScope = dynamic
            .add_subtree("restricted-subtree", OrderedTree::new())
            .await?;
        self.subtree.send(subtree).expect("test receiver open");
        Ok(())
    }
}

fn single_use_mount(report: mpsc::UnboundedSender<&'static str>) -> OrderedTree {
    let mount_builder = DynamicTree::new();
    let mount = mount_builder.handle();
    OrderedTree::new()
        .subtree("mount", mount_builder)
        .actor(ActorSpec::new("owner", move || BuilderHandleOwner {
            mount: mount.clone(),
            report: report.clone(),
        }))
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

async fn assert_snapshot_receiver_closes(mut snapshots: SupervisorSnapshotReceiver) {
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
        .add_actor(ActorSpec::new("worker", || Idle))
        .await
        .expect("pre-spawn handle controls the spawned scope");
    assert!(spawned.handle().snapshot().child("worker").is_some());

    pre_spawn
        .shutdown_and_wait()
        .await
        .expect("pre-spawn handle stops the spawned scope");
}

#[tokio::test]
async fn dynamic_capability_tracks_root_and_nested_scope_kinds() {
    let runtime = OrderedTree::new()
        .subtree("ordered", OrderedTree::new())
        .subtree("dynamic", DynamicTree::new())
        .spawn()
        .expect("mixed tree builds");
    let root = runtime.handle();
    let ordered = root.subtree("ordered").expect("ordered subtree handle");
    let dynamic = root.subtree("dynamic").expect("dynamic subtree handle");

    assert!(root.dynamic().is_none());
    assert!(ordered.dynamic().is_none());
    assert!(dynamic.dynamic().is_some());

    runtime.shutdown_and_wait().await.expect("runtime stops");

    let dynamic_root = DynamicTree::new().spawn().expect("dynamic root builds");
    let _: DynamicRuntimeHandle = dynamic_root.handle();
    dynamic_root
        .shutdown_and_wait()
        .await
        .expect("dynamic root stops");
}

#[tokio::test]
async fn dropping_every_root_and_nested_handle_leaves_the_owned_runtime_running() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let tree = OrderedTree::new().subtree(
        "nested",
        OrderedTree::new().task(ChildSpec::task("worker", move |ctx| {
            let lifecycle_tx = lifecycle_tx.clone();
            async move {
                lifecycle_tx.send("started").expect("test receiver open");
                ctx.shutdown_token().cancelled().await;
                lifecycle_tx.send("cancelled").expect("test receiver open");
                Ok(())
            }
        })),
    );
    let runtime = tree.spawn().expect("tree builds and spawns");
    let root = runtime.handle();
    let nested = root.subtree("nested").expect("nested runtime handle");

    assert_eq!(next_report(&mut lifecycle_rx).await, "started");
    drop(nested);
    drop(root);
    assert!(
        timeout(Duration::from_millis(100), lifecycle_rx.recv())
            .await
            .is_err(),
        "dropping non-owning handles must leave the runtime alive"
    );

    runtime.shutdown();
    assert_eq!(next_report(&mut lifecycle_rx).await, "cancelled");
    runtime.wait().await.expect("runtime stops cleanly");
}

#[tokio::test]
async fn dropping_runtime_requests_graceful_shutdown() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let tree = OrderedTree::new().task(ChildSpec::task("worker", move |ctx| {
        let lifecycle_tx = lifecycle_tx.clone();
        async move {
            lifecycle_tx.send("started").expect("test receiver open");
            ctx.shutdown_token().cancelled().await;
            lifecycle_tx.send("cancelled").expect("test receiver open");
            Ok(())
        }
    }));
    let runtime = tree.spawn().expect("tree builds and spawns");
    let handle = runtime.handle();

    assert_eq!(next_report(&mut lifecycle_rx).await, "started");
    drop(runtime);
    assert_eq!(next_report(&mut lifecycle_rx).await, "cancelled");
    handle.wait().await.expect("owner drop drains runtime");
}

#[tokio::test]
async fn fire_and_forget_tree_spawn_shuts_down_observably() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let tree = OrderedTree::new().task(ChildSpec::task("worker", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver open");
            Ok(())
        }
    }));
    let handle = tree.handle();

    let _ = tree.spawn().expect("tree builds and spawns");
    timeout(WAIT, cancelled_rx.recv())
        .await
        .expect("temporary owner requests shutdown")
        .expect("test receiver open");
    handle.wait().await.expect("temporary owner drains runtime");
}

#[tokio::test]
async fn pre_spawn_snapshot_subscription_follows_the_spawned_identity() {
    let tree = OrderedTree::new().task(ChildSpec::task("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = tree.handle();
    let mut snapshots = handle.subscribe_snapshots();
    let declared = snapshots
        .latest()
        .child("worker")
        .expect("worker is projected before spawn")
        .clone();
    assert!(matches!(declared.state, ChildStateView::Starting { .. }));

    let spawned = tree.spawn().expect("tree builds and spawns");
    timeout(
        WAIT,
        snapshots.wait_for(|snapshot| {
            snapshot.child("worker").is_some_and(|worker| {
                worker.lineage == declared.lineage && worker.state.is_running()
            })
        }),
    )
    .await
    .expect("pre-spawn subscription observes startup")
    .expect("same snapshot stream remains open");

    assert_eq!(handle.snapshot(), spawned.handle().snapshot());
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
    assert!(handle.dynamic().is_none());
    let builder = builder.strategy(Strategy::RestForOne);
    assert_eq!(handle.snapshot().strategy, Strategy::RestForOne);
    drop(builder);
    assert_snapshot_receiver_closes(snapshots).await;

    let builder = DynamicTree::new();
    let handle = builder.handle();
    let snapshots = handle.subscribe_snapshots();
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);
    let _: DynamicRuntimeHandle = handle.clone();
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
    let tree = OrderedTree::new()
        .task(ChildSpec::task("task", |_| async { Ok(()) }))
        .actor(ActorSpec::new("actor", || Idle));
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
        .task(ChildSpec::task("duplicate", |_| async { Ok(()) }))
        .task(ChildSpec::task("duplicate", |_| async { Ok(()) }));
    let failed_ordered = tree.handle();
    let failed_ordered_snapshots = failed_ordered.subscribe_snapshots();
    assert!(tree.spawn().is_err());
    assert_snapshot_receiver_closes(failed_ordered_snapshots).await;

    let builder = DynamicTree::new().restart_config(RestartConfig::new(1, Duration::ZERO));
    let failed_dynamic = builder.handle();
    let failed_dynamic_snapshots = failed_dynamic.subscribe_snapshots();
    assert!(builder.spawn().is_err());
    assert_snapshot_receiver_closes(failed_dynamic_snapshots).await;

    let parent = OrderedTree::new().spawn().expect("ordered parent builds");
    assert!(parent.handle().dynamic().is_none());
    parent
        .shutdown_and_wait()
        .await
        .expect("ordered parent stops");

    let parent = DynamicTree::new().spawn().expect("dynamic parent builds");
    parent
        .handle()
        .wait_started()
        .await
        .expect("dynamic parent starts");

    let invalid = OrderedTree::new()
        .actor(ActorSpec::new("duplicate-binding", || Idle))
        .actor(ActorSpec::new("duplicate-binding", || Idle));
    let rejected = invalid.handle();
    let rejected_snapshots = rejected.subscribe_snapshots();
    assert!(matches!(
        parent.handle().add_subtree("invalid", invalid).await,
        Err(ControlError::Rejected(
            SupervisorBuildError::DuplicateChildId(label)
        )) if label == "duplicate-binding"
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;

    parent
        .handle()
        .add_subtree("occupied", DynamicTree::new())
        .await
        .expect("first subtree inserts");
    let duplicate = DynamicTree::new();
    let rejected = duplicate.handle();
    let rejected_snapshots = rejected.subscribe_snapshots();
    assert!(matches!(
        parent.handle().add_subtree("occupied", duplicate).await,
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
    outer
        .handle()
        .wait_started()
        .await
        .expect("dynamic outer starts");

    let (awaited_tx, mut awaited_rx) = mpsc::unbounded_channel();
    let awaited = outer
        .handle()
        .add_subtree("awaited", single_use_mount(awaited_tx))
        .await
        .expect("awaited subtree inserts");
    awaited
        .wait_started()
        .await
        .expect("awaited subtree starts");
    assert_eq!(next_report(&mut awaited_rx).await, "mounted");

    let (pipelined_tx, mut pipelined_rx) = mpsc::unbounded_channel();
    let pipelined_outer = outer.handle();
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
    let handle = OrderedTree::new()
        .actor(ActorSpec::new("ordinary", move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::new(AtomicUsize::new(0)),
            child_stopped: None,
            mutate_children_on_start: false,
        }))
        .spawn()
        .expect("runtime builds");
    handle.handle().wait_started().await.expect("actor starts");
    assert_eq!(next_report(&mut reports_rx).await, "ordered-supervisor");
    assert_eq!(next_report(&mut reports_rx).await, "none");
    handle.shutdown_and_wait().await.expect("runtime stops");
}

#[tokio::test]
async fn actor_with_dynamic_scope_injects_children_for_on_start_and_handler_mutation() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let leader_spec = ActorSpec::new("leader", {
        let starts = Arc::clone(&starts);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: None,
            mutate_children_on_start: true,
        }
    });
    let leader = leader_spec.actor_ref();
    let handle = OrderedTree::new()
        .actor_with_scope(
            "owned",
            leader_spec,
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
        .handle()
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
        .handle()
        .subtree("owned")
        .and_then(|owned| owned.subtree("children"))
        .expect("owned dynamic scope is registered");
    assert!(children.snapshot().child("from-on-start").is_some());
    assert!(children.snapshot().child("from-handler").is_some());
    let snapshot = handle.handle().snapshot();
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
    let (subtree_tx, mut subtree_rx) = mpsc::unbounded_channel();
    let adder_spec = ActorSpec::new("adder", move || RestrictedTaskAdder {
        lineage: lineage_tx.clone(),
        subtree: subtree_tx.clone(),
    });
    let adder = adder_spec.actor_ref();
    let handle = OrderedTree::new()
        .actor_with_scope("owned", adder_spec, DynamicTree::new(), Strategy::OneForOne)
        .spawn()
        .expect("tree builds");
    handle.handle().wait_started().await.expect("tree starts");

    adder.send(()).await.expect("adder receives command");
    let lineage = timeout(WAIT, lineage_rx.recv())
        .await
        .expect("timed out waiting for lineage")
        .expect("lineage channel remains open");
    let restricted_subtree = timeout(WAIT, subtree_rx.recv())
        .await
        .expect("timed out waiting for restricted subtree")
        .expect("subtree channel remains open");
    assert!(restricted_subtree.dynamic().is_none());
    let children = handle
        .handle()
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
    let leader = ActorSpec::new("leader", {
        let starts = Arc::clone(&starts);
        let child_stopped = Arc::clone(&child_stopped);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: Some(Arc::clone(&child_stopped)),
            mutate_children_on_start: false,
        }
    });
    let worker = ActorSpec::new("worker", {
        let child_stopped = Arc::clone(&child_stopped);
        move || StopProbe(Arc::clone(&child_stopped))
    });
    let runtime = OrderedTree::new().actor_with_scope(
        "owned",
        leader,
        OrderedTree::new().actor(worker),
        Strategy::RestForOne,
    );
    assert_eq!(runtime.handle().snapshot().strategy, Strategy::OneForOne);
    let handle = runtime.spawn().expect("ordered ActorWithScope builds");
    assert_eq!(next_report(&mut reports_rx).await, "some");
    assert_eq!(next_report(&mut reports_rx).await, "ordered-children");
    handle
        .handle()
        .wait_started()
        .await
        .expect("ordered child starts");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    let snapshot = handle.handle().snapshot();
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
    let leader_spec = ActorSpec::new("leader", {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let leader = leader_spec.actor_ref();
    let worker_spec = ActorSpec::new("worker", {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let worker = worker_spec.actor_ref();
    let tree = OrderedTree::new().actor_with_scope(
        "owned",
        leader_spec,
        OrderedTree::new().actor(worker_spec),
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
    handle.handle().wait_started().await.expect("tree starts");

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
    let leader = ActorSpec::new("leader", {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let worker_spec = ActorSpec::new("worker", {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let worker = worker_spec.actor_ref();
    let inner = OrderedTree::new()
        .actor(worker_spec)
        .restart_config(RestartConfig::new(1, Duration::from_secs(30)));
    let handle = OrderedTree::new()
        .actor_with_scope("owned", leader, inner, Strategy::OneForAll)
        .spawn()
        .expect("tree builds");
    handle.handle().wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("first crash");
    wait_count(&worker_starts, 2).await;
    worker.send(LeaderMsg::Crash).await.expect("second crash");
    wait_count(&leader_starts, 2).await;

    handle.shutdown_and_wait().await.expect("tree stops");
}

#[tokio::test]
async fn consuming_a_graph_into_a_tree_preserves_issued_actor_refs() {
    let mut graph = GraphBuilder::new();
    let actor_ref = graph.actor(ActorSpec::new("actor", || Idle));
    let graph = graph.build().expect("graph builds");

    let tree = OrderedTree::graph(graph);
    let spawned = tree.spawn().expect("tree builds and spawns");
    spawned.handle().wait_started().await.expect("tree starts");
    actor_ref.send(()).await.expect("issued ref remains bound");

    spawned
        .shutdown_and_wait()
        .await
        .expect("tree stops cleanly");
}

#[tokio::test]
async fn duplicate_actor_bindings_are_rejected_during_tree_lowering() {
    let tree = OrderedTree::new()
        .actor(ActorSpec::new("actor", || Idle))
        .actor(ActorSpec::new("actor", || Idle));
    let handle = tree.handle();

    assert!(matches!(
        tree.spawn(),
        Err(SupervisorBuildError::DuplicateChildId(label)) if label == "actor"
    ));
    assert_snapshot_stream_closes(&handle).await;
}
