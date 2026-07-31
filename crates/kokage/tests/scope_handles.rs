mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorSpec, BoxError, BuildError, Context, ControlError, DynamicTree, ExitResult,
    RestartPolicy, ScopeRef, StopContext, Strategy, TaskSpec, Tree,
    observe::{ChildStateView, ScopeKind, SupervisorSnapshotReceiver},
};
use tokio::{sync::mpsc, time::timeout};

const WAIT: Duration = Duration::from_secs(3);

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
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
    children_id: Option<&'static str>,
    mutate_children_on_start: bool,
}

impl Actor for ScopeProbe {
    type Msg = LeaderMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let Some(children_id) = self.children_id else {
            let scope = ctx.scope();
            assert_eq!(scope.kind(), ScopeKind::Ordered);
            self.reports
                .send("ordered-supervisor")
                .expect("test receiver open");
            self.reports.send("none").expect("test receiver open");
            return Ok(());
        };
        let children = ctx
            .scope()
            .subtree(children_id)
            .expect("declared child scope resolves before startup");
        self.reports.send("some").expect("test receiver open");
        if children.kind() != ScopeKind::Dynamic {
            self.reports
                .send("ordered-children")
                .expect("test receiver open");
            return Ok(());
        }
        // Task insertion schedules startup rather than awaiting readiness.
        let before_ready = children
            .add_task_spec(TaskSpec::new("too-early", |_| async { Ok(()) }))
            .await;
        assert!(matches!(before_ready, Err(ControlError::Unavailable)));
        self.reports
            .send("unavailable-before-ready")
            .expect("test receiver open");

        if !self.mutate_children_on_start {
            return Ok(());
        }

        // The bounded wait runs outside startup and returns as a later
        // loop-owned offload completion.
        ctx.offload(
            WAIT,
            async move {
                children.wait_started().await.map_err(|_| ())?;
                children
                    .add_actor_spec(ActorSpec::new("from-on-start", || Idle))
                    .await
                    .map_err(|_| ())?;
                Ok::<_, ()>(())
            },
            |result| LeaderMsg::OnStartAdded(matches!(result, Ok(Ok(())))),
        )
        .detach();
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            LeaderMsg::AddFromHandler => {
                let children = ctx
                    .scope()
                    .subtree(self.children_id.expect("leader declares a child scope"))
                    .expect("declared child scope remains registered");
                children
                    .add_actor_spec(ActorSpec::new("from-handler", || Idle))
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

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct BuilderHandleOwner {
    mount: ScopeRef,
    report: mpsc::UnboundedSender<&'static str>,
}

impl Actor for BuilderHandleOwner {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.mount
            .add_actor_spec(ActorSpec::new("owned", || Idle))
            .await?;
        self.report.send("mounted").expect("test receiver open");
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct TaskAdder {
    inserted: mpsc::UnboundedSender<()>,
    subtree: mpsc::UnboundedSender<ScopeRef>,
}

struct DynamicCompletionLeader {
    reports: mpsc::UnboundedSender<&'static str>,
}

impl Actor for DynamicCompletionLeader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let dynamic = ctx.scope();
        let first = dynamic
            .add_task_spec(TaskSpec::new("first", |_| async { Ok(()) }))
            .await?;
        let second = dynamic
            .add_task_spec(TaskSpec::new("second", |_| async { Ok(()) }))
            .await?;
        self.reports.send("inserted").expect("test receiver open");
        first.wait().await?;
        second.wait().await?;
        self.reports.send("completed").expect("test receiver open");
        dynamic.shutdown();
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

impl Actor for TaskAdder {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
        let children = ctx
            .scope()
            .subtree("children")
            .expect("actor's declared child scope is registered");
        children
            .add_task_spec(TaskSpec::new("task", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await?;
        self.inserted.send(()).expect("test receiver open");
        let subtree = children.add_subtree("subtree", Tree::new()).await?;
        self.subtree.send(subtree).expect("test receiver open");
        Ok(())
    }
}

fn single_use_mount(report: mpsc::UnboundedSender<&'static str>) -> Tree {
    let mount_builder = DynamicTree::new();
    let mount = mount_builder.scope();
    let mut tree = Tree::new();
    tree.add_subtree("mount", mount_builder);
    tree.add_actor_spec(ActorSpec::new("owner", move || BuilderHandleOwner {
        mount: mount.clone(),
        report: report.clone(),
    }));
    tree
}

async fn next_report(reports: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
    timeout(WAIT, reports.recv())
        .await
        .expect("timed out waiting for report")
        .expect("report channel closed")
}

async fn assert_snapshot_stream_closes(handle: &ScopeRef) {
    assert_snapshot_receiver_closes(handle.snapshots()).await;
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
    let pre_spawn = tree.scope();
    let spawned = tree.spawn().expect("tree builds and spawns");

    pre_spawn
        .wait_started()
        .await
        .expect("pre-spawn scope starts");
    pre_spawn
        .add_actor_spec(ActorSpec::new("worker", || Idle))
        .await
        .expect("pre-spawn handle controls the spawned scope");
    assert!(spawned.scope().snapshot().child("worker").is_some());

    pre_spawn
        .shutdown_and_wait()
        .await
        .expect("pre-spawn handle stops the spawned scope");
}

#[tokio::test]
async fn dynamic_capability_tracks_root_and_nested_scope_kinds() {
    let mut tree = Tree::new();
    tree.add_subtree("ordered", Tree::new());
    tree.add_subtree("dynamic", DynamicTree::new());
    let runtime = tree.spawn().expect("mixed tree builds");
    let root = runtime.scope();
    let ordered = root.subtree("ordered").expect("ordered subtree handle");
    let dynamic = root.subtree("dynamic").expect("dynamic subtree handle");

    assert_eq!(root.kind(), ScopeKind::Ordered);
    assert_eq!(ordered.kind(), ScopeKind::Ordered);
    assert_eq!(dynamic.kind(), ScopeKind::Dynamic);

    runtime.shutdown().await.expect("runtime stops");

    let dynamic_tree = DynamicTree::new();
    let pre_spawn = dynamic_tree.scope();
    let dynamic_root = dynamic_tree.spawn().expect("dynamic root builds");
    let post_spawn = support::dynamic_root(&dynamic_root);

    pre_spawn
        .add_task_spec(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("pre-spawn capability mutates spawned root");
    assert!(post_spawn.snapshot().child("worker").is_some());
    assert_eq!(pre_spawn.snapshot(), post_spawn.snapshot());
    post_spawn
        .remove_child("worker")
        .await
        .expect("post-spawn capability reaches the same membership");
    assert!(pre_spawn.snapshot().child("worker").is_none());
    assert_eq!(pre_spawn.snapshot(), post_spawn.snapshot());

    dynamic_root.shutdown().await.expect("dynamic root stops");
}

#[tokio::test]
async fn dynamic_task_ref_waits_for_completion() {
    let tree = DynamicTree::new();
    let dynamic = tree.scope();
    let runtime = tree.spawn().expect("dynamic root builds");
    let task = dynamic
        .add_task_spec(TaskSpec::new("future", |_| async { Ok(()) }))
        .await
        .expect("future member added");
    let exit = timeout(WAIT, task.wait())
        .await
        .expect("completion wait resolves")
        .expect("task remains observable");
    assert!(exit.is_completed());

    runtime.shutdown().await.expect("dynamic root stops");
}

#[tokio::test]
async fn completed_dynamic_task_can_trigger_explicit_scope_shutdown() {
    let tree = DynamicTree::new();
    let dynamic = tree.scope();
    let runtime = tree.spawn().expect("dynamic root builds");

    let task = dynamic
        .add_task_spec(TaskSpec::new("future", |_| async { Ok(()) }))
        .await
        .expect("future member added");
    task.wait().await.expect("task completion observed");
    dynamic.shutdown();
    timeout(WAIT, runtime.wait())
        .await
        .expect("completion requests shutdown")
        .expect("dynamic root stops");
}

#[tokio::test]
async fn pre_spawn_task_ref_observes_a_fast_child() {
    let mut tree = Tree::new();
    let task = tree.add_task_spec(TaskSpec::new("fast", |_| async { Ok(()) }));
    let scope = tree.scope();

    let runtime = tree.spawn().expect("ordered tree builds");
    let exit = timeout(WAIT, task.wait())
        .await
        .expect("fast completion remains observable")
        .expect("task ref remains available");
    assert!(exit.is_completed());
    scope.shutdown();
    timeout(WAIT, runtime.wait())
        .await
        .expect("scope shutdown completes")
        .expect("scope stops cleanly");
}

#[tokio::test]
async fn ordered_scope_membership_methods_return_not_dynamic() {
    let runtime = Tree::new().spawn().expect("ordered tree builds");

    assert!(matches!(
        runtime
            .scope()
            .add_actor_spec(ActorSpec::new("actor", || Idle))
            .await,
        Err(ControlError::NotDynamic)
    ));
    assert!(matches!(
        runtime
            .scope()
            .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
            .await,
        Err(ControlError::NotDynamic)
    ));
    assert!(matches!(
        runtime.scope().add_subtree("subtree", Tree::new()).await,
        Err(ControlError::NotDynamic)
    ));
    assert!(matches!(
        runtime.scope().remove_child("missing").await,
        Err(ControlError::NotDynamic)
    ));

    runtime.shutdown().await.expect("runtime stops");
}

struct OrderedScopeProbe {
    checked: mpsc::UnboundedSender<()>,
}

impl Actor for OrderedScopeProbe {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let scope = ctx.scope();
        assert!(matches!(
            scope.add_actor_spec(ActorSpec::new("actor", || Idle)).await,
            Err(ControlError::NotDynamic)
        ));
        assert!(matches!(
            scope
                .add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }))
                .await,
            Err(ControlError::NotDynamic)
        ));
        assert!(matches!(
            scope.add_subtree("subtree", Tree::new()).await,
            Err(ControlError::NotDynamic)
        ));
        assert!(matches!(
            scope.remove_child("missing").await,
            Err(ControlError::NotDynamic)
        ));
        self.checked.send(()).expect("test receiver open");
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn ordered_context_scope_membership_methods_return_not_dynamic() {
    let (checked, mut checked_rx) = mpsc::unbounded_channel();
    let probe = ActorSpec::new("probe", move || OrderedScopeProbe {
        checked: checked.clone(),
    });
    let mut tree = Tree::new();
    tree.add_actor_spec(probe);
    let runtime = tree.spawn().expect("ordered tree builds");

    timeout(WAIT, checked_rx.recv())
        .await
        .expect("scope probe runs")
        .expect("scope probe reports");
    runtime.shutdown().await.expect("runtime stops");
}

struct StopScopeProbe {
    observed: mpsc::UnboundedSender<(ScopeKind, bool)>,
}

impl Actor for StopScopeProbe {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        // The shutdown hook holds up its own detach, so observation and
        // fire-and-forget control are what a scope handle is good for here.
        let scope: ScopeRef = ctx.scope();
        let visible = scope.snapshot().child("probe").is_some();
        scope.shutdown();
        self.observed
            .send((scope.kind(), visible))
            .expect("test receiver open");
        Ok(())
    }
}

#[tokio::test]
async fn stop_context_scope_observes_and_controls_its_scope() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_actor_spec(ActorSpec::new("probe", move || StopScopeProbe {
        observed: observed_tx.clone(),
    }));
    let runtime = tree.spawn().expect("ordered tree builds");

    runtime.shutdown().await.expect("runtime stops");

    let (kind, visible) = timeout(WAIT, observed_rx.recv())
        .await
        .expect("stop hook reports")
        .expect("report channel remains open");
    assert_eq!(kind, ScopeKind::Ordered);
    assert!(visible, "the stopping child is still visible to its scope");
}

#[tokio::test]
async fn dropping_every_root_and_nested_handle_leaves_the_owned_runtime_running() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let mut nested_tree = Tree::new();
    nested_tree.add_task_spec(TaskSpec::new("worker", move |ctx| {
        let lifecycle_tx = lifecycle_tx.clone();
        async move {
            lifecycle_tx.send("started").expect("test receiver open");
            ctx.shutdown_token().cancelled().await;
            lifecycle_tx.send("cancelled").expect("test receiver open");
            Ok(())
        }
    }));
    let mut tree = Tree::new();
    tree.add_subtree("nested", nested_tree);
    let runtime = tree.spawn().expect("tree builds and spawns");
    let root = runtime.scope();
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

    runtime.scope().shutdown();
    assert_eq!(next_report(&mut lifecycle_rx).await, "cancelled");
    runtime.wait().await.expect("runtime stops cleanly");
}

#[tokio::test]
async fn dropping_runtime_requests_graceful_shutdown() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("worker", move |ctx| {
        let lifecycle_tx = lifecycle_tx.clone();
        async move {
            lifecycle_tx.send("started").expect("test receiver open");
            ctx.shutdown_token().cancelled().await;
            lifecycle_tx.send("cancelled").expect("test receiver open");
            Ok(())
        }
    }));
    let runtime = tree.spawn().expect("tree builds and spawns");
    let handle = runtime.scope();

    assert_eq!(next_report(&mut lifecycle_rx).await, "started");
    drop(runtime);
    assert_eq!(next_report(&mut lifecycle_rx).await, "cancelled");
    handle.wait().await.expect("owner drop drains runtime");
}

#[tokio::test]
async fn fire_and_forget_tree_spawn_shuts_down_observably() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("worker", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver open");
            Ok(())
        }
    }));
    let handle = tree.scope();

    let _ = tree.spawn().expect("tree builds and spawns");
    timeout(WAIT, cancelled_rx.recv())
        .await
        .expect("temporary owner requests shutdown")
        .expect("test receiver open");
    handle.wait().await.expect("temporary owner drains runtime");
}

#[tokio::test]
async fn pre_spawn_snapshot_subscription_follows_the_spawned_identity() {
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = tree.scope();
    let mut snapshots = handle.snapshots();
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

    assert_eq!(handle.snapshot(), spawned.scope().snapshot());
    spawned.shutdown().await.expect("spawned tree stops");
}

#[tokio::test]
async fn trees_terminalize_handles_when_dropped() {
    let builder = Tree::new();
    let handle = builder.scope();
    let snapshots = handle.snapshots();
    assert_eq!(handle.snapshot().kind, ScopeKind::Ordered);
    assert_eq!(handle.kind(), ScopeKind::Ordered);
    let builder = builder.strategy(Strategy::RestForOne);
    assert_eq!(handle.snapshot().strategy, Strategy::RestForOne);
    drop(builder);
    assert_snapshot_receiver_closes(snapshots).await;

    let builder = DynamicTree::new();
    let handle = builder.scope();
    let snapshots = handle.snapshots();
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);
    let _: ScopeRef = handle.clone();
    drop(builder);
    assert_snapshot_receiver_closes(snapshots).await;

    let child = DynamicTree::new();
    let child_handle = child.scope();
    let child_snapshots = child_handle.snapshots();
    let mut parent = Tree::new();
    parent.add_subtree("child", child);
    drop(parent);
    assert_snapshot_receiver_closes(child_snapshots).await;
}

#[test]
fn tree_strategy_preserves_declared_pre_spawn_snapshot() {
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("task", |_| async { Ok(()) }));
    tree.add_actor_spec(ActorSpec::new("actor", || Idle));
    let handle = tree.scope();
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
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("duplicate", |_| async { Ok(()) }));
    tree.add_task_spec(TaskSpec::new("duplicate", |_| async { Ok(()) }));
    let failed_ordered = tree.scope();
    let failed_ordered_snapshots = failed_ordered.snapshots();
    assert!(tree.spawn().is_err());
    assert_snapshot_receiver_closes(failed_ordered_snapshots).await;

    let builder =
        DynamicTree::new().default_restart(RestartPolicy::on_failure().limit(1, Duration::ZERO));
    let failed_dynamic = builder.scope();
    let failed_dynamic_snapshots = failed_dynamic.snapshots();
    assert!(builder.spawn().is_err());
    assert_snapshot_receiver_closes(failed_dynamic_snapshots).await;

    let parent = Tree::new().spawn().expect("ordered parent builds");
    assert_eq!(parent.scope().kind(), ScopeKind::Ordered);
    parent.shutdown().await.expect("ordered parent stops");

    let parent = DynamicTree::new().spawn().expect("dynamic parent builds");
    parent
        .scope()
        .wait_started()
        .await
        .expect("dynamic parent starts");

    let mut invalid = Tree::new();
    invalid.add_actor_spec(ActorSpec::new("duplicate-binding", || Idle));
    invalid.add_actor_spec(ActorSpec::new("duplicate-binding", || Idle));
    let rejected = invalid.scope();
    let rejected_snapshots = rejected.snapshots();
    assert!(matches!(
        support::dynamic_root(&parent)
            .add_subtree("invalid", invalid)
            .await,
        Err(ControlError::Rejected(
            BuildError::DuplicateChildId(label)
        )) if label == "duplicate-binding"
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;

    support::dynamic_root(&parent)
        .add_subtree("occupied", DynamicTree::new())
        .await
        .expect("first subtree inserts");
    let duplicate = DynamicTree::new();
    let rejected = duplicate.scope();
    let rejected_snapshots = rejected.snapshots();
    assert!(matches!(
        support::dynamic_root(&parent)
            .add_subtree("occupied", duplicate)
            .await,
        Err(ControlError::Rejected(BuildError::DuplicateChildId(id)))
            if id == "occupied"
    ));
    assert_snapshot_receiver_closes(rejected_snapshots).await;
    parent.shutdown().await.expect("dynamic parent stops");
}

#[tokio::test]
async fn pre_spawn_mount_handle_supports_awaited_and_pipelined_subtree_adds() {
    let outer = DynamicTree::new().spawn().expect("dynamic outer builds");
    outer
        .scope()
        .wait_started()
        .await
        .expect("dynamic outer starts");

    let (awaited_tx, mut awaited_rx) = mpsc::unbounded_channel();
    let awaited = support::dynamic_root(&outer)
        .add_subtree("awaited", single_use_mount(awaited_tx))
        .await
        .expect("awaited subtree inserts");
    awaited
        .wait_started()
        .await
        .expect("awaited subtree starts");
    assert_eq!(next_report(&mut awaited_rx).await, "mounted");

    let (pipelined_tx, mut pipelined_rx) = mpsc::unbounded_channel();
    let pipelined_outer = support::dynamic_root(&outer);
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

    outer.shutdown().await.expect("outer stops");
}

#[tokio::test]
async fn ordinary_actor_gets_its_scope_but_no_owned_children() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_actor_spec(ActorSpec::new("ordinary", move || ScopeProbe {
        reports: reports_tx.clone(),
        starts: Arc::new(AtomicUsize::new(0)),
        child_stopped: None,
        children_id: None,
        mutate_children_on_start: false,
    }));
    let handle = tree.spawn().expect("runtime builds");
    handle.scope().wait_started().await.expect("actor starts");
    assert_eq!(next_report(&mut reports_rx).await, "ordered-supervisor");
    assert_eq!(next_report(&mut reports_rx).await, "none");
    handle.shutdown().await.expect("runtime stops");
}

#[tokio::test]
async fn declared_dynamic_scope_resolves_during_on_start_and_supports_handler_mutation() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let leader_spec = ActorSpec::new("leader", {
        let starts = Arc::clone(&starts);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: None,
            children_id: Some("children"),
            mutate_children_on_start: true,
        }
    });
    let leader = leader_spec.actor_ref();
    let mut owned = Tree::new().strategy(Strategy::RestForOne);
    owned.add_actor_spec(leader_spec);
    owned.add_subtree("children", DynamicTree::new());
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    let handle = tree.spawn().expect("leader-owned scope builds");

    assert_eq!(next_report(&mut reports_rx).await, "some");
    assert_eq!(
        next_report(&mut reports_rx).await,
        "unavailable-before-ready"
    );
    handle
        .scope()
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
        .scope()
        .subtree("owned")
        .and_then(|owned| owned.subtree("children"))
        .expect("owned dynamic scope is registered");
    assert!(children.snapshot().child("from-on-start").is_some());
    assert!(children.snapshot().child("from-handler").is_some());
    let snapshot = handle.scope().snapshot();
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

    handle.shutdown().await.expect("runtime stops");
}

#[tokio::test]
async fn context_scope_add_task_reports_insertion_success() {
    let (inserted_tx, mut inserted_rx) = mpsc::unbounded_channel();
    let (subtree_tx, mut subtree_rx) = mpsc::unbounded_channel();
    let adder_spec = ActorSpec::new("adder", move || TaskAdder {
        inserted: inserted_tx.clone(),
        subtree: subtree_tx.clone(),
    });
    let adder = adder_spec.actor_ref();
    let mut owned = Tree::new();
    owned.add_actor_spec(adder_spec);
    owned.add_subtree("children", DynamicTree::new());
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    let handle = tree.spawn().expect("tree builds");
    handle.scope().wait_started().await.expect("tree starts");

    adder.send(()).await.expect("adder receives command");
    timeout(WAIT, inserted_rx.recv())
        .await
        .expect("timed out waiting for insertion")
        .expect("insertion channel remains open");
    let subtree = timeout(WAIT, subtree_rx.recv())
        .await
        .expect("timed out waiting for subtree")
        .expect("subtree channel remains open");
    assert_eq!(subtree.kind(), ScopeKind::Ordered);
    let children = handle
        .scope()
        .subtree("owned")
        .and_then(|owned| owned.subtree("children"))
        .expect("owned dynamic scope is registered");
    assert!(children.snapshot().child("task").is_some());

    handle.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn dynamic_context_scope_uses_task_refs_for_completion() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let runtime = DynamicTree::new().spawn().expect("dynamic root builds");
    support::dynamic_root(&runtime)
        .add_actor_spec(ActorSpec::new("leader", move || DynamicCompletionLeader {
            reports: reports_tx.clone(),
        }))
        .await
        .expect("leader inserted");

    assert_eq!(next_report(&mut reports_rx).await, "inserted");
    assert_eq!(next_report(&mut reports_rx).await, "completed");
    timeout(WAIT, runtime.wait())
        .await
        .expect("task completion requests shutdown")
        .expect("dynamic root stops");
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
            children_id: Some("children"),
            mutate_children_on_start: false,
        }
    });
    let worker = ActorSpec::new("worker", {
        let child_stopped = Arc::clone(&child_stopped);
        move || StopProbe(Arc::clone(&child_stopped))
    });
    let mut children = Tree::new();
    children.add_actor_spec(worker);
    let mut owned = Tree::new().strategy(Strategy::RestForOne);
    owned.add_actor_spec(leader);
    owned.add_subtree("children", children);
    let mut runtime = Tree::new();
    runtime.add_subtree("owned", owned);
    assert_eq!(runtime.scope().snapshot().strategy, Strategy::OneForOne);
    let handle = runtime.spawn().expect("ordered leader-owned scope builds");
    assert_eq!(next_report(&mut reports_rx).await, "some");
    assert_eq!(next_report(&mut reports_rx).await, "ordered-children");
    handle
        .scope()
        .wait_started()
        .await
        .expect("ordered child starts");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    let snapshot = handle.scope().snapshot();
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
    handle.shutdown().await.expect("runtime stops");
    assert!(child_stopped.load(Ordering::SeqCst));
}

struct RestartProbe {
    starts: Arc<AtomicUsize>,
}

impl Actor for RestartProbe {
    type Msg = LeaderMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
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
async fn leader_owned_scope_uses_explicit_rest_for_one() {
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
    let mut children = Tree::new();
    children.add_actor_spec(worker_spec);
    let mut owned = Tree::new().strategy(Strategy::RestForOne);
    owned.add_actor_spec(leader_spec);
    owned.add_subtree("children", children);
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    #[cfg(feature = "serde")]
    {
        let outline = tree.outline();
        assert!(matches!(
            outline.child("owned"),
            Some(kokage::observe::ChildOutline::Scope { outline, .. })
                if outline.strategy == Strategy::RestForOne
        ));
    }
    let handle = tree.spawn().expect("tree builds");
    handle.scope().wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("worker crashes");
    wait_count(&worker_starts, 2).await;
    assert_eq!(leader_starts.load(Ordering::SeqCst), 1);

    leader.send(LeaderMsg::Crash).await.expect("leader crashes");
    wait_count(&leader_starts, 2).await;
    wait_count(&worker_starts, 3).await;
    handle.shutdown().await.expect("tree stops");
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
    let mut inner = Tree::new();
    inner.add_actor_spec(worker_spec);
    let inner =
        inner.default_restart(RestartPolicy::on_failure().limit(1, Duration::from_secs(30)));
    let mut owned = Tree::new().strategy(Strategy::OneForAll);
    owned.add_actor_spec(leader);
    owned.add_subtree("children", inner);
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    let handle = tree.spawn().expect("tree builds");
    handle.scope().wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("first crash");
    wait_count(&worker_starts, 2).await;
    worker.send(LeaderMsg::Crash).await.expect("second crash");
    wait_count(&leader_starts, 2).await;

    handle.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn consuming_a_tree_builder_preserves_issued_actor_refs() {
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new("actor", || Idle));
    let tree = builder;

    let spawned = tree.spawn().expect("tree builds and spawns");
    spawned.scope().wait_started().await.expect("tree starts");
    actor_ref.send(()).await.expect("issued ref remains bound");

    spawned.shutdown().await.expect("tree stops cleanly");
}

#[tokio::test]
async fn duplicate_actor_bindings_are_rejected_during_tree_lowering() {
    let mut tree = Tree::new();
    tree.add_actor_spec(ActorSpec::new("actor", || Idle));
    tree.add_actor_spec(ActorSpec::new("actor", || Idle));
    let handle = tree.scope();

    assert!(matches!(
        tree.spawn(),
        Err(BuildError::DuplicateChildId(label)) if label == "actor"
    ));
    assert_snapshot_stream_closes(&handle).await;
}

#[tokio::test]
async fn sibling_scopes_may_reuse_the_same_local_actor_id() {
    let mut left = Tree::new();
    left.add_actor_spec(ActorSpec::new("worker", || Idle));
    let mut right = Tree::new();
    right.add_actor_spec(ActorSpec::new("worker", || Idle));
    let mut tree = Tree::new();
    tree.add_subtree("left", left);
    tree.add_subtree("right", right);

    let runtime = tree.spawn().expect("sibling-local ids are independent");
    runtime.scope().wait_started().await.expect("tree starts");
    let snapshot = runtime.scope().snapshot();
    for scope in ["left", "right"] {
        assert!(
            snapshot
                .child(scope)
                .and_then(|child| child.supervisor.as_deref())
                .and_then(|subtree| subtree.child("worker"))
                .is_some(),
            "{scope}.worker exists"
        );
    }
    runtime.shutdown().await.expect("tree stops");
}
