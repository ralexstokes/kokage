use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::{
    Actor, ActorResult, ActorScope, AddSubtreeError, BoxError, ControlError, DynamicActorOptions,
    GraphBuilder, HandleContext, RestartIntensity, Runtime, RuntimeBuilder, RuntimeHandle,
    ScopeKind, StartContext, StopContext, Strategy, SupervisionTree,
    prelude::{Continue, Stop},
};
use tokio_supervisor::ChildSpec;

const WAIT: Duration = Duration::from_secs(3);

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(Continue)
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let Some(children) = ctx.children() else {
            let supervisor = ctx.supervisor();
            assert_eq!(supervisor.snapshot().kind, ScopeKind::Ordered);
            assert!(matches!(
                supervisor
                    .add_actor("forbidden", || Idle, DynamicActorOptions::new())
                    .await,
                Err(ControlError::UnsupportedByScopeKind { .. })
            ));
            self.reports
                .send("ordered-supervisor")
                .expect("test receiver open");
            self.reports.send("none").expect("test receiver open");
            return Ok(Continue);
        };
        self.reports.send("some").expect("test receiver open");
        let before_ready = children
            .supervisor_handle()
            .add_child(ChildSpec::new("too-early", |_| async { Ok(()) }))
            .await;
        assert!(matches!(before_ready, Err(ControlError::Unavailable)));
        self.reports
            .send("unavailable-before-ready")
            .expect("test receiver open");

        if !self.mutate_children_on_start {
            return Ok(Continue);
        }

        // The factory signature remains `|| ScopeProbe { .. }`: the runtime
        // injects `children`. Work launched from on_start waits for the inner
        // scope to bind, then mutates it without blocking leader readiness.
        // `after_start` is where the lifecycle waits become reachable again —
        // taking it here names the handoff that used to be a doc comment.
        let children = children.after_start();
        let myself = ctx.myself();
        tokio::spawn(async move {
            let result = async {
                children.wait_started().await.map_err(|_| ())?;
                children
                    .add_actor("from-on-start", || Idle, DynamicActorOptions::new())
                    .await
                    .map_err(|_| ())?;
                Ok::<_, ()>(())
            }
            .await;
            let _ = myself.send(LeaderMsg::OnStartAdded(result.is_ok())).await;
        });
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut HandleContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            LeaderMsg::AddFromHandler => {
                let children = ctx.children().expect("ActorWithScope leader has children");
                children
                    .add_actor("from-handler", || Idle, DynamicActorOptions::new())
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
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
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

    async fn handle(&mut self, (): (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, ()>) -> Result<(), BoxError> {
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

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.mount
            .add_actor("owned", || Idle, DynamicActorOptions::new())
            .await?;
        self.report.send("mounted").expect("test receiver open");
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }
}

fn builder_owned_mount(report: mpsc::UnboundedSender<&'static str>) -> RuntimeBuilder {
    let mount_builder = Runtime::dynamic();
    let mount = mount_builder.handle();
    let mut graph = GraphBuilder::new();
    graph.actor("owner", move || BuilderHandleOwner {
        mount: mount.clone(),
        report: report.clone(),
    });
    Runtime::builder()
        .subtree("mount", mount_builder)
        .graph(graph.build().expect("owner graph builds"))
}

async fn next_report(reports: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
    timeout(WAIT, reports.recv())
        .await
        .expect("timed out waiting for report")
        .expect("report channel closed")
}

async fn assert_snapshot_stream_closes(handle: &RuntimeHandle) {
    let mut snapshots = handle.subscribe_snapshots();
    timeout(
        WAIT,
        async move { while snapshots.changed().await.is_ok() {} },
    )
    .await
    .expect("snapshot stream closes");
}

#[tokio::test]
async fn runtime_builders_reserve_handles_and_terminalize_when_dropped() {
    let builder = Runtime::builder();
    let handle = builder.handle();
    assert_eq!(handle.snapshot().kind, ScopeKind::Ordered);
    assert!(matches!(
        handle
            .supervisor_handle()
            .add_child(ChildSpec::new("early", |_| async { Ok(()) }))
            .await,
        Err(ControlError::Unavailable)
    ));
    let builder = builder.strategy(Strategy::RestForOne);
    assert_eq!(handle.snapshot().strategy, Strategy::RestForOne);
    drop(builder);
    assert_snapshot_stream_closes(&handle).await;

    let builder = Runtime::builder();
    let handle = builder.handle();
    let runtime = builder.build().expect("ordered runtime builds");
    drop(runtime);
    assert_snapshot_stream_closes(&handle).await;

    let builder = Runtime::dynamic();
    let handle = builder.handle();
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);
    drop(builder);
    assert_snapshot_stream_closes(&handle).await;
}

#[test]
fn runtime_builder_strategy_preserves_declared_pre_spawn_snapshot() {
    let mut graph = GraphBuilder::new();
    graph.actor("actor", || Idle);
    let builder = Runtime::builder()
        .child(ChildSpec::new("task", |_| async { Ok(()) }))
        .graph(graph.build().expect("graph builds"));
    let handle = builder.handle();
    let declared_before = handle
        .snapshot()
        .children
        .into_iter()
        .map(|child| child.id)
        .collect::<Vec<_>>();

    let builder = builder.strategy(Strategy::RestForOne);
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
    drop(builder);
}

#[tokio::test]
async fn runtime_build_errors_and_rejected_subtrees_terminalize_reserved_handles() {
    let builder = Runtime::builder()
        .child(ChildSpec::new("duplicate", |_| async { Ok(()) }))
        .child(ChildSpec::new("duplicate", |_| async { Ok(()) }));
    let failed_ordered = builder.handle();
    assert!(builder.build().is_err());
    assert_snapshot_stream_closes(&failed_ordered).await;

    let builder = Runtime::dynamic().restart_intensity(RestartIntensity::new(1, Duration::ZERO));
    let failed_dynamic = builder.handle();
    assert!(builder.build().is_err());
    assert_snapshot_stream_closes(&failed_dynamic).await;

    let parent = Runtime::builder()
        .build()
        .expect("ordered parent builds")
        .spawn();
    parent.wait_started().await.expect("ordered parent starts");
    let child = Runtime::dynamic();
    let rejected = child.handle();
    assert!(matches!(
        parent.add_subtree("rejected", child).await,
        Err(AddSubtreeError::Control(
            ControlError::UnsupportedByScopeKind { .. }
        ))
    ));
    assert_snapshot_stream_closes(&rejected).await;
    parent
        .shutdown_and_wait()
        .await
        .expect("ordered parent stops");
}

#[tokio::test]
async fn builder_owned_mount_handle_supports_awaited_and_pipelined_subtree_adds() {
    let outer = Runtime::dynamic()
        .build()
        .expect("dynamic outer builds")
        .spawn();
    outer.wait_started().await.expect("dynamic outer starts");

    let (awaited_tx, mut awaited_rx) = mpsc::unbounded_channel();
    let awaited = outer
        .add_subtree("awaited", builder_owned_mount(awaited_tx))
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
            .add_subtree("pipelined", builder_owned_mount(pipelined_tx))
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
    graph.actor("ordinary", move || ScopeProbe {
        reports: reports_tx.clone(),
        starts: Arc::new(AtomicUsize::new(0)),
        child_stopped: None,
        mutate_children_on_start: false,
    });
    let runtime = Runtime::builder()
        .graph(graph.build().expect("graph builds"))
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();
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
    let leader = graph.actor("leader", {
        let starts = Arc::clone(&starts);
        move || ScopeProbe {
            reports: reports_tx.clone(),
            starts: Arc::clone(&starts),
            child_stopped: None,
            mutate_children_on_start: true,
        }
    });
    let graph = graph.build().expect("leader graph builds");
    let runtime = SupervisionTree::new()
        .actor_with_scope(
            "owned",
            graph.actors()[0].clone(),
            SupervisionTree::dynamic(),
        )
        .build()
        .expect("ActorWithScope builds");
    let handle = runtime.spawn();

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
async fn actor_with_ordered_scope_starts_after_leader_and_stops_before_it() {
    let (reports_tx, mut reports_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let child_stopped = Arc::new(AtomicBool::new(false));
    let mut leaders = GraphBuilder::new();
    leaders.actor("leader", {
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
    workers.actor("worker", {
        let child_stopped = Arc::clone(&child_stopped);
        move || StopProbe(Arc::clone(&child_stopped))
    });
    let workers = workers.build().expect("worker graph builds");
    let runtime = SupervisionTree::new()
        .actor_with_scope(
            "owned",
            leaders.actors()[0].clone(),
            SupervisionTree::graph(&workers),
        )
        .build()
        .expect("ordered ActorWithScope builds");
    assert_eq!(runtime.handle().snapshot().strategy, Strategy::OneForOne);
    let handle = runtime.spawn();
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

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut HandleContext<'_, Self::Msg>,
    ) -> ActorResult {
        if matches!(message, LeaderMsg::Crash) {
            panic!("scripted crash");
        }
        Ok(Stop)
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
async fn actor_with_scope_defaults_to_rest_for_one() {
    let leader_starts = Arc::new(AtomicUsize::new(0));
    let worker_starts = Arc::new(AtomicUsize::new(0));
    let mut leaders = GraphBuilder::new();
    let leader = leaders.actor("leader", {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let leaders = leaders.build().expect("leaders build");
    let mut workers = GraphBuilder::new();
    let worker = workers.actor("worker", {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let workers = workers.build().expect("workers build");
    let tree = SupervisionTree::new().actor_with_scope(
        "owned",
        leaders.actors()[0].clone(),
        SupervisionTree::graph(&workers),
    );
    let outline = tree.outline().expect("valid tree has an outline");
    assert!(matches!(
        outline.child("owned"),
        Some(tokio_otp::ChildOutline::ActorWithScope {
            strategy: Strategy::RestForOne,
            ..
        })
    ));
    let handle = tree.build().expect("tree builds").spawn();
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
    leaders.actor("leader", {
        let starts = Arc::clone(&leader_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let leaders = leaders.build().expect("leaders build");
    let mut workers = GraphBuilder::new();
    let worker = workers.actor("worker", {
        let starts = Arc::clone(&worker_starts);
        move || RestartProbe {
            starts: Arc::clone(&starts),
        }
    });
    let workers = workers.build().expect("workers build");
    let inner = SupervisionTree::graph(&workers)
        .restart_intensity(RestartIntensity::new(1, Duration::from_secs(30)));
    let handle = SupervisionTree::new()
        .actor_with_scope_strategy(
            "owned",
            leaders.actors()[0].clone(),
            inner,
            Strategy::OneForAll,
        )
        .build()
        .expect("tree builds")
        .spawn();
    handle.wait_started().await.expect("tree starts");

    worker.send(LeaderMsg::Crash).await.expect("first crash");
    wait_count(&worker_starts, 2).await;
    worker.send(LeaderMsg::Crash).await.expect("second crash");
    wait_count(&leader_starts, 2).await;

    handle.shutdown_and_wait().await.expect("tree stops");
}

#[tokio::test]
async fn cloning_a_declaration_reserves_a_fresh_identity_for_the_copy() {
    let builder = Runtime::builder().child(ChildSpec::new("task", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let reserved = builder.handle();
    let tree = builder.into_tree();

    // The clone carries the declaration but not the reservation, so building
    // and spawning it must not bind the handle taken from the original.
    let spawned = tree
        .clone()
        .build()
        .expect("cloned declaration builds")
        .spawn();
    spawned.wait_started().await.expect("the clone starts");
    assert!(matches!(
        reserved
            .supervisor_handle()
            .add_child(ChildSpec::new("late", |_| async { Ok(()) }))
            .await,
        Err(ControlError::Unavailable)
    ));

    // Dropping the original declaration abandons the reserved identity.
    drop(tree);
    assert_snapshot_stream_closes(&reserved).await;

    spawned.shutdown();
    spawned.wait().await.expect("the clone stops cleanly");
}
