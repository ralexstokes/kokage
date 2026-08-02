mod support;

use support::TreeBuilder;

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorSlot, ActorSpec, Context, DynamicScopeRef, DynamicTree, ExitResult, Guard,
    RestartPolicy, RunningTree, ScopeRef, Tree,
    observe::{ChildEventKind, ChildObservationUpdate, LifecycleEvent, LifecycleEventKind},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

enum SinkMsg {
    Lifecycle(LifecycleEvent),
    Crash,
    Barrier(oneshot::Sender<()>),
}

fn child_seq(event: &LifecycleEvent) -> u64 {
    event
        .kind
        .seq()
        .unwrap_or_else(|| panic!("expected child lifecycle event: {event:?}"))
}

fn child_kind(event: &LifecycleEvent) -> Option<&ChildEventKind> {
    let LifecycleEventKind::Child(child) = &event.kind else {
        return None;
    };
    Some(&child.kind)
}

struct Sink {
    generation: u64,
    observed: mpsc::UnboundedSender<(u64, LifecycleEvent)>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(&mut self, message: SinkMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SinkMsg::Lifecycle(event) => self
                .observed
                .send((self.generation, event))
                .expect("observer remains live"),
            SinkMsg::Crash => return Err(io::Error::other("sink crash requested").into()),
            SinkMsg::Barrier(reply) => {
                let _ = reply.send(());
            }
        }
        Ok(())
    }
}

struct Crasher;

impl Actor for Crasher {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Err(io::Error::other("crash requested").into())
    }
}

enum ScopeSinkMsg {
    Lifecycle(LifecycleEvent),
}

struct ScopeSink {
    observed: mpsc::UnboundedSender<LifecycleEvent>,
    watch: Option<Guard>,
}

enum ObservationSinkMsg {
    Update(ChildObservationUpdate),
    Crash,
}

struct ObservationSink {
    generation: u64,
    observed: mpsc::UnboundedSender<(u64, ChildObservationUpdate)>,
}

impl Actor for ObservationSink {
    type Msg = ObservationSinkMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ObservationSinkMsg::Update(update) => self
                .observed
                .send((self.generation, update))
                .expect("observation receiver remains live"),
            ObservationSinkMsg::Crash => {
                return Err(io::Error::other("observation sink crash requested").into());
            }
        }
        Ok(())
    }
}

impl Actor for ScopeSink {
    type Msg = ScopeSinkMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.watch = Some(
            ctx.scope()
                .subscribe_lifecycle()
                .direct_children()
                .forward_to(&ctx.myself(), ScopeSinkMsg::Lifecycle),
        );
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let ScopeSinkMsg::Lifecycle(event) = message;
        self.observed
            .send(event)
            .expect("scope observer remains live");
        Ok(())
    }
}

async fn runtime_with_watched_subtree() -> (
    RunningTree<DynamicScopeRef>,
    ScopeRef,
    kokage::ActorRef<SinkMsg>,
    kokage::ActorRef<()>,
    mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) {
    let running_tree = DynamicTree::new().spawn().expect("runtime builds");
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let sink_generation = Arc::new(AtomicU64::new(0));
    let sink = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("sink", move || {
                let generation = sink_generation.fetch_add(1, Ordering::SeqCst);
                Sink {
                    generation,
                    observed: observed_tx.clone(),
                }
            })
            .restart(RestartPolicy::on_failure()),
        )
        .await
        .expect("sink added");
    let mut graph = TreeBuilder::new();
    let crasher_slot = ActorSlot::new("crasher");
    let crasher = crasher_slot.actor_ref();
    graph.define(crasher_slot, || Crasher);
    let watched = support::dynamic_root(&running_tree)
        .add_subtree(
            "watched",
            graph
                .build()
                .default_restart(RestartPolicy::on_failure().limit(8, Duration::from_secs(1))),
        )
        .await
        .expect("watched subtree added");
    timeout(Duration::from_secs(2), running_tree.scope().wait_started())
        .await
        .expect("runtime startup timed out")
        .expect("runtime starts");
    (running_tree, watched, sink, crasher, observed_rx)
}

async fn recv_event(
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) -> (u64, LifecycleEvent) {
    timeout(Duration::from_secs(2), observed.recv())
        .await
        .expect("lifecycle delivery timed out")
        .expect("observer remains live")
}

async fn wait_for_generation(handle: &ScopeRef, id: &str, generation: u64) {
    let mut snapshots = handle.subscribe_snapshots();
    timeout(Duration::from_secs(2), async {
        loop {
            if snapshots
                .latest()
                .child(id)
                .is_some_and(|child| child.generation == generation && child.state.is_running())
            {
                break;
            }
            snapshots
                .changed()
                .await
                .expect("snapshot stream remains open");
        }
    })
    .await
    .expect("child reaches expected generation");
}

async fn shutdown_runtime(handle: &ScopeRef, phase: &str) {
    timeout(Duration::from_secs(2), handle.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn crash_and_receive_events(
    crasher: &kokage::ActorRef<()>,
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) -> [(u64, LifecycleEvent); 3] {
    crasher.send(()).await.expect("crash request delivered");
    [
        recv_event(observed).await,
        recv_event(observed).await,
        recv_event(observed).await,
    ]
}

async fn assert_no_buffered_lifecycle(
    sink: &kokage::ActorRef<SinkMsg>,
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
    phase: &str,
) {
    let (barrier_tx, barrier_rx) = oneshot::channel();
    timeout(
        Duration::from_secs(2),
        sink.send(SinkMsg::Barrier(barrier_tx)),
    )
    .await
    .expect("timed out sending lifecycle barrier")
    .expect("sink accepts lifecycle barrier");
    timeout(Duration::from_secs(2), barrier_rx)
        .await
        .expect("sink lifecycle barrier timed out")
        .expect("sink keeps lifecycle barrier sender alive");
    match observed.try_recv() {
        Err(mpsc::error::TryRecvError::Empty) => {}
        Err(mpsc::error::TryRecvError::Disconnected) => {
            panic!("lifecycle observer disconnected during {phase}")
        }
        Ok((generation, event)) => {
            panic!(
                "unexpected lifecycle event during {phase}: sink generation {generation}, {event:?}"
            )
        }
    }
}

#[tokio::test]
async fn retained_lifecycle_pump_forwards_events_without_replay_after_target_restart() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);

    let first = crash_and_receive_events(&crasher, &mut observed).await;
    assert!(matches!(
        child_kind(&first[0].1),
        Some(ChildEventKind::Exited { generation: 0, .. })
    ));
    assert!(matches!(
        child_kind(&first[1].1),
        Some(ChildEventKind::RestartScheduled { generation: 0, .. })
    ));
    assert!(matches!(
        child_kind(&first[2].1),
        Some(ChildEventKind::Started { generation: 1 })
    ));
    assert_eq!(child_seq(&first[1].1), child_seq(&first[0].1) + 1);
    assert_eq!(child_seq(&first[2].1), child_seq(&first[1].1) + 1);
    assert_eq!(first[0].0, 0);
    assert_eq!(first[1].0, 0);
    assert_eq!(first[2].0, 0);

    sink.send(SinkMsg::Crash)
        .await
        .expect("sink crash request delivered");
    wait_for_generation(&handle.scope(), "sink", 1).await;

    let second = crash_and_receive_events(&crasher, &mut observed).await;
    assert_eq!(second[0].0, 1);
    assert_eq!(second[1].0, 1);
    assert_eq!(second[2].0, 1);
    assert_eq!(child_seq(&second[0].1), child_seq(&first[2].1) + 1);
    assert_eq!(child_seq(&second[1].1), child_seq(&second[0].1) + 1);
    assert_eq!(child_seq(&second[2].1), child_seq(&second[1].1) + 1);
    assert!(!guard.is_cancelled());
    assert!(!guard.is_finished());
    guard.detach();
    assert_no_buffered_lifecycle(
        &sink,
        &mut observed,
        "fresh target replay check after a newer lifecycle pair",
    )
    .await;

    shutdown_runtime(&handle.scope(), "lifecycle replay test shutdown").await;
}

#[tokio::test]
async fn dropping_or_cancelling_lifecycle_guard_stops_delivery() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);
    guard.cancel();
    assert!(guard.is_cancelled());

    crasher.send(()).await.expect("crash request delivered");
    wait_for_generation(&watched, "crasher", 1).await;

    let guard = watched
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);
    drop(guard);
    crasher
        .send(())
        .await
        .expect("second crash request delivered");
    wait_for_generation(&watched, "crasher", 2).await;

    let guard = watched
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);
    let later = crash_and_receive_events(&crasher, &mut observed).await;
    assert!(matches!(
        child_kind(&later[0].1),
        Some(ChildEventKind::Exited { generation: 2, .. })
    ));
    assert!(matches!(
        child_kind(&later[2].1),
        Some(ChildEventKind::Started { generation: 3 })
    ));
    assert_no_buffered_lifecycle(
        &sink,
        &mut observed,
        "cancelled and dropped guard check after a later positive delivery",
    )
    .await;
    guard.cancel();

    shutdown_runtime(&handle.scope(), "lifecycle guard test shutdown").await;
}

#[tokio::test]
async fn lifecycle_pump_stops_on_watched_or_target_terminality() {
    let (handle, watched, sink, _crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);
    handle
        .scope()
        .remove_named("watched")
        .await
        .expect("watched subtree removed");
    let (_, final_event) = recv_event(&mut observed).await;
    assert!(matches!(
        child_kind(&final_event),
        Some(ChildEventKind::Exited { generation: 0, .. })
    ));
    timeout(Duration::from_secs(2), guard.finished())
        .await
        .expect("pump stops with watched identity");
    assert!(
        !guard.is_cancelled(),
        "normal completion is not cancellation"
    );

    let replacement = handle
        .scope()
        .add_subtree("replacement", Tree::new())
        .await
        .expect("replacement subtree added");
    let guard = replacement
        .subscribe_lifecycle()
        .direct_children()
        .forward_to(&sink, SinkMsg::Lifecycle);
    handle
        .scope()
        .remove_named("sink")
        .await
        .expect("target removed");
    timeout(Duration::from_secs(2), guard.finished())
        .await
        .expect("pump stops with target identity");
    assert!(
        !guard.is_cancelled(),
        "normal completion is not cancellation"
    );

    shutdown_runtime(&handle.scope(), "lifecycle terminality test shutdown").await;
}

#[tokio::test]
async fn context_scope_can_start_a_lifecycle_pump_from_on_start() {
    let running_tree = DynamicTree::new().spawn().expect("runtime builds");
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("sink", move || ScopeSink {
            observed: observed_tx.clone(),
            watch: None,
        }))
        .await
        .expect("scope sink added");
    let crasher = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("crasher", || Crasher).restart(RestartPolicy::on_failure()))
        .await
        .expect("crasher added");
    running_tree
        .scope()
        .wait_started()
        .await
        .expect("runtime starts");

    crasher.send(()).await.expect("crash delivered");
    let scheduled = timeout(Duration::from_secs(2), async {
        loop {
            let event = observed_rx.recv().await.expect("observer remains live");
            if matches!(
                child_kind(&event),
                Some(ChildEventKind::RestartScheduled { .. })
            ) {
                break event;
            }
        }
    })
    .await
    .expect("context-scope lifecycle event arrives");
    assert!(matches!(
        child_kind(&scheduled),
        Some(ChildEventKind::RestartScheduled { .. })
    ));
    assert!(matches!(
        &scheduled.kind,
        LifecycleEventKind::Child(child)
            if child.child_id == "crasher"
                && matches!(child.kind, ChildEventKind::RestartScheduled { .. })
    ));

    let handle = running_tree.scope();
    shutdown_runtime(&handle, "context-scope lifecycle pump shutdown").await;
}

#[tokio::test]
async fn child_observation_pump_forwards_resets_and_post_reset_transitions() {
    let running_tree = DynamicTree::new().spawn().expect("runtime builds");
    let root = support::dynamic_root(&running_tree);
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let sink = root
        .add_actor("observation-sink", move || ObservationSink {
            generation: 0,
            observed: observed_tx.clone(),
        })
        .await
        .expect("observation sink added");
    running_tree
        .scope()
        .wait_started()
        .await
        .expect("observation sink starts");
    let observation = running_tree.scope().observe_children();

    for index in 0..70 {
        let task = root
            .add_task(format!("overflow-{index}"), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .await
            .expect("overflow task added");
        root.remove(&task).await.expect("overflow task removed");
    }

    let guard = observation
        .events
        .forward_to(&sink, ObservationSinkMsg::Update);
    let (generation, reset) = timeout(Duration::from_secs(2), observed_rx.recv())
        .await
        .expect("recovery reset is forwarded")
        .expect("observation receiver remains live");
    assert_eq!(generation, 0);
    let ChildObservationUpdate::Reset { snapshot, dropped } = reset else {
        panic!("the first forwarded update after overflow must be a reset");
    };
    assert!(dropped > 0);
    assert!(snapshot.child("observation-sink").is_some());
    assert_eq!(snapshot.children, running_tree.scope().snapshot().children);
    let reset_sequence = snapshot.lifecycle_seq;

    let fresh = root
        .add_task("after-reset", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .await
        .expect("post-reset task added");
    let (generation, transition) = timeout(Duration::from_secs(2), observed_rx.recv())
        .await
        .expect("post-reset transition is forwarded")
        .expect("observation receiver remains live");
    assert_eq!(generation, 0);
    let ChildObservationUpdate::Transition(transition) = transition else {
        panic!("post-reset child change must be a transition");
    };
    assert_eq!(transition.child_id, "after-reset");
    assert!(transition.seq > reset_sequence);

    root.remove(&fresh).await.expect("post-reset task removed");
    guard.cancel();
    shutdown_runtime(&running_tree.scope(), "child observation pump shutdown").await;
}

#[tokio::test]
async fn child_observation_pump_resets_a_restarted_target_incarnation() {
    let running_tree = DynamicTree::new().spawn().expect("runtime builds");
    let root = support::dynamic_root(&running_tree);
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let sink_generation = Arc::new(AtomicU64::new(0));
    let sink = root
        .add_actor_spec(
            ActorSpec::new("observation-sink", move || ObservationSink {
                generation: sink_generation.fetch_add(1, Ordering::SeqCst),
                observed: observed_tx.clone(),
            })
            .restart(RestartPolicy::on_failure()),
        )
        .await
        .expect("observation sink added");
    running_tree
        .scope()
        .wait_started()
        .await
        .expect("observation sink starts");
    let guard = running_tree
        .scope()
        .observe_children()
        .events
        .forward_to(&sink, ObservationSinkMsg::Update);

    let before_restart = root
        .add_task("before-target-restart", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .await
        .expect("pre-restart task added");
    let (generation, initial) = timeout(Duration::from_secs(2), observed_rx.recv())
        .await
        .expect("initial transition is forwarded")
        .expect("observation receiver remains live");
    assert_eq!(generation, 0);
    assert!(matches!(
        initial,
        ChildObservationUpdate::Transition(ref child)
            if child.child_id == "before-target-restart"
    ));

    sink.send(ObservationSinkMsg::Crash)
        .await
        .expect("observation sink crash delivered");
    wait_for_generation(&running_tree.scope(), "observation-sink", 1).await;

    let reset = timeout(Duration::from_secs(2), async {
        loop {
            let (generation, update) = observed_rx.recv().await.expect("observer remains live");
            if generation == 1
                && let ChildObservationUpdate::Reset { snapshot, dropped } = update
                && dropped == 0
            {
                break snapshot;
            }
        }
    })
    .await
    .expect("fresh target incarnation receives a reset");
    assert!(
        reset
            .child("observation-sink")
            .is_some_and(|child| child.generation == 1)
    );

    let after_restart = root
        .add_task("after-target-restart", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .await
        .expect("post-restart task added");
    timeout(Duration::from_secs(2), async {
        loop {
            let (generation, update) = observed_rx.recv().await.expect("observer remains live");
            if generation == 1
                && matches!(
                    update,
                    ChildObservationUpdate::Transition(ref child)
                        if child.child_id == "after-target-restart"
                )
            {
                break;
            }
        }
    })
    .await
    .expect("post-reset transition reaches fresh target incarnation");

    root.remove(&before_restart)
        .await
        .expect("pre-restart task removed");
    root.remove(&after_restart)
        .await
        .expect("post-restart task removed");
    guard.cancel();
    shutdown_runtime(
        &running_tree.scope(),
        "restarted child observation target shutdown",
    )
    .await;
}
