use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorResult, DynamicActorOptions, DynamicTree, GraphBuilder, MessageContext,
    OrderedTree, RestartConfig, RestartPolicy, Runtime, RuntimeHandle, StartContext,
    observe::{ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleWatchGuard},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

enum SinkMsg {
    Lifecycle(ChildLifecycleEvent),
    Crash,
    Barrier(oneshot::Sender<()>),
}

struct Sink {
    generation: u64,
    observed: mpsc::UnboundedSender<(u64, ChildLifecycleEvent)>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(
        &mut self,
        message: SinkMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
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

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Err(io::Error::other("crash requested").into())
    }
}

enum RestrictedSinkMsg {
    Lifecycle(ChildLifecycleEvent),
}

struct RestrictedSink {
    observed: mpsc::UnboundedSender<ChildLifecycleEvent>,
    watch: Option<LifecycleWatchGuard>,
}

impl Actor for RestrictedSink {
    type Msg = RestrictedSinkMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.watch = Some(
            ctx.supervisor()
                .watch_lifecycle_to(&ctx.myself(), RestrictedSinkMsg::Lifecycle),
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let RestrictedSinkMsg::Lifecycle(event) = message;
        self.observed
            .send(event)
            .expect("restricted-scope observer remains live");
        Ok(())
    }
}

async fn runtime_with_watched_subtree() -> (
    Runtime,
    RuntimeHandle,
    kokage::ActorRef<SinkMsg>,
    kokage::ActorRef<()>,
    mpsc::UnboundedReceiver<(u64, ChildLifecycleEvent)>,
) {
    let handle = DynamicTree::new().spawn().expect("runtime builds");
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let sink_generation = Arc::new(AtomicU64::new(0));
    let sink = handle
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            move || {
                let generation = sink_generation.fetch_add(1, Ordering::SeqCst);
                Sink {
                    generation,
                    observed: observed_tx.clone(),
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("sink added");
    let mut graph = GraphBuilder::new();
    let (crasher_slot, crasher) = graph.slot("crasher");
    graph.define(crasher_slot, || Crasher);
    let watched = handle
        .dynamic()
        .expect("dynamic scope")
        .add_subtree(
            "watched",
            OrderedTree::graph(graph.build().expect("nested graph builds"))
                .default_restart(RestartPolicy::OnFailure)
                .restart_config(RestartConfig::new(8, Duration::from_secs(1))),
        )
        .await
        .expect("watched subtree added");
    timeout(Duration::from_secs(2), handle.wait_started())
        .await
        .expect("runtime startup timed out")
        .expect("runtime starts");
    (handle, watched, sink, crasher, observed_rx)
}

async fn recv_event(
    observed: &mut mpsc::UnboundedReceiver<(u64, ChildLifecycleEvent)>,
) -> (u64, ChildLifecycleEvent) {
    timeout(Duration::from_secs(2), observed.recv())
        .await
        .expect("lifecycle delivery timed out")
        .expect("observer remains live")
}

async fn wait_for_generation(handle: &RuntimeHandle, id: &str, generation: u64) {
    let mut snapshots = handle.subscribe_snapshots();
    timeout(Duration::from_secs(2), async {
        loop {
            if snapshots
                .borrow()
                .child(id)
                .is_some_and(|child| child.generation == generation && child.state.started())
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

async fn shutdown_runtime(handle: &RuntimeHandle, phase: &str) {
    timeout(Duration::from_secs(2), handle.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn crash_and_receive_events(
    crasher: &kokage::ActorRef<()>,
    observed: &mut mpsc::UnboundedReceiver<(u64, ChildLifecycleEvent)>,
) -> [(u64, ChildLifecycleEvent); 3] {
    crasher.send(()).await.expect("crash request delivered");
    [
        recv_event(observed).await,
        recv_event(observed).await,
        recv_event(observed).await,
    ]
}

async fn assert_no_buffered_lifecycle(
    sink: &kokage::ActorRef<SinkMsg>,
    observed: &mut mpsc::UnboundedReceiver<(u64, ChildLifecycleEvent)>,
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
async fn lifecycle_pump_forwards_ordered_events_and_never_replays_after_target_restart() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);

    let first = crash_and_receive_events(&crasher, &mut observed).await;
    assert!(matches!(
        &first[0].1.kind,
        ChildLifecycleEventKind::Exited { generation: 0, .. }
    ));
    assert!(matches!(
        &first[1].1.kind,
        ChildLifecycleEventKind::RestartScheduled { generation: 0, .. }
    ));
    assert!(matches!(
        &first[2].1.kind,
        ChildLifecycleEventKind::Started { generation: 1 }
    ));
    assert_eq!(first[1].1.seq, first[0].1.seq + 1);
    assert_eq!(first[2].1.seq, first[1].1.seq + 1);
    assert_eq!(first[0].0, 0);
    assert_eq!(first[1].0, 0);
    assert_eq!(first[2].0, 0);

    sink.send(SinkMsg::Crash)
        .await
        .expect("sink crash request delivered");
    wait_for_generation(&handle, "sink", 1).await;

    let second = crash_and_receive_events(&crasher, &mut observed).await;
    assert_eq!(second[0].0, 1);
    assert_eq!(second[1].0, 1);
    assert_eq!(second[2].0, 1);
    assert_eq!(second[0].1.seq, first[2].1.seq + 1);
    assert_eq!(second[1].1.seq, second[0].1.seq + 1);
    assert_eq!(second[2].1.seq, second[1].1.seq + 1);
    assert!(!guard.is_cancelled());
    assert_no_buffered_lifecycle(
        &sink,
        &mut observed,
        "fresh target replay check after a newer lifecycle pair",
    )
    .await;

    shutdown_runtime(&handle, "lifecycle replay test shutdown").await;
}

#[tokio::test]
async fn dropping_or_cancelling_lifecycle_guard_stops_delivery() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    guard.cancel();
    assert!(guard.is_cancelled());

    crasher.send(()).await.expect("crash request delivered");
    wait_for_generation(&watched, "crasher", 1).await;

    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    drop(guard);
    crasher
        .send(())
        .await
        .expect("second crash request delivered");
    wait_for_generation(&watched, "crasher", 2).await;

    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    let later = crash_and_receive_events(&crasher, &mut observed).await;
    assert!(matches!(
        &later[0].1.kind,
        ChildLifecycleEventKind::Exited { generation: 2, .. }
    ));
    assert!(matches!(
        &later[2].1.kind,
        ChildLifecycleEventKind::Started { generation: 3 }
    ));
    assert_no_buffered_lifecycle(
        &sink,
        &mut observed,
        "cancelled and dropped guard check after a later positive delivery",
    )
    .await;
    guard.cancel();

    shutdown_runtime(&handle, "lifecycle guard test shutdown").await;
}

#[tokio::test]
async fn lifecycle_pump_stops_on_watched_or_target_terminality() {
    let (handle, watched, sink, _crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    handle
        .dynamic()
        .expect("dynamic scope")
        .remove_child("watched")
        .await
        .expect("watched subtree removed");
    let (_, final_event) = recv_event(&mut observed).await;
    assert!(matches!(
        final_event.kind,
        ChildLifecycleEventKind::Exited { generation: 0, .. }
    ));
    timeout(Duration::from_secs(2), async {
        while !guard.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump stops with watched identity");

    let replacement = handle
        .dynamic()
        .expect("dynamic scope")
        .add_subtree("replacement", OrderedTree::new())
        .await
        .expect("replacement subtree added");
    let guard = replacement.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    handle
        .dynamic()
        .expect("dynamic scope")
        .remove_child("sink")
        .await
        .expect("target removed");
    timeout(Duration::from_secs(2), async {
        while !guard.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump stops with target identity");

    shutdown_runtime(&handle, "lifecycle terminality test shutdown").await;
}

#[tokio::test]
async fn restricted_scope_can_start_a_lifecycle_pump_from_on_start() {
    let handle = DynamicTree::new().spawn().expect("runtime builds");
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    handle
        .dynamic()
        .expect("dynamic scope")
        .add_actor("sink", move || RestrictedSink {
            observed: observed_tx.clone(),
            watch: None,
        })
        .await
        .expect("restricted sink added");
    let crasher = handle
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "crasher",
            || Crasher,
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("crasher added");
    handle.wait_started().await.expect("runtime starts");

    crasher.send(()).await.expect("crash delivered");
    let scheduled = timeout(Duration::from_secs(2), async {
        loop {
            let event = observed_rx.recv().await.expect("observer remains live");
            if matches!(
                &event.kind,
                ChildLifecycleEventKind::RestartScheduled { .. }
            ) {
                break event;
            }
        }
    })
    .await
    .expect("restricted-scope lifecycle event arrives");
    assert_eq!(scheduled.child_id, "crasher");

    shutdown_runtime(&handle, "restricted-scope lifecycle pump shutdown").await;
}
