use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_otp::{
    Actor, ActorResult, DynamicActorOptions, GraphBuilder, LifecycleEvent, LifecycleEventKind,
    MessageContext, RestartIntensity, RestartPolicy, Runtime, RuntimeHandle, prelude::Continue,
};

enum SinkMsg {
    Lifecycle(LifecycleEvent),
    Crash,
    Barrier(oneshot::Sender<()>),
}

struct Sink {
    generation: u64,
    observed: mpsc::UnboundedSender<(u64, LifecycleEvent)>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(
        &mut self,
        message: SinkMsg,
        _ctx: &mut MessageContext<'_, SinkMsg>,
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
        Ok(Continue)
    }
}

struct Crasher;

impl Actor for Crasher {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        Err(io::Error::other("crash requested").into())
    }
}

async fn runtime_with_watched_subtree() -> (
    RuntimeHandle,
    RuntimeHandle,
    tokio_otp::ActorRef<SinkMsg>,
    tokio_otp::ActorRef<()>,
    mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) {
    let handle = Runtime::dynamic().build().expect("runtime builds").spawn();
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let sink_generation = Arc::new(AtomicU64::new(0));
    let sink = handle
        .add_actor(
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
    let crasher = graph.actor("crasher", || Crasher);
    let watched = handle
        .add_subtree(
            "watched",
            Runtime::builder()
                .graph(graph.build().expect("nested graph builds"))
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(8, Duration::from_secs(1))),
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
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) -> (u64, LifecycleEvent) {
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
                .is_some_and(|child| child.generation == generation && child.started)
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

async fn crash_and_receive_pair(
    crasher: &tokio_otp::ActorRef<()>,
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) -> [(u64, LifecycleEvent); 2] {
    crasher.send(()).await.expect("crash request delivered");
    [recv_event(observed).await, recv_event(observed).await]
}

async fn assert_no_buffered_lifecycle(
    sink: &tokio_otp::ActorRef<SinkMsg>,
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
async fn lifecycle_pump_forwards_ordered_events_and_never_replays_after_target_restart() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);

    let first = crash_and_receive_pair(&crasher, &mut observed).await;
    assert!(matches!(
        first[0].1.kind,
        LifecycleEventKind::Exited { generation: 0, .. }
    ));
    assert!(matches!(
        first[1].1.kind,
        LifecycleEventKind::Started { generation: 1 }
    ));
    assert_eq!(first[1].1.seq, first[0].1.seq + 1);
    assert_eq!(first[0].0, 0);
    assert_eq!(first[1].0, 0);

    sink.send(SinkMsg::Crash)
        .await
        .expect("sink crash request delivered");
    wait_for_generation(&handle, "sink", 1).await;

    let second = crash_and_receive_pair(&crasher, &mut observed).await;
    assert_eq!(second[0].0, 1);
    assert_eq!(second[1].0, 1);
    assert_eq!(second[0].1.seq, first[1].1.seq + 1);
    assert_eq!(second[1].1.seq, second[0].1.seq + 1);
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
    let later = crash_and_receive_pair(&crasher, &mut observed).await;
    assert!(matches!(
        later[0].1.kind,
        LifecycleEventKind::Exited { generation: 2, .. }
    ));
    assert!(matches!(
        later[1].1.kind,
        LifecycleEventKind::Started { generation: 3 }
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
        .remove_child("watched")
        .await
        .expect("watched subtree removed");
    let (_, final_event) = recv_event(&mut observed).await;
    assert!(matches!(
        final_event.kind,
        LifecycleEventKind::Exited { generation: 0, .. }
    ));
    timeout(Duration::from_secs(2), async {
        while !guard.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump stops with watched identity");

    let replacement = handle
        .add_subtree("replacement", Runtime::builder())
        .await
        .expect("replacement subtree added");
    let guard = replacement.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    handle.remove_child("sink").await.expect("target removed");
    timeout(Duration::from_secs(2), async {
        while !guard.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump stops with target identity");

    shutdown_runtime(&handle, "lifecycle terminality test shutdown").await;
}
