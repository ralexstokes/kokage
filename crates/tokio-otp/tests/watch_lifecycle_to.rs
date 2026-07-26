use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::{
    Actor, ActorContext, ActorResult, DynamicActorOptions, GraphBuilder, LifecycleEvent,
    LifecycleEventKind, RestartPolicy, Runtime, RuntimeHandle, prelude::Continue,
};

enum SinkMsg {
    Lifecycle(LifecycleEvent),
    Crash,
}

struct Sink {
    generation: u64,
    observed: mpsc::UnboundedSender<(u64, LifecycleEvent)>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(&mut self, message: SinkMsg, _ctx: &mut ActorContext<SinkMsg>) -> ActorResult {
        match message {
            SinkMsg::Lifecycle(event) => self
                .observed
                .send((self.generation, event))
                .expect("observer remains live"),
            SinkMsg::Crash => return Err(io::Error::other("sink crash requested").into()),
        }
        Ok(Continue)
    }
}

struct Crasher;

impl Actor for Crasher {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut ActorContext<()>) -> ActorResult {
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
    let handle = Runtime::builder().build().expect("runtime builds").spawn();
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
                .restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("watched subtree added");
    handle.wait_started().await.expect("runtime starts");
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

async fn crash_and_receive_pair(
    crasher: &tokio_otp::ActorRef<()>,
    observed: &mut mpsc::UnboundedReceiver<(u64, LifecycleEvent)>,
) -> [(u64, LifecycleEvent); 2] {
    crasher.send(()).await.expect("crash request delivered");
    [recv_event(observed).await, recv_event(observed).await]
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
    assert!(
        timeout(Duration::from_millis(150), observed.recv())
            .await
            .is_err(),
        "fresh target incarnation received replayed lifecycle history"
    );

    let second = crash_and_receive_pair(&crasher, &mut observed).await;
    assert_eq!(second[0].0, 1);
    assert_eq!(second[1].0, 1);
    assert_eq!(second[0].1.seq, first[1].1.seq + 1);
    assert_eq!(second[1].1.seq, second[0].1.seq + 1);
    assert!(!guard.is_cancelled());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn dropping_or_cancelling_lifecycle_guard_stops_delivery() {
    let (handle, watched, sink, crasher, mut observed) = runtime_with_watched_subtree().await;
    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    guard.cancel();
    assert!(guard.is_cancelled());

    crasher.send(()).await.expect("crash request delivered");
    assert!(
        timeout(Duration::from_millis(150), observed.recv())
            .await
            .is_err(),
        "cancelled lifecycle guard delivered an event"
    );

    let guard = watched.watch_lifecycle_to(&sink, SinkMsg::Lifecycle);
    drop(guard);
    crasher
        .send(())
        .await
        .expect("second crash request delivered");
    assert!(
        timeout(Duration::from_millis(150), observed.recv())
            .await
            .is_err(),
        "dropped lifecycle guard delivered an event"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
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

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
