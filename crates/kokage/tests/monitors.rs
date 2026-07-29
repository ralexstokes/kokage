use std::{future::pending, sync::Arc, time::Duration};

use kokage::{
    ActorContext, ActorFactory, ActorRef, ActorResult, CancellationHandle, Down, DownReason,
    DynamicActorOptions, DynamicTree, GraphBuilder, MonitorEvent, RestartPolicy,
    host::{DEFAULT_SHUTDOWN_BOUND, RawActor, RunnableActor},
};
use kokage_supervisor::ShutdownPolicy;
use tokio::{
    sync::{Notify, mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum PeerMessage {
    Stop,
    Panic,
}

#[derive(Clone)]
struct Peer {
    started: mpsc::UnboundedSender<()>,
}

impl RawActor for Peer {
    type Msg = PeerMessage;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        self.started.send(()).expect("start receiver alive");
        match ctx.recv().await {
            Some(PeerMessage::Stop) | None => Ok(()),
            Some(PeerMessage::Panic) => panic!("deliberate peer panic"),
        }
    }
}

enum ObserverMessage {
    Event(MonitorEvent),
    Crash,
    Barrier(oneshot::Sender<()>),
}

#[derive(Clone)]
struct Observer {
    peer: ActorRef<PeerMessage>,
    observed: mpsc::UnboundedSender<MonitorEvent>,
    started: mpsc::UnboundedSender<()>,
    cancel_watch: bool,
}

impl RawActor for Observer {
    type Msg = ObserverMessage;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let watch = ctx.watch(&self.peer, ObserverMessage::Event);
        if self.cancel_watch {
            watch.cancel();
        }
        self.started.send(()).expect("start receiver alive");

        while let Some(message) = ctx.recv().await {
            match message {
                ObserverMessage::Event(event) => {
                    self.observed.send(event).expect("observer receiver alive");
                }
                ObserverMessage::Crash => panic!("deliberate observer panic"),
                ObserverMessage::Barrier(reply) => {
                    let _ = reply.send(());
                }
            }
        }
        Ok(())
    }
}

struct Fixture {
    peer: RunnableActor,
    peer_ref: ActorRef<PeerMessage>,
    peer_started: mpsc::UnboundedReceiver<()>,
    observer: RunnableActor,
    observer_ref: ActorRef<ObserverMessage>,
    observer_started: mpsc::UnboundedReceiver<()>,
    observed: mpsc::UnboundedReceiver<MonitorEvent>,
}

fn runnable_actor<F>(
    label: &str,
    factory: F,
) -> (RunnableActor, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = GraphBuilder::new();
    let (slot, actor_ref) = builder.slot(label);
    builder.define(slot, factory);
    let graph = builder.build().expect("test graph builds");
    let actor = graph
        .actor_for(&actor_ref)
        .expect("test actor is registered");
    (actor, actor_ref)
}

fn fixture(cancel_watch: bool) -> Fixture {
    let (peer_started_tx, peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (observed_tx, observed) = mpsc::unbounded_channel();
    let (observer_started_tx, observer_started) = mpsc::unbounded_channel();
    let (observer, observer_ref) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        move || Observer {
            peer: peer_ref.clone(),
            observed: observed_tx.clone(),
            started: observer_started_tx.clone(),
            cancel_watch,
        }
    });
    Fixture {
        peer,
        peer_ref,
        peer_started,
        observer,
        observer_ref,
        observer_started,
        observed,
    }
}

async fn started(receiver: &mut mpsc::UnboundedReceiver<()>) {
    timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("actor started promptly")
        .expect("start sender alive");
}

async fn next_event(receiver: &mut mpsc::UnboundedReceiver<MonitorEvent>) -> MonitorEvent {
    timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("event delivered promptly")
        .expect("observer sender alive")
}

async fn recv_test_event<T>(receiver: &mut mpsc::UnboundedReceiver<T>, phase: &str) -> T {
    timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|| panic!("channel closed while waiting for {phase}"))
}

async fn assert_silence(
    observer: &ActorRef<ObserverMessage>,
    receiver: &mut mpsc::UnboundedReceiver<MonitorEvent>,
) {
    let (barrier_tx, barrier_rx) = oneshot::channel();
    observer
        .send(ObserverMessage::Barrier(barrier_tx))
        .await
        .expect("observer accepts the silence barrier");
    timeout(Duration::from_secs(1), barrier_rx)
        .await
        .expect("observer processes the silence barrier promptly")
        .expect("observer keeps the silence barrier sender alive");
    match receiver.try_recv() {
        Err(mpsc::error::TryRecvError::Empty) => {}
        Err(mpsc::error::TryRecvError::Disconnected) => {
            panic!("observer channel closed before silence could be checked")
        }
        Ok(event) => panic!("unexpected event after observer barrier: {event:?}"),
    }
}

async fn watch_cancelled(watch: &CancellationHandle) {
    timeout(Duration::from_secs(1), async {
        while !watch.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watch cancelled promptly");
}

fn up(actor_id: &str, generation: u64) -> MonitorEvent {
    MonitorEvent::Up {
        actor_id: actor_id.to_owned(),
        generation,
    }
}

fn expect_down(event: MonitorEvent) -> Down {
    match event {
        MonitorEvent::Down(down) => down,
        other => panic!("expected Down, got {other:?}"),
    }
}

fn expect_terminated(event: MonitorEvent, actor_id: &str) -> Option<u64> {
    match event {
        MonitorEvent::Terminated {
            actor_id: id,
            generation,
            ..
        } => {
            assert_eq!(id, actor_id);
            generation
        }
        other => panic!("expected Terminated, got {other:?}"),
    }
}

#[tokio::test]
async fn watch_reports_panicked_peer_as_failure() {
    let mut fixture = fixture(false);
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_stop = CancellationToken::new();
    let observer = fixture.observer.clone();
    let stop = observer_stop.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                stop.cancelled(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));

    fixture
        .peer_ref
        .send(PeerMessage::Panic)
        .await
        .expect("panic command sent");
    let notification = expect_down(next_event(&mut fixture.observed).await);
    assert_eq!(notification.actor_id, "peer");
    assert_eq!(notification.generation, 0);
    assert_eq!(notification.reason, DownReason::Failure);
    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        Some(0),
        "RestartPolicy::Never terminates the binding after the failed run"
    );

    assert!(peer_task.await.expect_err("peer task panicked").is_panic());
    observer_stop.cancel();
    observer_task
        .await
        .expect("observer task joined")
        .expect("observer stopped cleanly");
}

#[tokio::test]
async fn watch_reports_clean_stop_as_normal() {
    let mut fixture = fixture(false);
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_stop = CancellationToken::new();
    let stop = observer_stop.clone();
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                stop.cancelled(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));

    fixture
        .peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    assert_eq!(
        expect_down(next_event(&mut fixture.observed).await).reason,
        DownReason::Normal
    );
    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        Some(0)
    );
    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");

    observer_stop.cancel();
    observer_task
        .await
        .expect("observer task joined")
        .expect("observer stopped cleanly");
}

#[tokio::test]
async fn cancelled_watch_suppresses_delivery() {
    let mut fixture = fixture(true);
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;

    fixture
        .peer_ref
        .send(PeerMessage::Panic)
        .await
        .expect("panic command sent");
    assert!(peer_task.await.expect_err("peer task panicked").is_panic());
    assert_silence(&fixture.observer_ref, &mut fixture.observed).await;
    observer_task.abort();
}

#[tokio::test]
async fn watch_survives_observer_restart_without_duplicate_registration() {
    let mut fixture = fixture(false);
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    started(&mut fixture.peer_started).await;

    let first_observer = fixture.observer.clone();
    let first_task = tokio::spawn(async move {
        first_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::OnFailure,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));
    fixture
        .observer_ref
        .send(ObserverMessage::Crash)
        .await
        .expect("crash command sent");
    assert!(
        first_task
            .await
            .expect_err("observer task panicked")
            .is_panic()
    );

    // Exit the subject while the observer has no bound incarnation. The
    // membership-owned forwarder must wait for the replacement mailbox.
    fixture
        .peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");

    let second_observer = fixture.observer.clone();
    let second_task = tokio::spawn(async move {
        second_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_eq!(
        expect_down(next_event(&mut fixture.observed).await).reason,
        DownReason::Normal
    );
    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        Some(0)
    );
    assert_silence(&fixture.observer_ref, &mut fixture.observed).await;

    second_task.abort();
}

enum TaggedObserverMessage {
    Event {
        registration: usize,
        event: MonitorEvent,
    },
    Crash,
    Barrier(oneshot::Sender<()>),
}

#[derive(Clone)]
struct TaggedObserver {
    peer: ActorRef<PeerMessage>,
    registrations: Arc<std::sync::atomic::AtomicUsize>,
    started: mpsc::UnboundedSender<()>,
    observed: mpsc::UnboundedSender<(usize, MonitorEvent)>,
}

impl RawActor for TaggedObserver {
    type Msg = TaggedObserverMessage;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let registration = self
            .registrations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ctx.watch(&self.peer, move |event| TaggedObserverMessage::Event {
            registration,
            event,
        });
        self.started.send(()).expect("start receiver alive");
        while let Some(message) = ctx.recv().await {
            match message {
                TaggedObserverMessage::Event {
                    registration,
                    event,
                } => self
                    .observed
                    .send((registration, event))
                    .expect("event receiver alive"),
                TaggedObserverMessage::Crash => panic!("deliberate observer panic"),
                TaggedObserverMessage::Barrier(reply) => {
                    let _ = reply.send(());
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn replacement_incarnation_keeps_the_membership_owned_mapper() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let registrations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        let registrations = registrations.clone();
        move || TaggedObserver {
            peer: peer_ref.clone(),
            registrations: registrations.clone(),
            started: observer_started_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let first_observer = observer.clone();
    let first_task = tokio::spawn(async move {
        first_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::OnFailure,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut peer_started).await;
    started(&mut observer_started).await;
    let (registration, event) = recv_test_event(&mut observed, "initial tagged up event").await;
    assert_eq!(registration, 0);
    assert_eq!(event, up("peer", 0));

    observer_ref
        .send(TaggedObserverMessage::Crash)
        .await
        .expect("observer crash sent");
    assert!(
        first_task
            .await
            .expect_err("observer task panicked")
            .is_panic()
    );
    let second_observer = observer.clone();
    let second_task = tokio::spawn(async move {
        second_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut observer_started).await;
    assert_eq!(
        registrations.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "both incarnations attempted registration"
    );

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("peer stop sent");
    let (registration, event) = recv_test_event(&mut observed, "tagged down event").await;
    assert_eq!(
        registration, 0,
        "replacement mapper did not replace the watch"
    );
    assert_eq!(expect_down(event).reason, DownReason::Normal);
    let (registration, event) = recv_test_event(&mut observed, "tagged terminal event").await;
    assert_eq!(registration, 0);
    assert_eq!(expect_terminated(event, "peer"), Some(0));
    let (barrier_tx, barrier_rx) = oneshot::channel();
    observer_ref
        .send(TaggedObserverMessage::Barrier(barrier_tx))
        .await
        .expect("replacement observer accepts the duplicate-check barrier");
    timeout(Duration::from_secs(1), barrier_rx)
        .await
        .expect("replacement observer processes the duplicate-check barrier")
        .expect("replacement observer keeps the barrier sender alive");
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    second_task.abort();
}

enum AliasedObserverMessage {
    Event {
        registration: usize,
        event: MonitorEvent,
    },
    Rewatch,
}

#[derive(Clone)]
struct AliasedObserver {
    peer: ActorRef<PeerMessage>,
    watches: mpsc::UnboundedSender<CancellationHandle>,
    started: mpsc::UnboundedSender<()>,
    observed: mpsc::UnboundedSender<(usize, MonitorEvent)>,
}

impl RawActor for AliasedObserver {
    type Msg = AliasedObserverMessage;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        for registration in 0..2 {
            let watch = ctx.watch(&self.peer, move |event| AliasedObserverMessage::Event {
                registration,
                event,
            });
            self.watches.send(watch).expect("watch receiver alive");
        }
        self.started.send(()).expect("start receiver alive");

        while let Some(message) = ctx.recv().await {
            match message {
                AliasedObserverMessage::Event {
                    registration,
                    event,
                } => self
                    .observed
                    .send((registration, event))
                    .expect("event receiver alive"),
                AliasedObserverMessage::Rewatch => {
                    let watch = ctx.watch(&self.peer, |event| AliasedObserverMessage::Event {
                        registration: 2,
                        event,
                    });
                    self.watches.send(watch).expect("watch receiver alive");
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn repeated_watch_calls_alias_until_cancelled() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        move || AliasedObserver {
            peer: peer_ref.clone(),
            watches: watch_tx.clone(),
            started: observer_started_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    started(&mut peer_started).await;
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut observer_started).await;
    let first = recv_test_event(&mut watch_rx, "first aliased watch handle").await;
    let second = recv_test_event(&mut watch_rx, "second aliased watch handle").await;

    let (registration, event) = recv_test_event(&mut observed, "initial aliased up event").await;
    assert_eq!(registration, 0, "the first mapper owns the watch");
    assert_eq!(event, up("peer", 0));

    second.cancel();
    watch_cancelled(&first).await;
    assert!(second.is_cancelled(), "both handles alias one watch");

    observer_ref
        .send(AliasedObserverMessage::Rewatch)
        .await
        .expect("rewatch command sent");
    let fresh = recv_test_event(&mut watch_rx, "replacement watch handle").await;
    assert!(!fresh.is_cancelled());
    let (registration, event) = recv_test_event(&mut observed, "replacement up event").await;
    assert_eq!(registration, 2, "the fresh mapper owns the new watch");
    assert_eq!(event, up("peer", 0));

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("peer stop sent");
    let (registration, event) = recv_test_event(&mut observed, "replacement down event").await;
    assert_eq!(registration, 2);
    assert_eq!(expect_down(event).reason, DownReason::Normal);
    let (registration, event) = recv_test_event(&mut observed, "replacement terminal event").await;
    assert_eq!(registration, 2);
    assert_eq!(expect_terminated(event, "peer"), Some(0));
    watch_cancelled(&fresh).await;

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    observer_task.abort();
}

enum ManagedObserverMessage {
    Event(MonitorEvent),
    Stop,
}

#[derive(Clone)]
struct ManagedObserver {
    peer: ActorRef<PeerMessage>,
    watch: mpsc::UnboundedSender<CancellationHandle>,
    observed: mpsc::UnboundedSender<MonitorEvent>,
}

impl RawActor for ManagedObserver {
    type Msg = ManagedObserverMessage;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let watch = ctx.watch(&self.peer, ManagedObserverMessage::Event);
        self.watch.send(watch).expect("watch receiver alive");
        while let Some(message) = ctx.recv().await {
            match message {
                ManagedObserverMessage::Event(event) => {
                    self.observed.send(event).expect("event receiver alive");
                }
                ManagedObserverMessage::Stop => break,
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn observer_membership_removal_cancels_its_watches() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, _observed_rx) = mpsc::unbounded_channel();
    let (observer, observer_ref) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        move || ManagedObserver {
            peer: peer_ref.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut peer_started).await;
    let watch = recv_test_event(&mut watch_rx, "managed watch handle").await;

    observer_ref
        .send(ManagedObserverMessage::Stop)
        .await
        .expect("observer stop sent");
    observer_task
        .await
        .expect("observer task joined")
        .expect("observer stopped cleanly");
    assert!(
        watch.is_cancelled(),
        "terminating the observer membership ends its outbound watch"
    );

    peer_task.abort();
}

#[tokio::test]
async fn subject_membership_removal_delivers_terminal_then_ends_watch() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, _) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        move || ManagedObserver {
            peer: peer_ref.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut peer_started).await;
    let watch = recv_test_event(&mut watch_rx, "subject-removal watch handle").await;
    assert_eq!(next_event(&mut observed).await, up("peer", 0));

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("peer stop sent");
    assert_eq!(
        expect_down(next_event(&mut observed).await).reason,
        DownReason::Normal
    );
    assert_eq!(
        expect_terminated(next_event(&mut observed).await, "peer"),
        Some(0)
    );
    watch_cancelled(&watch).await;

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    observer_task.abort();
}

#[tokio::test]
async fn watching_terminated_peer_delivers_immediate_terminated() {
    let mut fixture = fixture(false);
    fixture.peer.terminate_binding();
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.observer_started).await;

    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        None,
        "a never-started terminated target has no last generation"
    );
    assert_silence(&fixture.observer_ref, &mut fixture.observed).await;
    observer_task.abort();
}

#[tokio::test]
async fn watching_detached_peer_delivers_immediate_terminated() {
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (started_tx, mut observer_started) = mpsc::unbounded_channel();
    let detached_peer = {
        let (_, peer_ref) = runnable_actor("detached-peer", || Peer {
            started: mpsc::unbounded_channel().0,
        });
        peer_ref
    };
    let (observer, _) = runnable_actor("observer", move || Observer {
        peer: detached_peer.clone(),
        observed: observed_tx.clone(),
        started: started_tx.clone(),
        cancel_watch: false,
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut observer_started).await;

    assert_eq!(
        expect_terminated(next_event(&mut observed).await, "detached-peer"),
        None
    );
    observer_task.abort();
}

#[tokio::test]
async fn watch_survives_peer_restart_without_reregistration() {
    let mut fixture = fixture(false);
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    let first_peer = fixture.peer.clone();
    let first_task = tokio::spawn(async move {
        first_peer
            .run_until(
                pending::<()>(),
                RestartPolicy::OnFailure,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));
    fixture
        .peer_ref
        .send(PeerMessage::Panic)
        .await
        .expect("panic command sent");
    let notification = expect_down(next_event(&mut fixture.observed).await);
    assert_eq!(notification.generation, 0);
    assert_eq!(notification.reason, DownReason::Failure);
    assert!(
        first_task
            .await
            .expect_err("first peer task panicked")
            .is_panic()
    );

    let second_peer = fixture.peer.clone();
    let second_task = tokio::spawn(async move {
        second_peer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    assert_eq!(
        next_event(&mut fixture.observed).await,
        up("peer", 1),
        "the original watch reports the replacement incarnation"
    );
    fixture
        .peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    let notification = expect_down(next_event(&mut fixture.observed).await);
    assert_eq!(notification.generation, 1);
    assert_eq!(notification.reason, DownReason::Normal);

    second_task
        .await
        .expect("second peer task joined")
        .expect("second peer stopped cleanly");
    observer_task.abort();
}

#[tokio::test]
async fn watch_registered_between_incarnations_waits_for_next_up() {
    let mut fixture = fixture(false);
    let first_peer = fixture.peer.clone();
    let first_task = tokio::spawn(async move {
        first_peer
            .run_until(
                pending::<()>(),
                RestartPolicy::OnFailure,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    fixture
        .peer_ref
        .send(PeerMessage::Panic)
        .await
        .expect("panic command sent");
    assert!(
        first_task
            .await
            .expect_err("first peer task panicked")
            .is_panic()
    );

    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_silence(&fixture.observer_ref, &mut fixture.observed).await;

    let second_peer = fixture.peer.clone();
    let second_task = tokio::spawn(async move {
        second_peer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    assert_eq!(
        next_event(&mut fixture.observed).await,
        up("peer", 1),
        "a watch registered in the restart gap converges without retry"
    );

    second_task.abort();
    observer_task.abort();
}

#[tokio::test]
async fn pre_start_watch_attaches_to_first_incarnation() {
    let mut fixture = fixture(false);
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_silence(&fixture.observer_ref, &mut fixture.observed).await;

    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    started(&mut fixture.peer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));
    fixture
        .peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    let notification = expect_down(next_event(&mut fixture.observed).await);
    assert_eq!(notification.generation, 0);
    assert_eq!(notification.reason, DownReason::Normal);

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    observer_task.abort();
}

#[tokio::test]
async fn shutdown_request_reports_normal_exit() {
    let mut fixture = fixture(false);
    let peer_stop = CancellationToken::new();
    let stop = peer_stop.clone();
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            stop.cancelled(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;
    assert_eq!(next_event(&mut fixture.observed).await, up("peer", 0));

    peer_stop.cancel();
    assert_eq!(
        expect_down(next_event(&mut fixture.observed).await).reason,
        DownReason::Normal
    );
    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        Some(0)
    );
    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    observer_task.abort();
}

#[tokio::test]
async fn two_observers_receive_the_same_events() {
    let mut fixture = fixture(false);
    let (second_observed_tx, mut second_observed) = mpsc::unbounded_channel();
    let (second_started_tx, mut second_started) = mpsc::unbounded_channel();
    let (second_observer, _) = runnable_actor("second-observer", {
        let peer_ref = fixture.peer_ref.clone();
        move || Observer {
            peer: peer_ref.clone(),
            observed: second_observed_tx.clone(),
            started: second_started_tx.clone(),
            cancel_watch: false,
        }
    });
    let first_observer = fixture.observer.clone();
    let first_task = tokio::spawn(async move {
        first_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    let second_task = tokio::spawn(async move {
        second_observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    let peer = fixture.peer.clone();
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;
    started(&mut second_started).await;

    fixture
        .peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    for _ in 0..3 {
        let first = next_event(&mut fixture.observed).await;
        let second = next_event(&mut second_observed).await;
        assert_eq!(first, second);
    }

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    first_task.abort();
    second_task.abort();
}

#[derive(Clone)]
struct GatedObserver {
    peer: ActorRef<PeerMessage>,
    gate: Arc<Notify>,
    watch: mpsc::UnboundedSender<CancellationHandle>,
    observed: mpsc::UnboundedSender<MonitorEvent>,
}

impl RawActor for GatedObserver {
    type Msg = MonitorEvent;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let watch = ctx.watch(&self.peer, |event| event);
        self.watch.send(watch).expect("watch receiver alive");
        self.gate.notified().await;
        while let Some(event) = ctx.recv().await {
            let done = matches!(event, MonitorEvent::Down(_));
            self.observed.send(event).expect("observer receiver alive");
            if done {
                break;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn cloned_watch_cancels_and_cannot_retract_accepted_events() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let gate = Arc::new(Notify::new());
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        let gate = gate.clone();
        move || GatedObserver {
            peer: peer_ref.clone(),
            gate: gate.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut peer_started).await;
    let watch = recv_test_event(&mut watch_rx, "gated observer watch handle").await;
    let clone = watch.clone();
    assert!(!watch.is_cancelled());

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    timeout(Duration::from_secs(1), async {
        while observer_ref.stats().messages_accepted < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("up and down accepted by observer mailbox");
    clone.cancel();
    assert!(watch.is_cancelled());
    gate.notify_one();
    assert_eq!(next_event(&mut observed).await, up("peer", 0));
    assert_eq!(
        expect_down(next_event(&mut observed).await).reason,
        DownReason::Normal
    );

    peer_task
        .await
        .expect("peer task joined")
        .expect("peer stopped cleanly");
    observer_task
        .await
        .expect("observer task joined")
        .expect("observer stopped cleanly");
}

#[derive(Clone)]
struct StubbornPeer {
    started: mpsc::UnboundedSender<()>,
}

impl RawActor for StubbornPeer {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<Self::Msg>) -> ActorResult {
        self.started.send(()).expect("start receiver alive");
        pending().await
    }
}

#[derive(Clone)]
struct UnitObserver {
    peer: ActorRef<()>,
    observed: mpsc::UnboundedSender<MonitorEvent>,
    started: mpsc::UnboundedSender<()>,
}

impl RawActor for UnitObserver {
    type Msg = MonitorEvent;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.watch(&self.peer, |event| event);
        self.started.send(()).expect("start receiver alive");
        while let Some(event) = ctx.recv().await {
            self.observed.send(event).expect("observer receiver alive");
        }
        Ok(())
    }
}

#[tokio::test]
async fn supervisor_abort_delivers_failure_down_then_terminated() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let handle = DynamicTree::new().spawn().expect("dynamic runtime builds");
    let peer_ref = handle
        .add_actor_with(
            "peer",
            move || StubbornPeer {
                started: peer_started_tx.clone(),
            },
            DynamicActorOptions::new().shutdown(ShutdownPolicy::abort()),
        )
        .await
        .expect("peer added");
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    handle
        .add_actor_with(
            "observer",
            {
                let peer_ref = peer_ref.clone();
                move || UnitObserver {
                    peer: peer_ref.clone(),
                    observed: observed_tx.clone(),
                    started: observer_started_tx.clone(),
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("observer added");
    started(&mut peer_started).await;
    started(&mut observer_started).await;
    assert_eq!(next_event(&mut observed).await, up("peer", 0));

    handle
        .remove_child("peer")
        .await
        .expect("peer removed by abort");
    let notification = expect_down(next_event(&mut observed).await);
    assert_eq!(notification.actor_id, "peer");
    assert_eq!(notification.reason, DownReason::Failure);
    assert_eq!(
        expect_terminated(next_event(&mut observed).await, "peer"),
        Some(0),
        "removing the child terminates the binding"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("runtime stopped cleanly");
}

#[derive(Clone)]
struct PanickingMapper {
    peer: ActorRef<PeerMessage>,
    started: mpsc::UnboundedSender<()>,
    mapped: mpsc::UnboundedSender<()>,
}

impl RawActor for PanickingMapper {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let mapped = self.mapped.clone();
        ctx.watch(&self.peer, move |_event| {
            mapped.send(()).expect("mapping receiver alive");
            panic!("deliberate mapping panic")
        });
        self.started.send(()).expect("start receiver alive");
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn mapping_panic_does_not_change_target_exit() {
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = runnable_actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (mapped_tx, mut mapped_rx) = mpsc::unbounded_channel();
    let (observer, _) = runnable_actor("observer", {
        let peer_ref = peer_ref.clone();
        move || PanickingMapper {
            peer: peer_ref.clone(),
            started: observer_started_tx.clone(),
            mapped: mapped_tx.clone(),
        }
    });
    let peer_task = tokio::spawn(async move {
        peer.run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut peer_started).await;
    started(&mut observer_started).await;
    timeout(Duration::from_secs(1), mapped_rx.recv())
        .await
        .expect("mapping closure ran")
        .expect("mapping sender alive");

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("stop command sent");
    peer_task
        .await
        .expect("peer task joined")
        .expect("mapping panic did not affect clean peer exit");
    observer_task.abort();
}

#[tokio::test]
async fn pending_target_can_be_dropped_from_non_runtime_thread() {
    let (peer, peer_ref) = runnable_actor("peer", || Peer {
        started: mpsc::unbounded_channel().0,
    });
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observer, _) = runnable_actor("observer", move || Observer {
        peer: peer_ref.clone(),
        observed: observed_tx.clone(),
        started: observer_started_tx.clone(),
        cancel_watch: false,
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    started(&mut observer_started).await;

    std::thread::spawn(move || drop(peer))
        .join()
        .expect("dropping target outside Tokio does not panic");
    assert_eq!(
        expect_terminated(next_event(&mut observed).await, "peer"),
        None
    );
    observer_task.abort();
}
