use std::{future::pending, sync::Arc, time::Duration};

use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_otp::{
    ActorContext, ActorRef, ActorResult, CancellationHandle, Down, DownReason, GraphBuilder,
    MonitorEvent, RawActor, RebindPolicy, RunnableActor, RunnableActorFactory, Runtime,
    prelude::Continue,
};
use tokio_supervisor::ShutdownPolicy;
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
            Some(PeerMessage::Stop) | None => Ok(Continue),
            Some(PeerMessage::Panic) => panic!("deliberate peer panic"),
        }
    }
}

enum ObserverMessage {
    Event(MonitorEvent),
    Crash,
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
            }
        }
        Ok(Continue)
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

fn fixture(cancel_watch: bool) -> Fixture {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (observed_tx, observed) = mpsc::unbounded_channel();
    let (observer_started_tx, observer_started) = mpsc::unbounded_channel();
    let (observer, observer_ref) = factory.actor("observer", {
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

async fn assert_silence(receiver: &mut mpsc::UnboundedReceiver<MonitorEvent>) {
    assert!(
        timeout(Duration::from_millis(100), receiver.recv())
            .await
            .is_err(),
        "no further events expected"
    );
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
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_stop = CancellationToken::new();
    let observer = fixture.observer.clone();
    let stop = observer_stop.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(stop.cancelled(), RebindPolicy::Never)
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
        "RebindPolicy::Never terminates the binding after the failed run"
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
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_stop = CancellationToken::new();
    let stop = observer_stop.clone();
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(stop.cancelled(), RebindPolicy::Never)
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
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut fixture.peer_started).await;
    started(&mut fixture.observer_started).await;

    fixture
        .peer_ref
        .send(PeerMessage::Panic)
        .await
        .expect("panic command sent");
    assert_silence(&mut fixture.observed).await;
    assert!(peer_task.await.expect_err("peer task panicked").is_panic());
    observer_task.abort();
}

#[tokio::test]
async fn watch_survives_observer_restart_without_duplicate_registration() {
    let mut fixture = fixture(false);
    let peer = fixture.peer.clone();
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    started(&mut fixture.peer_started).await;

    let first_observer = fixture.observer.clone();
    let first_task = tokio::spawn(async move {
        first_observer
            .run_until(pending::<()>(), RebindPolicy::OnFailure)
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
            .run_until(pending::<()>(), RebindPolicy::Never)
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
    assert_silence(&mut fixture.observed).await;

    second_task.abort();
}

enum TaggedObserverMessage {
    Event {
        registration: usize,
        event: MonitorEvent,
    },
    Crash,
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
            }
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn replacement_incarnation_keeps_the_membership_owned_mapper() {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let registrations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        let registrations = registrations.clone();
        move || TaggedObserver {
            peer: peer_ref.clone(),
            registrations: registrations.clone(),
            started: observer_started_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let first_observer = observer.clone();
    let first_task = tokio::spawn(async move {
        first_observer
            .run_until(pending::<()>(), RebindPolicy::OnFailure)
            .await
    });
    started(&mut peer_started).await;
    started(&mut observer_started).await;
    let (registration, event) = observed.recv().await.expect("initial up delivered");
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
            .run_until(pending::<()>(), RebindPolicy::Never)
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
    let (registration, event) = observed.recv().await.expect("down delivered");
    assert_eq!(
        registration, 0,
        "replacement mapper did not replace the watch"
    );
    assert_eq!(expect_down(event).reason, DownReason::Normal);
    let (registration, event) = observed.recv().await.expect("terminal delivered");
    assert_eq!(registration, 0);
    assert_eq!(expect_terminated(event, "peer"), Some(0));
    assert!(
        timeout(Duration::from_millis(100), observed.recv())
            .await
            .is_err(),
        "replacement registration must not create a duplicate watch"
    );

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
        Ok(Continue)
    }
}

#[tokio::test]
async fn repeated_watch_calls_alias_until_cancelled() {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        move || AliasedObserver {
            peer: peer_ref.clone(),
            watches: watch_tx.clone(),
            started: observer_started_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    started(&mut peer_started).await;
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut observer_started).await;
    let first = watch_rx.recv().await.expect("first watch created");
    let second = watch_rx.recv().await.expect("second watch created");

    let (registration, event) = observed.recv().await.expect("initial up delivered");
    assert_eq!(registration, 0, "the first mapper owns the watch");
    assert_eq!(event, up("peer", 0));

    second.cancel();
    watch_cancelled(&first).await;
    assert!(second.is_cancelled(), "both handles alias one watch");

    observer_ref
        .send(AliasedObserverMessage::Rewatch)
        .await
        .expect("rewatch command sent");
    let fresh = watch_rx.recv().await.expect("replacement watch created");
    assert!(!fresh.is_cancelled());
    let (registration, event) = observed.recv().await.expect("fresh up delivered");
    assert_eq!(registration, 2, "the fresh mapper owns the new watch");
    assert_eq!(event, up("peer", 0));

    peer_ref
        .send(PeerMessage::Stop)
        .await
        .expect("peer stop sent");
    let (registration, event) = observed.recv().await.expect("down delivered");
    assert_eq!(registration, 2);
    assert_eq!(expect_down(event).reason, DownReason::Normal);
    let (registration, event) = observed.recv().await.expect("terminal delivered");
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
        Ok(Continue)
    }
}

#[tokio::test]
async fn observer_membership_removal_cancels_its_watches() {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, _observed_rx) = mpsc::unbounded_channel();
    let (observer, observer_ref) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        move || ManagedObserver {
            peer: peer_ref.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut peer_started).await;
    let watch = watch_rx.recv().await.expect("watch created");

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
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, _) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        move || ManagedObserver {
            peer: peer_ref.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut peer_started).await;
    let watch = watch_rx.recv().await.expect("watch created");
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
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut fixture.observer_started).await;

    assert_eq!(
        expect_terminated(next_event(&mut fixture.observed).await, "peer"),
        None,
        "a never-started terminated target has no last generation"
    );
    assert_silence(&mut fixture.observed).await;
    observer_task.abort();
}

#[tokio::test]
async fn watching_detached_peer_delivers_immediate_terminated() {
    let factory = RunnableActorFactory::new();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (started_tx, mut observer_started) = mpsc::unbounded_channel();
    let detached_peer = {
        let (_, peer_ref) = factory.actor("detached-peer", || Peer {
            started: mpsc::unbounded_channel().0,
        });
        peer_ref
    };
    let (observer, _) = factory.actor("observer", move || Observer {
        peer: detached_peer.clone(),
        observed: observed_tx.clone(),
        started: started_tx.clone(),
        cancel_watch: false,
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
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
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    let first_peer = fixture.peer.clone();
    let first_task = tokio::spawn(async move {
        first_peer
            .run_until(pending::<()>(), RebindPolicy::OnFailure)
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
            .run_until(pending::<()>(), RebindPolicy::Never)
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
            .run_until(pending::<()>(), RebindPolicy::OnFailure)
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
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_silence(&mut fixture.observed).await;

    let second_peer = fixture.peer.clone();
    let second_task = tokio::spawn(async move {
        second_peer
            .run_until(pending::<()>(), RebindPolicy::Never)
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
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut fixture.observer_started).await;
    assert_silence(&mut fixture.observed).await;

    let peer = fixture.peer.clone();
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
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
    let peer_task =
        tokio::spawn(async move { peer.run_until(stop.cancelled(), RebindPolicy::Never).await });
    let observer = fixture.observer.clone();
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
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
    let factory = RunnableActorFactory::new();
    let (second_observed_tx, mut second_observed) = mpsc::unbounded_channel();
    let (second_started_tx, mut second_started) = mpsc::unbounded_channel();
    let (second_observer, _) = factory.actor("second-observer", {
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
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    let second_task = tokio::spawn(async move {
        second_observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    let peer = fixture.peer.clone();
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
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
        Ok(Continue)
    }
}

#[tokio::test]
async fn cloned_watch_cancels_and_cannot_retract_accepted_events() {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let gate = Arc::new(Notify::new());
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer, observer_ref) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        let gate = gate.clone();
        move || GatedObserver {
            peer: peer_ref.clone(),
            gate: gate.clone(),
            watch: watch_tx.clone(),
            observed: observed_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
            .await
    });
    started(&mut peer_started).await;
    let watch = watch_rx.recv().await.expect("watch created");
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
        Ok(Continue)
    }
}

#[tokio::test]
async fn supervisor_abort_delivers_failure_down_then_terminated() {
    let mut builder = GraphBuilder::new();
    let (peer_slot, peer_ref) = builder.slot("peer");
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    builder.define(peer_slot, move || StubbornPeer {
        started: peer_started_tx.clone(),
    });
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    builder.actor("observer", {
        let peer_ref = peer_ref.clone();
        move || UnitObserver {
            peer: peer_ref.clone(),
            observed: observed_tx.clone(),
            started: observer_started_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let runtime = Runtime::builder()
        .graph(graph)
        .actor_shutdown(&peer_ref, ShutdownPolicy::abort())
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();
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
        Ok(Continue)
    }
}

#[tokio::test]
async fn mapping_panic_does_not_change_target_exit() {
    let factory = RunnableActorFactory::new();
    let (peer_started_tx, mut peer_started) = mpsc::unbounded_channel();
    let (peer, peer_ref) = factory.actor("peer", move || Peer {
        started: peer_started_tx.clone(),
    });
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (mapped_tx, mut mapped_rx) = mpsc::unbounded_channel();
    let (observer, _) = factory.actor("observer", {
        let peer_ref = peer_ref.clone();
        move || PanickingMapper {
            peer: peer_ref.clone(),
            started: observer_started_tx.clone(),
            mapped: mapped_tx.clone(),
        }
    });
    let peer_task =
        tokio::spawn(async move { peer.run_until(pending::<()>(), RebindPolicy::Never).await });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
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
    let factory = RunnableActorFactory::new();
    let (peer, peer_ref) = factory.actor("peer", || Peer {
        started: mpsc::unbounded_channel().0,
    });
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let (observer_started_tx, mut observer_started) = mpsc::unbounded_channel();
    let (observer, _) = factory.actor("observer", move || Observer {
        peer: peer_ref.clone(),
        observed: observed_tx.clone(),
        started: observer_started_tx.clone(),
        cancel_watch: false,
    });
    let observer_task = tokio::spawn(async move {
        observer
            .run_until(pending::<()>(), RebindPolicy::Never)
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
