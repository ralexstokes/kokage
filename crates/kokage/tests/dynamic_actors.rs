use std::{
    future::{Future, pending, poll_fn},
    io,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorResult, CancellationHandle, ControlError, DownReason, DrainPolicy,
    DynamicActorOptions, DynamicTree, GraphBuilder, MailboxMode, MessageContext, MonitorEvent,
    OrderedTree, RestartPolicy, RuntimeHandle, SendError, ShutdownPolicy, StartContext,
    StopContext, SupervisorBuildError, SupervisorError, TrySendError,
    host::{ActorContext, BoxError, ChildSpec, RawActor},
    observe::ChildMembershipView,
};
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};

struct Drain<M>(PhantomData<fn(M)>);

impl<M> Drain<M> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Clone for Drain<M> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Drain<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: ActorContext<M>) -> ActorResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[derive(Clone)]
struct GatedExit {
    release: Arc<Notify>,
    fail: bool,
}

impl RawActor for GatedExit {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        self.release.notified().await;
        if self.fail {
            Err(io::Error::other("dynamic actor failed").into())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct CleanStop {
    starts: Arc<AtomicUsize>,
}

impl Actor for CleanStop {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, (): (), ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        ctx.stop();
        Ok(())
    }
}

#[derive(Clone)]
struct RestartOnce {
    starts: Arc<AtomicUsize>,
}

impl RawActor for RestartOnce {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(io::Error::other("restart me").into())
        } else {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    }
}

enum WatchMsg {
    Watch(ActorRef<()>),
    Event(MonitorEvent),
}

#[derive(Clone)]
struct Watcher {
    observed: mpsc::UnboundedSender<MonitorEvent>,
}

impl RawActor for Watcher {
    type Msg = WatchMsg;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let mut watch: Option<CancellationHandle> = None;
        while let Some(message) = ctx.recv().await {
            match message {
                WatchMsg::Watch(target) => {
                    watch = Some(ctx.watch(&target, WatchMsg::Event));
                }
                WatchMsg::Event(event) => {
                    self.observed
                        .send(event)
                        .expect("monitor receiver remains alive");
                }
            }
        }
        drop(watch);
        Ok(())
    }
}

async fn wait_for_child(handle: &RuntimeHandle, id: &str, present: bool) {
    timeout(Duration::from_secs(1), async {
        let mut snapshots = handle.subscribe_snapshots();
        loop {
            if snapshots.latest().child(id).is_some() == present {
                return;
            }
            snapshots
                .changed()
                .await
                .expect("runtime remains available");
        }
    })
    .await
    .expect("child membership reached expected state");
}

// A child that recorded its exit but has not been dropped from membership yet
// is also observable on the removal path, so seeing that snapshot alone does not
// prove retention. Settling a later control operation flushes the removal path,
// and only then is the surviving entry a retention decision.
async fn wait_for_retained_terminal_child(handle: &RuntimeHandle, id: &str) {
    timeout(Duration::from_secs(1), async {
        let mut snapshots = handle.subscribe_snapshots();
        loop {
            if snapshots
                .latest()
                .child(id)
                .is_some_and(|child| child.state.last_exit().map(|exit| &exit.status).is_some())
            {
                return;
            }
            snapshots
                .changed()
                .await
                .expect("runtime remains available");
        }
    })
    .await
    .expect("terminal child remains in membership");

    handle
        .dynamic()
        .expect("dynamic scope")
        .add_actor("settle", Drain::<()>::new)
        .await
        .expect("settling actor added");
    handle
        .dynamic()
        .expect("dynamic scope")
        .remove_child("settle")
        .await
        .expect("settling actor removed");
    wait_for_child(handle, "settle", false).await;

    assert!(
        handle.snapshot().child(id).is_some_and(|child| child
            .state
            .last_exit()
            .map(|exit| &exit.status)
            .is_some()),
        "terminal child stays retained once the control loop has settled"
    );
}

async fn next_monitor_event(events: &mut mpsc::UnboundedReceiver<MonitorEvent>) -> MonitorEvent {
    timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("monitor event arrived")
        .expect("monitor sender remains alive")
}

async fn recv_test_event<T>(rx: &mut mpsc::UnboundedReceiver<T>, phase: &str) -> T {
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|| panic!("channel closed while waiting for {phase}"))
}

async fn wait_runtime_started(handle: &RuntimeHandle, phase: &str) {
    timeout(Duration::from_secs(2), handle.wait_started())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn shutdown_runtime(handle: &RuntimeHandle, phase: &str) {
    timeout(Duration::from_secs(2), handle.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

struct SizedMessage(Vec<u8>);

fn sized_message_size(message: &SizedMessage) -> usize {
    message.0.len()
}

#[derive(Clone)]
struct Observe {
    observed: mpsc::UnboundedSender<String>,
}

#[derive(Clone)]
struct ObserveOrder {
    observed: mpsc::UnboundedSender<(u8, u32)>,
}

impl RawActor for ObserveOrder {
    type Msg = (u8, u32);

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

impl RawActor for Observe {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

enum ForwardMsg {
    Target(ActorRef<String>),
    Forward(String),
}

#[derive(Clone)]
struct Forwarder;

impl RawActor for Forwarder {
    type Msg = ForwardMsg;

    async fn run(&mut self, mut ctx: ActorContext<ForwardMsg>) -> ActorResult {
        let mut target = None;
        while let Some(message) = ctx.recv().await {
            match message {
                ForwardMsg::Target(actor_ref) => target = Some(actor_ref),
                ForwardMsg::Forward(message) => {
                    target
                        .as_ref()
                        .expect("target distributed before forwarding")
                        .send(message)
                        .await?
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn graphless_runtime_adds_removes_and_readds_actors() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();

    assert!(handle.handle().snapshot().children.is_empty());
    let sink = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            {
                let observed_tx = observed_tx.clone();
                move || Observe {
                    observed: observed_tx.clone(),
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("sink added");
    sink.send("first".to_owned()).await.expect("message sent");
    assert_eq!(
        recv_test_event(&mut observed_rx, "first observed message").await,
        "first"
    );
    let initial_lineage = handle
        .handle()
        .snapshot()
        .child("sink")
        .expect("sink snapshot available")
        .lineage;
    assert_eq!(
        handle
            .handle()
            .actor_stats()
            .into_iter()
            .find(|stats| stats.actor_id == "sink")
            .expect("sink stats available")
            .lineage,
        Some(initial_lineage)
    );
    assert_eq!(
        sink.stats().lineage,
        None,
        "standalone ref stats have no supervisor context"
    );

    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .remove_child("sink")
        .await
        .expect("sink removed");
    assert!(matches!(
        sink.send("after-remove".to_owned()).await,
        Err(SendError { actor_id , .. }) if actor_id == "sink"
    ));

    let replacement = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            move || Observe {
                observed: observed_tx.clone(),
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("label can be reused");
    assert!(matches!(
        sink.send("stale-ref-must-not-cross-membership".to_owned())
            .await,
        Err(SendError { actor_id , .. }) if actor_id == "sink"
    ));
    replacement
        .send("second".to_owned())
        .await
        .expect("replacement receives");
    assert_eq!(
        recv_test_event(&mut observed_rx, "second observed message").await,
        "second"
    );
    let replacement_snapshot_lineage = handle
        .handle()
        .snapshot()
        .child("sink")
        .expect("replacement snapshot available")
        .lineage;
    let replacement_lineage = handle
        .handle()
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == "sink")
        .expect("replacement stats available")
        .lineage
        .expect("runtime stats include supervisor membership");
    assert_eq!(replacement_lineage, replacement_snapshot_lineage);
    assert!(replacement_lineage > initial_lineage);

    shutdown_runtime(&handle.handle(), "dynamic actor reference test shutdown").await;
}

#[tokio::test]
async fn fifo_mailbox_preserves_each_senders_enqueue_order() {
    const MESSAGES_PER_SENDER: u32 = 64;

    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let actor = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "ordered",
            move || ObserveOrder {
                observed: observed_tx.clone(),
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("ordered actor added");

    let mut senders = Vec::new();
    for sender in 0..2 {
        let actor = actor.clone();
        senders.push(tokio::spawn(async move {
            for sequence in 0..MESSAGES_PER_SENDER {
                actor
                    .send((sender, sequence))
                    .await
                    .expect("membership remains active");
                tokio::task::yield_now().await;
            }
        }));
    }

    let mut next = [0; 2];
    for _ in 0..(2 * MESSAGES_PER_SENDER) {
        let (sender, sequence) = timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("ordered message arrived")
            .expect("actor remains alive");
        assert_eq!(sequence, next[sender as usize]);
        next[sender as usize] += 1;
    }
    for sender in senders {
        sender.await.expect("sender task joined");
    }
    assert_eq!(next, [MESSAGES_PER_SENDER; 2]);

    shutdown_runtime(&handle.handle(), "FIFO mailbox test shutdown").await;
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemovalEvent {
    Holding,
    Drained(u32),
    OnStopStarted,
    OnStopFinished,
}

enum RemovalMsg {
    Hold,
    Work(u32),
}

#[derive(Clone)]
struct RemovalProbe {
    release_handler: Arc<Notify>,
    release_on_stop: Arc<Notify>,
    events: mpsc::UnboundedSender<RemovalEvent>,
    drain_policy: DrainPolicy,
}

impl Actor for RemovalProbe {
    type Msg = RemovalMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            RemovalMsg::Hold => {
                self.events
                    .send(RemovalEvent::Holding)
                    .expect("receiver alive");
                self.release_handler.notified().await;
            }
            RemovalMsg::Work(value) => {
                self.events
                    .send(RemovalEvent::Drained(value))
                    .expect("receiver alive");
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events
            .send(RemovalEvent::OnStopStarted)
            .expect("receiver alive");
        self.release_on_stop.notified().await;
        self.events
            .send(RemovalEvent::OnStopFinished)
            .expect("receiver alive");
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        self.drain_policy
    }
}

#[tokio::test]
async fn remove_child_closes_intake_drains_then_runs_on_stop_before_detach() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release_handler = Arc::new(Notify::new());
    let release_on_stop = Arc::new(Notify::new());
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let actor = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "removable",
            {
                let release_handler = release_handler.clone();
                let release_on_stop = release_on_stop.clone();
                move || RemovalProbe {
                    release_handler: release_handler.clone(),
                    release_on_stop: release_on_stop.clone(),
                    events: events_tx.clone(),
                    drain_policy: DrainPolicy::Drain,
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("actor added");

    actor.send(RemovalMsg::Hold).await.expect("hold accepted");
    assert_eq!(
        recv_test_event(&mut events_rx, "actor entering its held handler").await,
        RemovalEvent::Holding
    );

    let mut snapshots = handle.handle().subscribe_snapshots();
    let remover = handle.handle();
    let removal = tokio::spawn(async move {
        remover
            .dynamic()
            .expect("dynamic scope")
            .remove_child("removable")
            .await
    });
    timeout(Duration::from_secs(1), async {
        loop {
            if snapshots
                .latest()
                .child("removable")
                .is_some_and(|child| child.membership == ChildMembershipView::Removing)
            {
                break;
            }
            snapshots.changed().await.expect("runtime remains alive");
        }
    })
    .await
    .expect("membership entered Removing");

    // Removal has been requested, but the current handler has not yielded to
    // observe cancellation yet. This racing send is deliberately accepted and
    // becomes part of the prefix that Drain must handle.
    actor
        .send(RemovalMsg::Work(7))
        .await
        .expect("racing work accepted before intake closes");
    release_handler.notify_one();

    assert_eq!(
        recv_test_event(&mut events_rx, "queued work draining during removal").await,
        RemovalEvent::Drained(7)
    );
    assert_eq!(
        recv_test_event(&mut events_rx, "on_stop starting during removal").await,
        RemovalEvent::OnStopStarted
    );
    assert!(!removal.is_finished(), "removal waits for on_stop");
    assert!(snapshots.latest().child("removable").is_some());
    assert!(matches!(
        actor.try_send(RemovalMsg::Work(8)),
        Err(TrySendError::Closed { actor_id , .. }) if actor_id == "removable"
    ));

    // There is no public Draining state. An awaited send observes the closed
    // incarnation and waits for its terminal membership disposition.
    let stale = actor.clone();
    let mut during_on_stop = Box::pin(stale.send(RemovalMsg::Work(9)));
    let first_poll = poll_fn(|cx| Poll::Ready(during_on_stop.as_mut().poll(cx))).await;
    assert!(
        first_poll.is_pending(),
        "send waits while on_stop is still resolving lifecycle"
    );

    release_on_stop.notify_one();
    assert_eq!(
        recv_test_event(&mut events_rx, "on_stop finishing during removal").await,
        RemovalEvent::OnStopFinished
    );
    assert!(matches!(
        during_on_stop.await,
        Err(SendError { actor_id , .. }) if actor_id == "removable"
    ));
    removal
        .await
        .expect("removal task joined")
        .expect("removal completed");
    assert!(handle.handle().snapshot().child("removable").is_none());

    let replacement = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "removable",
            Drain::<RemovalMsg>::new,
            DynamicActorOptions::default(),
        )
        .await
        .expect("id reused with a fresh membership");
    assert!(matches!(
        actor.send(RemovalMsg::Work(10)).await,
        Err(SendError { actor_id , .. }) if actor_id == "removable"
    ));
    replacement
        .send(RemovalMsg::Work(11))
        .await
        .expect("fresh ref addresses replacement membership");

    shutdown_runtime(&handle.handle(), "cooperative removal test shutdown").await;
}

#[tokio::test]
async fn discard_closes_intake_and_drops_racing_messages() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release_handler = Arc::new(Notify::new());
    let release_on_stop = Arc::new(Notify::new());
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let actor = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "discarding",
            {
                let release_handler = release_handler.clone();
                let release_on_stop = release_on_stop.clone();
                move || RemovalProbe {
                    release_handler: release_handler.clone(),
                    release_on_stop: release_on_stop.clone(),
                    events: events_tx.clone(),
                    drain_policy: DrainPolicy::Discard,
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("actor added");

    actor.send(RemovalMsg::Hold).await.expect("hold accepted");
    assert_eq!(
        recv_test_event(&mut events_rx, "actor entering its held handler").await,
        RemovalEvent::Holding
    );

    let mut snapshots = handle.handle().subscribe_snapshots();
    let remover = handle.handle();
    let removal = tokio::spawn(async move {
        remover
            .dynamic()
            .expect("dynamic scope")
            .remove_child("discarding")
            .await
    });
    timeout(Duration::from_secs(1), async {
        loop {
            if snapshots
                .latest()
                .child("discarding")
                .is_some_and(|child| child.membership == ChildMembershipView::Removing)
            {
                break;
            }
            snapshots.changed().await.expect("runtime remains alive");
        }
    })
    .await
    .expect("membership entered Removing");

    actor
        .send(RemovalMsg::Work(7))
        .await
        .expect("racing work accepted before handler observes shutdown");
    release_handler.notify_one();
    assert_eq!(
        recv_test_event(&mut events_rx, "on_stop starting during shutdown").await,
        RemovalEvent::OnStopStarted
    );

    assert!(matches!(
        actor.try_send(RemovalMsg::Work(8)),
        Err(TrySendError::Closed { actor_id , .. }) if actor_id == "discarding"
    ));
    assert!(!removal.is_finished(), "removal waits for on_stop");
    release_on_stop.notify_one();
    assert_eq!(
        recv_test_event(&mut events_rx, "on_stop finishing during shutdown").await,
        RemovalEvent::OnStopFinished
    );
    removal
        .await
        .expect("removal task joined")
        .expect("removal completed");
    assert!(
        matches!(
            events_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty
                | tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ),
        "messages accepted around Discard removal were not handled"
    );

    shutdown_runtime(&handle.handle(), "shutdown-during-removal test shutdown").await;
}

#[tokio::test]
async fn default_terminal_removal_preserves_monitor_order_and_reuses_id() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (watcher_slot, watcher) = graph.slot("watcher");
    graph.define(watcher_slot, move || Watcher {
        observed: observed_tx.clone(),
    });
    let graph = graph.build().expect("valid graph");
    let handle = OrderedTree::graph(graph)
        .subtree("dynamic", DynamicTree::new())
        .spawn()
        .expect("mixed scope runtime builds");
    wait_runtime_started(&handle.handle(), "mixed runtime startup").await;
    let dynamic = handle
        .handle()
        .subtree("dynamic")
        .expect("dynamic subtree is available");
    let starts = Arc::new(AtomicUsize::new(0));
    let target = dynamic
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "temporary",
            {
                let starts = starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("temporary actor added");

    watcher
        .send(WatchMsg::Watch(target.clone()))
        .await
        .expect("watch requested");
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent::Up { ref actor_id, .. } if actor_id == "temporary"
    ));

    target.send(()).await.expect("clean stop requested");
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent::Down { ref actor_id, reason: DownReason::Normal, .. }
            if actor_id == "temporary"
    ));
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent::Terminated { ref actor_id, .. } if actor_id == "temporary"
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    wait_for_child(&dynamic, "temporary", false).await;
    assert!(matches!(
        target.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "temporary"
    ));

    dynamic
        .dynamic()
        .expect("dynamic scope")
        .add_actor("temporary", Drain::<()>::new)
        .await
        .expect("auto-removed actor id is reusable");
    shutdown_runtime(&handle.handle(), "remove-on-exit monitor test shutdown").await;
}

#[tokio::test]
async fn message_context_stop_applies_restart_policy_before_default_removal() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();

    let transient_starts = Arc::new(AtomicUsize::new(0));
    let transient = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "transient",
            {
                let starts = transient_starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("transient actor added");
    transient.send(()).await.expect("clean stop requested");
    wait_for_child(&handle.handle(), "transient", false).await;
    assert_eq!(transient_starts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        transient.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "transient"
    ));

    let permanent_starts = Arc::new(AtomicUsize::new(0));
    let permanent = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "permanent",
            {
                let starts = permanent_starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::Always),
        )
        .await
        .expect("permanent actor added");
    permanent.send(()).await.expect("clean stop requested");
    timeout(Duration::from_secs(1), async {
        while permanent_starts.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Always actor restarted after clean stop");
    assert!(
        handle
            .handle()
            .snapshot()
            .child("permanent")
            .is_some_and(|child| child.generation >= 1)
    );

    shutdown_runtime(&handle.handle(), "restart policy test shutdown").await;
}

#[tokio::test]
async fn dynamic_runtime_defaults_apply_and_explicit_actor_options_win() {
    let runtime = DynamicTree::new()
        .default_restart(RestartPolicy::Always)
        .default_shutdown(ShutdownPolicy::Abort)
        .spawn()
        .expect("dynamic runtime builds");
    let handle = runtime.handle();
    let inherited_starts = Arc::new(AtomicUsize::new(0));
    let inherited = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "inherited",
            {
                let starts = Arc::clone(&inherited_starts);
                move || CleanStop {
                    starts: Arc::clone(&starts),
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("inherited actor added");
    let explicit_starts = Arc::new(AtomicUsize::new(0));
    let explicit = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "explicit",
            {
                let starts = Arc::clone(&explicit_starts);
                move || CleanStop {
                    starts: Arc::clone(&starts),
                }
            },
            DynamicActorOptions::default().restart(RestartPolicy::Never),
        )
        .await
        .expect("explicit actor added");

    inherited.send(()).await.expect("inherited actor stops");
    explicit.send(()).await.expect("explicit actor stops");
    timeout(Duration::from_secs(1), async {
        while inherited_starts.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("builder default restarts the cleanly stopped actor");
    wait_for_child(&handle.handle(), "explicit", false).await;
    assert_eq!(explicit_starts.load(Ordering::SeqCst), 1);

    shutdown_runtime(&handle.handle(), "dynamic default options test shutdown").await;
}

#[tokio::test]
async fn dynamic_tree_applies_scope_defaults_to_runtime_actors() {
    let runtime = DynamicTree::new()
        .default_restart(RestartPolicy::Always)
        .default_shutdown(ShutdownPolicy::Abort)
        .spawn()
        .expect("dynamic tree builds");
    let handle = runtime.handle();
    let starts = Arc::new(AtomicUsize::new(0));
    let actor = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "inherited-restart",
            {
                let starts = Arc::clone(&starts);
                move || CleanStop {
                    starts: Arc::clone(&starts),
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("actor added");
    actor.send(()).await.expect("actor stops cleanly");
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supplied supervisor restart default is inherited");

    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "inherited-shutdown",
            || PendingActor,
            DynamicActorOptions::default(),
        )
        .await
        .expect("pending actor added");
    timeout(
        Duration::from_millis(100),
        handle
            .handle()
            .dynamic()
            .expect("dynamic scope")
            .remove_child("inherited-shutdown"),
    )
    .await
    .expect("supplied abort default makes removal immediate")
    .expect("pending actor removed");

    shutdown_runtime(
        &handle.handle(),
        "supplied supervisor defaults test shutdown",
    )
    .await;
}

#[tokio::test]
async fn never_actor_auto_removes_after_failure() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let release = Arc::new(Notify::new());
    let target = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "temporary",
            {
                let release = release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: true,
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::Never),
        )
        .await
        .expect("temporary actor added");

    release.notify_one();
    wait_for_child(&handle.handle(), "temporary", false).await;
    assert!(matches!(
        target.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "temporary"
    ));
    shutdown_runtime(&handle.handle(), "never-actor removal test shutdown").await;
}

#[tokio::test]
async fn remove_on_exit_defaults_to_true_and_false_override_is_order_independent() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();

    let default_release = Arc::new(Notify::new());
    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "transient-default",
            {
                let release = default_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("transient actor added");
    default_release.notify_one();
    wait_for_child(&handle.handle(), "transient-default", false).await;

    let transient_release = Arc::new(Notify::new());
    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "transient-retained",
            {
                let release = transient_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            },
            DynamicActorOptions::new()
                .remove_on_exit(false)
                .restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("retained transient actor added");
    transient_release.notify_one();
    wait_for_retained_terminal_child(&handle.handle(), "transient-retained").await;

    let reversed_release = Arc::new(Notify::new());
    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "transient-retained-reversed",
            {
                let release = reversed_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            },
            DynamicActorOptions::new()
                .restart(RestartPolicy::OnFailure)
                .remove_on_exit(false),
        )
        .await
        .expect("reversed retained transient actor added");
    reversed_release.notify_one();
    wait_for_retained_terminal_child(&handle.handle(), "transient-retained-reversed").await;

    let never_release = Arc::new(Notify::new());
    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "never-retained",
            {
                let release = never_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            },
            DynamicActorOptions::new()
                .restart(RestartPolicy::Never)
                .remove_on_exit(false),
        )
        .await
        .expect("retained never actor added");
    never_release.notify_one();
    wait_for_retained_terminal_child(&handle.handle(), "never-retained").await;

    shutdown_runtime(&handle.handle(), "remove-on-exit ordering test shutdown").await;
}

#[tokio::test]
async fn default_remove_on_exit_does_not_remove_an_actor_that_restarts() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let starts = Arc::new(AtomicUsize::new(0));
    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "restart-once",
            {
                let starts = starts.clone();
                move || RestartOnce {
                    starts: starts.clone(),
                }
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("restartable actor added");

    timeout(Duration::from_secs(1), async {
        let mut snapshots = handle.handle().subscribe_snapshots();
        loop {
            if snapshots
                .latest()
                .child("restart-once")
                .is_some_and(|child| child.generation >= 1)
            {
                break;
            }
            snapshots
                .changed()
                .await
                .expect("runtime remains available");
        }
    })
    .await
    .expect("actor restarted");
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    shutdown_runtime(&handle.handle(), "restarting remove-on-exit test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_can_observe_message_sizes() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let sink = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            Drain::<SizedMessage>::new,
            DynamicActorOptions::default()
                .mailbox(MailboxMode::conflate())
                .message_size(sized_message_size),
        )
        .await
        .expect("sized actor added");

    sink.send(SizedMessage(vec![0; 12]))
        .await
        .expect("message sent");
    let stats = handle
        .handle()
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == "sink")
        .expect("dynamic actor stats available");
    assert_eq!(stats.message_bytes_accepted, Some(12));
    assert_eq!(stats.mailbox_capacity, 1);

    shutdown_runtime(&handle.handle(), "message-size observation test shutdown").await;
}

#[derive(Clone)]
struct GatedDrain {
    release: Arc<Notify>,
}

impl RawActor for GatedDrain {
    type Msg = u64;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        self.release.notified().await;
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn runtime_added_actor_uses_non_default_mailbox_options() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let release = Arc::new(Notify::new());
    let sink = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            {
                let release = release.clone();
                move || GatedDrain {
                    release: release.clone(),
                }
            },
            DynamicActorOptions::default().mailbox(MailboxMode::conflate()),
        )
        .await
        .expect("conflating actor added");

    for message in 0..3 {
        sink.send(message).await.expect("message accepted");
    }
    let stats = sink.stats();
    assert_eq!(stats.messages_accepted, 3);
    assert_eq!(stats.messages_conflated, 2);
    assert_eq!(stats.mailbox_capacity, 1);

    release.notify_one();
    shutdown_runtime(&handle.handle(), "conflating dynamic actor test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_can_override_mailbox_capacity() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let sink = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            Drain::<u64>::new,
            DynamicActorOptions::default().mailbox_capacity(9),
        )
        .await
        .expect("actor with a capacity override is added");

    sink.send(1).await.expect("message accepted");
    assert_eq!(sink.stats().mailbox_capacity, 9);

    shutdown_runtime(&handle.handle(), "mailbox capacity override test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_rejects_zero_mailbox_capacity() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let handle = runtime.handle();
    let result = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            Drain::<u64>::new,
            DynamicActorOptions::default().mailbox_capacity(0),
        )
        .await;

    assert!(matches!(
        result,
        Err(ControlError::Rejected(SupervisorBuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        )))
    ));

    shutdown_runtime(&handle.handle(), "zero mailbox capacity test shutdown").await;
}

#[tokio::test]
async fn runtime_added_ref_is_distributed_to_static_actor_by_message() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let (forwarder_slot, forwarder) = builder.slot("forwarder");
    builder.define(forwarder_slot, || Forwarder);
    let graph = builder.build().expect("valid graph");
    let handle = OrderedTree::graph(graph)
        .subtree("dynamic", DynamicTree::new())
        .spawn()
        .expect("mixed scope runtime builds");
    wait_runtime_started(&handle.handle(), "mixed runtime startup").await;
    let dynamic = handle
        .handle()
        .subtree("dynamic")
        .expect("dynamic subtree is available");

    let sink = dynamic
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "sink",
            move || Observe {
                observed: observed_tx.clone(),
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("sink added");
    forwarder
        .send(ForwardMsg::Target(sink))
        .await
        .expect("typed ref distributed");
    forwarder
        .send(ForwardMsg::Forward("forwarded".to_owned()))
        .await
        .expect("message forwarded");

    assert_eq!(
        recv_test_event(&mut observed_rx, "forwarded static-to-dynamic message").await,
        "forwarded"
    );
    shutdown_runtime(
        &handle.handle(),
        "static-to-dynamic forwarding test shutdown",
    )
    .await;
}

#[derive(Clone)]
struct ForwardTo {
    target: ActorRef<String>,
}

impl RawActor for ForwardTo {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        while let Some(message) = ctx.recv().await {
            self.target.send(message).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn runtime_added_actor_can_receive_static_ref_at_creation() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let (sink_slot, sink) = builder.slot("sink");
    builder.define(sink_slot, move || Observe {
        observed: observed_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");
    let handle = OrderedTree::graph(graph)
        .subtree("dynamic", DynamicTree::new())
        .spawn()
        .expect("mixed scope runtime builds");
    wait_runtime_started(&handle.handle(), "mixed runtime startup").await;
    let dynamic_scope = handle
        .handle()
        .subtree("dynamic")
        .expect("dynamic subtree is available");

    let dynamic = dynamic_scope
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "dynamic",
            move || ForwardTo {
                target: sink.clone(),
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("dynamic actor added");
    dynamic
        .send("forwarded".to_owned())
        .await
        .expect("dynamic actor receives");
    assert_eq!(
        recv_test_event(&mut observed_rx, "forwarded dynamic-to-static message").await,
        "forwarded"
    );

    shutdown_runtime(
        &handle.handle(),
        "dynamic-to-static forwarding test shutdown",
    )
    .await;
}

#[derive(Clone)]
struct PendingActor;

impl RawActor for PendingActor {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        pending::<()>().await;
        Ok(())
    }
}

#[tokio::test]
async fn timed_out_removal_terminates_the_typed_ref() {
    let runtime = DynamicTree::new().spawn().expect("runtime builds");
    let handle = runtime.handle();
    let actor_ref = handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor_with(
            "dynamic",
            || PendingActor,
            DynamicActorOptions::new().shutdown(ShutdownPolicy::Cooperative {
                grace: Duration::from_millis(20),
            }),
        )
        .await
        .expect("actor added");

    assert!(matches!(
        handle.handle().dynamic().expect("dynamic scope").remove_child("dynamic").await,
        Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(actor_id)))
            if actor_id == "dynamic"
    ));
    assert!(
        handle
            .handle()
            .actor_stats()
            .iter()
            .all(|stats| stats.actor_id != "dynamic"),
        "timed-out removal immediately forgets actor stats"
    );
    assert!(matches!(
        actor_ref.send(()).await,
        Err(SendError { actor_id , .. }) if actor_id == "dynamic"
    ));

    handle
        .handle()
        .dynamic()
        .expect("dynamic scope")
        .add_actor("dynamic", Drain::<()>::new)
        .await
        .expect("label reusable after timed-out removal");
    shutdown_runtime(&handle.handle(), "timed-out removal test shutdown").await;
}

#[tokio::test]
async fn ordered_tree_has_no_runtime_membership_capability() {
    let handle = OrderedTree::new()
        .task(ChildSpec::task("seed", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .spawn()
        .expect("valid tree");

    assert!(handle.handle().dynamic().is_none());

    timeout(Duration::from_secs(1), handle.shutdown_and_wait())
        .await
        .expect("shutdown completed")
        .expect("clean shutdown");
}
