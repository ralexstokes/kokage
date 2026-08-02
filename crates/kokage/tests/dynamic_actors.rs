mod support;

use support::TreeBuilder;

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
    Actor, ActorRef, ActorSlot, ActorSpec, BoxError, BuildError, Context, ControlError,
    DynamicScopeRef, DynamicTree, ExitResult, Guard, Mailbox, MailboxShutdown, MonitorEvent,
    MonitorEventKind, RestartPolicy, RunningTree, ScopeRef, SendError, SendErrorKind, Shutdown,
    StopContext, SupervisorError, TaskSpec, Tree,
    observe::{ChildMembershipView, ExitStatus, SupervisorStateView},
    raw::{RawActor, RawContext},
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

    async fn run(&mut self, mut ctx: RawContext<M>) -> ExitResult {
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

    async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
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

    async fn run(&mut self, ctx: RawContext<()>) -> ExitResult {
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

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        let mut watch: Option<Guard> = None;
        while let Some(message) = ctx.recv().await {
            match message {
                WatchMsg::Watch(target) => {
                    watch = Some(ctx.watch_scoped(&target, WatchMsg::Event));
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

async fn wait_for_child(handle: &ScopeRef, id: &str, present: bool) {
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
async fn wait_for_retained_terminal_child(handle: &DynamicScopeRef, id: &str) {
    timeout(Duration::from_secs(1), async {
        let mut snapshots = handle.subscribe_snapshots();
        loop {
            if snapshots
                .latest()
                .child(id)
                .is_some_and(|child| child.state.last_exit().is_some())
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
        .add_actor_spec(ActorSpec::new("settle", Drain::<()>::new))
        .await
        .expect("settling actor added");
    handle
        .remove_named("settle")
        .await
        .expect("settling actor removed");
    wait_for_child(handle, "settle", false).await;

    assert!(
        handle
            .snapshot()
            .child(id)
            .is_some_and(|child| child.state.last_exit().is_some()),
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

async fn wait_runtime_started(handle: &ScopeRef, phase: &str) {
    timeout(Duration::from_secs(2), handle.wait_started())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn shutdown_runtime(handle: &ScopeRef, phase: &str) {
    timeout(Duration::from_secs(2), handle.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn shutdown_running_tree(running_tree: RunningTree<DynamicScopeRef>, phase: &str) {
    timeout(Duration::from_secs(2), running_tree.shutdown())
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

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

impl RawActor for Observe {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
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

    async fn run(&mut self, mut ctx: RawContext<ForwardMsg>) -> ExitResult {
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
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    assert!(running_tree.scope().snapshot().children.is_empty());
    let sink = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("sink", {
            let observed_tx = observed_tx.clone();
            move || Observe {
                observed: observed_tx.clone(),
            }
        }))
        .await
        .expect("sink added");
    sink.send("first".to_owned()).await.expect("message sent");
    assert_eq!(
        recv_test_event(&mut observed_rx, "first observed message").await,
        "first"
    );
    let initial_lineage = running_tree
        .scope()
        .snapshot()
        .child("sink")
        .expect("sink snapshot available")
        .lineage;
    assert_eq!(
        running_tree
            .scope()
            .actor_stats()
            .into_iter()
            .find(|stats| stats.stats.actor_id == "sink")
            .expect("sink stats available")
            .lineage,
        initial_lineage
    );
    assert_eq!(sink.stats().actor_id, "sink");

    support::dynamic_root(&running_tree)
        .remove_named("sink")
        .await
        .expect("sink removed");
    assert!(matches!(
        sink.send("after-remove".to_owned()).await,
        Err(SendError { actor_id , .. }) if actor_id == "sink"
    ));

    let replacement = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("sink", move || Observe {
            observed: observed_tx.clone(),
        }))
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
    let replacement_snapshot_lineage = running_tree
        .scope()
        .snapshot()
        .child("sink")
        .expect("replacement snapshot available")
        .lineage;
    let replacement_lineage = running_tree
        .scope()
        .actor_stats()
        .into_iter()
        .find(|stats| stats.stats.actor_id == "sink")
        .expect("replacement stats available")
        .lineage;
    assert_eq!(replacement_lineage, replacement_snapshot_lineage);
    assert!(replacement_lineage > initial_lineage);

    shutdown_running_tree(running_tree, "dynamic actor reference test shutdown").await;
}

#[tokio::test]
async fn fifo_mailbox_preserves_each_senders_enqueue_order() {
    const MESSAGES_PER_SENDER: u32 = 64;

    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let actor = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("ordered", move || ObserveOrder {
            observed: observed_tx.clone(),
        }))
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

    shutdown_running_tree(running_tree, "FIFO mailbox test shutdown").await;
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
}

impl Actor for RemovalProbe {
    type Msg = RemovalMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
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
}

#[tokio::test]
async fn remove_child_closes_intake_drains_then_runs_on_stop_before_detach() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release_handler = Arc::new(Notify::new());
    let release_on_stop = Arc::new(Notify::new());
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let actor = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("removable", {
                let release_handler = release_handler.clone();
                let release_on_stop = release_on_stop.clone();
                move || RemovalProbe {
                    release_handler: release_handler.clone(),
                    release_on_stop: release_on_stop.clone(),
                    events: events_tx.clone(),
                }
            })
            .shutdown(Shutdown::graceful_for(Duration::from_secs(5))),
        )
        .await
        .expect("actor added");

    actor.send(RemovalMsg::Hold).await.expect("hold accepted");
    assert_eq!(
        recv_test_event(&mut events_rx, "actor entering its held handler").await,
        RemovalEvent::Holding
    );

    let mut snapshots = running_tree.scope().subscribe_snapshots();
    let remover = support::dynamic_root(&running_tree);
    let removal = tokio::spawn(async move { remover.remove_named("removable").await });
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
    let not_running = actor
        .try_send(RemovalMsg::Work(8))
        .expect_err("closed intake rejects try_send");
    assert!(matches!(
        &not_running,
        SendError {
            actor_id,
            kind: SendErrorKind::NotRunning,
            ..
        } if actor_id == "removable"
    ));
    assert!(matches!(not_running.into_message(), RemovalMsg::Work(8)));

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
    assert!(running_tree.scope().snapshot().child("removable").is_none());

    let replacement = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("removable", Drain::<RemovalMsg>::new))
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

    shutdown_running_tree(running_tree, "cooperative removal test shutdown").await;
}

#[tokio::test]
async fn discard_closes_intake_and_drops_racing_messages() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release_handler = Arc::new(Notify::new());
    let release_on_stop = Arc::new(Notify::new());
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let actor = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("discarding", {
                let release_handler = release_handler.clone();
                let release_on_stop = release_on_stop.clone();
                move || RemovalProbe {
                    release_handler: release_handler.clone(),
                    release_on_stop: release_on_stop.clone(),
                    events: events_tx.clone(),
                }
            })
            .shutdown(Shutdown::graceful_for(Duration::from_secs(5)))
            .mailbox_shutdown(MailboxShutdown::Discard),
        )
        .await
        .expect("actor added");

    actor.send(RemovalMsg::Hold).await.expect("hold accepted");
    assert_eq!(
        recv_test_event(&mut events_rx, "actor entering its held handler").await,
        RemovalEvent::Holding
    );

    let mut snapshots = running_tree.scope().subscribe_snapshots();
    let remover = support::dynamic_root(&running_tree);
    let removal = tokio::spawn(async move { remover.remove_named("discarding").await });
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
        Err(SendError {
            actor_id,
            kind: SendErrorKind::NotRunning,
            ..
        }) if actor_id == "discarding"
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

    shutdown_running_tree(running_tree, "shutdown-during-removal test shutdown").await;
}

#[tokio::test]
async fn explicit_terminal_removal_preserves_monitor_order_and_reuses_id() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = TreeBuilder::new();
    let watcher_slot = ActorSlot::new("watcher");
    let watcher = watcher_slot.actor_ref();
    graph.define(watcher_slot, move || Watcher {
        observed: observed_tx.clone(),
    });
    let mut graph = graph.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let handle = graph.spawn().expect("mixed scope runtime builds");
    wait_runtime_started(&handle.scope(), "mixed runtime startup").await;
    let dynamic = handle
        .scope()
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("dynamic subtree is available");
    let starts = Arc::new(AtomicUsize::new(0));
    let target = dynamic
        .add_actor_spec(
            ActorSpec::new("temporary", {
                let starts = starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            })
            .restart(RestartPolicy::on_failure())
            .remove_on_terminal_exit(),
        )
        .await
        .expect("temporary actor added");

    watcher
        .send(WatchMsg::Watch(target.clone()))
        .await
        .expect("watch requested");
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent { ref actor_id, kind: MonitorEventKind::Started { .. }, .. }
            if actor_id == "temporary"
    ));

    target.send(()).await.expect("clean stop requested");
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent {
            ref actor_id,
            kind: MonitorEventKind::Exited {
                status: ExitStatus::Completed { .. },
                ..
            },
            ..
        }
            if actor_id == "temporary"
    ));
    assert!(matches!(
        next_monitor_event(&mut observed_rx).await,
        MonitorEvent { ref actor_id, kind: MonitorEventKind::Removed { .. }, .. }
            if actor_id == "temporary"
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    wait_for_child(&dynamic, "temporary", false).await;
    assert!(matches!(
        target.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "temporary"
    ));

    dynamic
        .add_actor_spec(ActorSpec::new("temporary", Drain::<()>::new))
        .await
        .expect("auto-removed actor id is reusable");
    shutdown_runtime(&handle.scope(), "remove-on-exit monitor test shutdown").await;
}

#[tokio::test]
async fn context_stop_applies_restart_policy_before_explicit_removal() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let transient_starts = Arc::new(AtomicUsize::new(0));
    let transient = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("transient", {
                let starts = transient_starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            })
            .restart(RestartPolicy::on_failure())
            .remove_on_terminal_exit(),
        )
        .await
        .expect("transient actor added");
    transient.send(()).await.expect("clean stop requested");
    wait_for_child(&running_tree.scope(), "transient", false).await;
    assert_eq!(transient_starts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        transient.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "transient"
    ));

    let permanent_starts = Arc::new(AtomicUsize::new(0));
    let permanent = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("permanent", {
                let starts = permanent_starts.clone();
                move || CleanStop {
                    starts: starts.clone(),
                }
            })
            .restart(RestartPolicy::always()),
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
        running_tree
            .scope()
            .snapshot()
            .child("permanent")
            .is_some_and(|child| child.generation >= 1)
    );

    shutdown_running_tree(running_tree, "restart policy test shutdown").await;
}

#[tokio::test]
async fn dynamic_runtime_defaults_apply_and_explicit_actor_options_win() {
    let running_tree = DynamicTree::new()
        .default_child_restart(RestartPolicy::always())
        .default_child_shutdown(Shutdown::abort())
        .spawn()
        .expect("dynamic runtime builds");
    let inherited_starts = Arc::new(AtomicUsize::new(0));
    let inherited = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("inherited", {
            let starts = Arc::clone(&inherited_starts);
            move || CleanStop {
                starts: Arc::clone(&starts),
            }
        }))
        .await
        .expect("inherited actor added");
    let explicit_starts = Arc::new(AtomicUsize::new(0));
    let explicit = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("explicit", {
                let starts = Arc::clone(&explicit_starts);
                move || CleanStop {
                    starts: Arc::clone(&starts),
                }
            })
            .temporary(),
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
    wait_for_child(&running_tree.scope(), "explicit", false).await;
    assert_eq!(explicit_starts.load(Ordering::SeqCst), 1);

    shutdown_running_tree(running_tree, "dynamic default options test shutdown").await;
}

#[tokio::test]
async fn dynamic_tree_applies_scope_defaults_to_runtime_actors() {
    let running_tree = DynamicTree::new()
        .default_child_restart(RestartPolicy::always())
        .default_child_shutdown(Shutdown::abort())
        .spawn()
        .expect("dynamic tree builds");
    let starts = Arc::new(AtomicUsize::new(0));
    let actor = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("inherited-restart", {
            let starts = Arc::clone(&starts);
            move || CleanStop {
                starts: Arc::clone(&starts),
            }
        }))
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

    support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("inherited-shutdown", || PendingActor))
        .await
        .expect("pending actor added");
    timeout(
        Duration::from_millis(100),
        support::dynamic_root(&running_tree).remove_named("inherited-shutdown"),
    )
    .await
    .expect("supplied abort default makes removal immediate")
    .expect("pending actor removed");

    shutdown_running_tree(running_tree, "supplied supervisor defaults test shutdown").await;
}

#[tokio::test]
async fn temporary_actor_auto_removes_after_failure() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let release = Arc::new(Notify::new());
    let target = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("temporary", {
                let release = release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: true,
                }
            })
            .temporary(),
        )
        .await
        .expect("temporary actor added");

    release.notify_one();
    wait_for_child(&running_tree.scope(), "temporary", false).await;
    assert!(matches!(
        target.send(()).await,
        Err(SendError { actor_id, .. }) if actor_id == "temporary"
    ));
    shutdown_running_tree(running_tree, "temporary-actor removal test shutdown").await;
}

#[tokio::test]
async fn completed_membership_is_retained_unless_spec_removes_it() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let default_release = Arc::new(Notify::new());
    support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("transient-removed", {
                let release = default_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            })
            .restart(RestartPolicy::on_failure())
            .remove_on_terminal_exit(),
        )
        .await
        .expect("transient actor added");
    default_release.notify_one();
    wait_for_child(&running_tree.scope(), "transient-removed", false).await;

    let transient_release = Arc::new(Notify::new());
    support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("transient-retained", {
                let release = transient_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            })
            .restart(RestartPolicy::on_failure()),
        )
        .await
        .expect("retained transient actor added");
    transient_release.notify_one();
    wait_for_retained_terminal_child(&running_tree.scope(), "transient-retained").await;

    let reversed_release = Arc::new(Notify::new());
    support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("transient-retained-reversed", {
                let release = reversed_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            })
            .restart(RestartPolicy::on_failure()),
        )
        .await
        .expect("reversed retained transient actor added");
    reversed_release.notify_one();
    wait_for_retained_terminal_child(&running_tree.scope(), "transient-retained-reversed").await;

    let never_release = Arc::new(Notify::new());
    support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("never-retained", {
                let release = never_release.clone();
                move || GatedExit {
                    release: release.clone(),
                    fail: false,
                }
            })
            .restart(RestartPolicy::never()),
        )
        .await
        .expect("retained never actor added");
    never_release.notify_one();
    wait_for_retained_terminal_child(&running_tree.scope(), "never-retained").await;

    shutdown_running_tree(running_tree, "terminal-membership ordering test shutdown").await;
}

#[tokio::test]
async fn remove_on_terminal_exit_does_not_remove_an_actor_that_restarts() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let starts = Arc::new(AtomicUsize::new(0));
    support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("restart-once", {
                let starts = starts.clone();
                move || RestartOnce {
                    starts: starts.clone(),
                }
            })
            .restart(RestartPolicy::on_failure())
            .remove_on_terminal_exit(),
        )
        .await
        .expect("restartable actor added");

    timeout(Duration::from_secs(1), async {
        let mut snapshots = running_tree.scope().subscribe_snapshots();
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
    assert!(
        running_tree
            .scope()
            .snapshot()
            .child("restart-once")
            .expect("restarting actor keeps its membership")
            .remove_on_terminal_exit,
        "a live snapshot reports the spec-level retention declaration"
    );

    shutdown_running_tree(running_tree, "restarting removal test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_can_observe_message_sizes() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let sink_spec = ActorSpec::new("sink", Drain::<SizedMessage>::new).mailbox(Mailbox::latest());
    let sink = sink_spec.actor_ref();
    let second_ref = sink_spec.actor_ref();
    let sink_spec = sink_spec.message_size(sized_message_size);
    let inserted = support::dynamic_root(&running_tree)
        .add_actor_spec(sink_spec)
        .await
        .expect("sized actor added");

    assert_eq!(sink.id(), second_ref.id());
    assert_eq!(sink.id(), inserted.id());
    sink.send(SizedMessage(vec![0; 12]))
        .await
        .expect("message sent");
    let stats = running_tree
        .scope()
        .actor_stats()
        .into_iter()
        .find(|stats| stats.stats.actor_id == "sink")
        .expect("dynamic actor stats available");
    assert_eq!(stats.stats.message_bytes_accepted, Some(12));
    assert_eq!(second_ref.stats().message_bytes_accepted, Some(12));
    assert_eq!(stats.stats.mailbox_capacity, 1);

    shutdown_running_tree(running_tree, "message-size observation test shutdown").await;
}

#[derive(Clone)]
struct GatedDrain {
    release: Arc<Notify>,
}

impl RawActor for GatedDrain {
    type Msg = u64;

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        self.release.notified().await;
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

enum MailboxShutdownProbe {
    Gate,
    Count,
}

struct GatedMailboxProbe {
    id: &'static str,
    entered: mpsc::UnboundedSender<&'static str>,
    handled: mpsc::UnboundedSender<&'static str>,
    release: Arc<Notify>,
}

impl Actor for GatedMailboxProbe {
    type Msg = MailboxShutdownProbe;

    async fn handle(
        &mut self,
        message: MailboxShutdownProbe,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        match message {
            MailboxShutdownProbe::Gate => {
                self.entered.send(self.id).expect("probe receiver alive");
                self.release.notified().await;
            }
            MailboxShutdownProbe::Count => {
                self.handled.send(self.id).expect("probe receiver alive");
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn dynamic_scope_mailbox_shutdown_default_is_inherited_and_overridable() {
    let running_tree = DynamicTree::new()
        .default_actor_mailbox_shutdown(MailboxShutdown::Discard)
        .spawn()
        .expect("dynamic tree builds");
    let scope = support::dynamic_root(&running_tree);
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (handled_tx, mut handled_rx) = mpsc::unbounded_channel();
    let discard_release = Arc::new(Notify::new());
    let drain_release = Arc::new(Notify::new());

    let discarded = scope
        .add_actor("discarded", {
            let entered = entered_tx.clone();
            let handled = handled_tx.clone();
            let release = discard_release.clone();
            move || GatedMailboxProbe {
                id: "discarded",
                entered: entered.clone(),
                handled: handled.clone(),
                release: release.clone(),
            }
        })
        .await
        .expect("defaulted actor added");
    let drained = scope
        .add_actor_spec(
            ActorSpec::new("drained", {
                let entered = entered_tx.clone();
                let handled = handled_tx.clone();
                let release = drain_release.clone();
                move || GatedMailboxProbe {
                    id: "drained",
                    entered: entered.clone(),
                    handled: handled.clone(),
                    release: release.clone(),
                }
            })
            .mailbox_shutdown(MailboxShutdown::Drain),
        )
        .await
        .expect("overridden actor added");

    discarded
        .send(MailboxShutdownProbe::Gate)
        .await
        .expect("discard gate accepted");
    drained
        .send(MailboxShutdownProbe::Gate)
        .await
        .expect("drain gate accepted");
    let first = entered_rx.recv().await.expect("first probe entered");
    let second = entered_rx.recv().await.expect("second probe entered");
    assert_ne!(first, second);
    discarded
        .send(MailboxShutdownProbe::Count)
        .await
        .expect("discard count queued");
    drained
        .send(MailboxShutdownProbe::Count)
        .await
        .expect("drain count queued");

    let scope = running_tree.scope();
    let mut snapshots = scope.subscribe_snapshots();
    scope.request_shutdown();
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for(|snapshot| snapshot.state == SupervisorStateView::Stopping),
    )
    .await
    .expect("shutdown state published")
    .expect("snapshot stream remains open");
    discard_release.notify_one();
    drain_release.notify_one();
    shutdown_running_tree(running_tree, "mailbox shutdown default test").await;

    assert_eq!(handled_rx.recv().await, Some("drained"));
    assert!(handled_rx.try_recv().is_err());
}

#[tokio::test]
async fn runtime_added_actor_uses_non_default_mailbox_options() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let release = Arc::new(Notify::new());
    let sink = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("sink", {
                let release = release.clone();
                move || GatedDrain {
                    release: release.clone(),
                }
            })
            .mailbox(Mailbox::latest()),
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
    shutdown_running_tree(running_tree, "conflating dynamic actor test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actors_use_scope_default_and_explicit_mailbox_capacities() {
    let running_tree = DynamicTree::new()
        .default_actor_mailbox_capacity(4)
        .spawn()
        .expect("graphless runtime builds");
    let scope = support::dynamic_root(&running_tree);
    let inherited = scope
        .add_actor_spec(ActorSpec::new("inherited", Drain::<u64>::new))
        .await
        .expect("actor with the scope default is added");
    let overridden = scope
        .add_actor_spec(ActorSpec::new("sink", Drain::<u64>::new).mailbox(Mailbox::queue(9)))
        .await
        .expect("actor with a capacity override is added");

    inherited.send(1).await.expect("message accepted");
    overridden.send(1).await.expect("message accepted");
    assert_eq!(inherited.stats().mailbox_capacity, 4);
    assert_eq!(overridden.stats().mailbox_capacity, 9);

    shutdown_running_tree(running_tree, "mailbox capacity override test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_rejects_zero_mailbox_capacity() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let result = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("sink", Drain::<u64>::new).mailbox(Mailbox::queue(0)))
        .await;

    assert!(matches!(
        result,
        Err(ControlError::Rejected(BuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        )))
    ));

    shutdown_running_tree(running_tree, "zero mailbox capacity test shutdown").await;
}

#[tokio::test]
async fn runtime_added_actor_uses_its_actor_id_as_the_child_id() {
    let running_tree = DynamicTree::new()
        .spawn()
        .expect("graphless runtime builds");
    let actor = support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("local-actor", Drain::<u64>::new))
        .await
        .expect("actor is added");

    actor.send(1).await.expect("actor receives");
    let snapshot = running_tree.scope().snapshot();
    assert!(snapshot.child("local-actor").is_some());
    assert_eq!(actor.id(), "local-actor");

    shutdown_running_tree(running_tree, "child id test shutdown").await;
}

#[tokio::test]
async fn runtime_added_ref_is_distributed_to_static_actor_by_message() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = TreeBuilder::new();
    let forwarder_slot = ActorSlot::new("forwarder");
    let forwarder = forwarder_slot.actor_ref();
    builder.define(forwarder_slot, || Forwarder);
    let mut graph = builder.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let handle = graph.spawn().expect("mixed scope runtime builds");
    wait_runtime_started(&handle.scope(), "mixed runtime startup").await;
    let dynamic = handle
        .scope()
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("dynamic subtree is available");

    let sink = dynamic
        .add_actor_spec(ActorSpec::new("sink", move || Observe {
            observed: observed_tx.clone(),
        }))
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
        &handle.scope(),
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

    async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
        while let Some(message) = ctx.recv().await {
            self.target.send(message).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn runtime_added_actor_can_receive_static_ref_at_creation() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = TreeBuilder::new();
    let sink_slot = ActorSlot::new("sink");
    let sink = sink_slot.actor_ref();
    builder.define(sink_slot, move || Observe {
        observed: observed_tx.clone(),
    });
    let mut graph = builder.build();
    graph.add_subtree("dynamic", DynamicTree::new());
    let handle = graph.spawn().expect("mixed scope runtime builds");
    wait_runtime_started(&handle.scope(), "mixed runtime startup").await;
    let dynamic_scope = handle
        .scope()
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("dynamic subtree is available");

    let dynamic = dynamic_scope
        .add_actor_spec(ActorSpec::new("dynamic", move || ForwardTo {
            target: sink.clone(),
        }))
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
        &handle.scope(),
        "dynamic-to-static forwarding test shutdown",
    )
    .await;
}

#[derive(Clone)]
struct PendingActor;

impl RawActor for PendingActor {
    type Msg = ();

    async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
        pending::<()>().await;
        Ok(())
    }
}

#[tokio::test]
async fn timed_out_removal_terminates_the_typed_ref() {
    let running_tree = DynamicTree::new().spawn().expect("runtime builds");
    let actor_ref = support::dynamic_root(&running_tree)
        .add_actor_spec(
            ActorSpec::new("dynamic", || PendingActor)
                .shutdown(Shutdown::graceful_for(Duration::from_millis(20))),
        )
        .await
        .expect("actor added");

    assert!(matches!(
        support::dynamic_root(&running_tree)
            .remove_named("dynamic")
            .await,
        Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(actor_id)))
            if actor_id == "dynamic"
    ));
    assert!(
        running_tree
            .scope()
            .actor_stats()
            .iter()
            .all(|stats| stats.stats.actor_id != "dynamic"),
        "timed-out removal immediately forgets actor stats"
    );
    assert!(matches!(
        actor_ref.send(()).await,
        Err(SendError { actor_id , .. }) if actor_id == "dynamic"
    ));

    support::dynamic_root(&running_tree)
        .add_actor_spec(ActorSpec::new("dynamic", Drain::<()>::new))
        .await
        .expect("label reusable after timed-out removal");
    shutdown_running_tree(running_tree, "timed-out removal test shutdown").await;
}

#[tokio::test]
async fn ordered_tree_has_no_runtime_membership_capability() {
    let mut tree = Tree::new();
    tree.add_task_spec(TaskSpec::new("seed", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = tree.spawn().expect("valid tree");

    assert_eq!(handle.scope().kind(), kokage::observe::ScopeKind::Ordered);
    assert!(handle.scope().dynamic().is_none());

    timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("shutdown completed")
        .expect("clean shutdown");
}
