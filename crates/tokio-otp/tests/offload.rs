use std::{
    future::pending,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::{Notify, mpsc, oneshot};
use tokio_otp::{
    DrainPolicy, LiveContext, MailboxMode, OffloadDeadline, OffloadHandle, prelude::*,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

async fn wait_runtime_started(runtime: &RuntimeHandle, phase: &str) {
    tokio::time::timeout(TEST_TIMEOUT, runtime.wait_started())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn shutdown_runtime(runtime: &RuntimeHandle, phase: &str) {
    tokio::time::timeout(TEST_TIMEOUT, runtime.shutdown_and_wait())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|error| panic!("runtime failed during {phase}: {error}"));
}

async fn wait_notification(notification: &Notify, phase: &str) {
    tokio::time::timeout(TEST_TIMEOUT, notification.notified())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"));
}

async fn recv_test_event<T>(receiver: &mut mpsc::UnboundedReceiver<T>, phase: &str) -> T {
    tokio::time::timeout(TEST_TIMEOUT, receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|| panic!("channel closed while waiting for {phase}"))
}

#[derive(Debug)]
enum OutcomeMsg {
    Success(Result<u32, OffloadDeadline>),
    Timeout(Result<(), OffloadDeadline>),
    OrSuccess(u32),
    OrFallback(u32),
}

#[derive(Clone)]
struct Outcomes {
    observed: mpsc::UnboundedSender<OutcomeMsg>,
}

impl Actor for Outcomes {
    type Msg = OutcomeMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.offload(Duration::from_secs(1), async { 42 }, OutcomeMsg::Success);
        ctx.offload(
            Duration::from_millis(10),
            pending::<()>(),
            OutcomeMsg::Timeout,
        );
        ctx.offload_or(
            Duration::from_secs(1),
            async { 42 },
            0,
            OutcomeMsg::OrSuccess,
        );
        ctx.offload_or(
            Duration::from_millis(10),
            pending::<u32>(),
            7,
            OutcomeMsg::OrFallback,
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn offload_and_offload_or_deliver_total_and_fallback_outcomes() {
    let (observed, mut outcomes) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (actor_slot, _) = graph.slot("Outcomes");
    graph.define(actor_slot, move || Outcomes {
        observed: observed.clone(),
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "offload outcome runtime startup").await;

    let mut observed = Vec::new();
    for _ in 0..4 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(1), outcomes.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        observed
            .iter()
            .any(|message| matches!(message, OutcomeMsg::Success(Ok(42))))
    );
    assert!(
        observed
            .iter()
            .any(|message| matches!(message, OutcomeMsg::Timeout(Err(OffloadDeadline))))
    );
    assert!(
        observed
            .iter()
            .any(|message| matches!(message, OutcomeMsg::OrSuccess(42)))
    );
    assert!(
        observed
            .iter()
            .any(|message| matches!(message, OutcomeMsg::OrFallback(7)))
    );
    shutdown_runtime(&runtime, "offload outcome runtime shutdown").await;
}

#[derive(Debug)]
enum StaleMsg {
    Start,
    Done,
    Probe(oneshot::Sender<()>),
}

struct StaleActor {
    incarnation: usize,
    drop_started: Arc<AtomicBool>,
    drop_finished: Arc<AtomicBool>,
    release_drop: Arc<AtomicBool>,
    done: Arc<AtomicUsize>,
}

struct SlowDropFuture {
    drop_started: Arc<AtomicBool>,
    drop_finished: Arc<AtomicBool>,
    release_drop: Arc<AtomicBool>,
}

impl std::future::Future for SlowDropFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for SlowDropFuture {
    fn drop(&mut self) {
        self.drop_started.store(true, Ordering::Release);
        tokio::task::block_in_place(|| {
            while !self.release_drop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        });
        self.drop_finished.store(true, Ordering::Release);
    }
}

struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Actor for StaleActor {
    type Msg = StaleMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            StaleMsg::Start => {
                assert_eq!(self.incarnation, 0);
                ctx.offload(
                    Duration::from_secs(1),
                    SlowDropFuture {
                        drop_started: self.drop_started.clone(),
                        drop_finished: self.drop_finished.clone(),
                        release_drop: self.release_drop.clone(),
                    },
                    |_| StaleMsg::Done,
                );
                return Err(std::io::Error::other("restart after starting offload").into());
            }
            StaleMsg::Done => {
                self.done.fetch_add(1, Ordering::Relaxed);
            }
            StaleMsg::Probe(reply) => {
                let _ = reply.send(());
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offload_is_aborted_and_never_reaches_a_fresh_incarnation() {
    let constructed = Arc::new(AtomicUsize::new(0));
    let drop_started = Arc::new(AtomicBool::new(false));
    let drop_finished = Arc::new(AtomicBool::new(false));
    let release_drop = Arc::new(AtomicBool::new(false));
    let _release_on_drop = ReleaseOnDrop(release_drop.clone());
    let done = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("StaleActor");
    graph.define(actor_slot, {
        let constructed = constructed.clone();
        let drop_started = drop_started.clone();
        let drop_finished = drop_finished.clone();
        let release_drop = release_drop.clone();
        let done = done.clone();
        move || StaleActor {
            incarnation: constructed.fetch_add(1, Ordering::Relaxed),
            drop_started: drop_started.clone(),
            drop_finished: drop_finished.clone(),
            release_drop: release_drop.clone(),
            done: done.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap())
        .default_restart(RestartPolicy::OnFailure)
        .spawn()
        .unwrap();
    wait_runtime_started(&runtime, "stale-offload runtime startup").await;
    actor.send(StaleMsg::Start).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while constructed.load(Ordering::Relaxed) < 2 || !drop_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(actor.stats().outstanding_offloads, 0);
    release_drop.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !drop_finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted offload drop should finish after release");
    let (probe_tx, probe_rx) = oneshot::channel();
    actor.send(StaleMsg::Probe(probe_tx)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), probe_rx)
        .await
        .expect("fresh incarnation should process the post-abort probe")
        .expect("fresh incarnation should keep the probe sender alive");
    assert_eq!(done.load(Ordering::Relaxed), 0);
    shutdown_runtime(&runtime, "stale-offload runtime shutdown").await;
}

#[derive(Debug)]
enum AbortMsg {
    Start,
    Done,
}

#[derive(Clone)]
struct AbortActor {
    handle: Arc<Mutex<Option<OffloadHandle>>>,
    done: Arc<AtomicUsize>,
}

impl Actor for AbortActor {
    type Msg = AbortMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            AbortMsg::Start => {
                let handle =
                    ctx.offload(Duration::from_secs(1), pending::<()>(), |_| AbortMsg::Done);
                *self.handle.lock().unwrap() = Some(handle);
            }
            AbortMsg::Done => {
                self.done.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn offload_handle_aborts_and_updates_the_outstanding_gauge() {
    let handle_slot = Arc::new(Mutex::new(None));
    let done = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("AbortActor");
    graph.define(actor_slot, {
        let handle_slot = handle_slot.clone();
        let done = done.clone();
        move || AbortActor {
            handle: handle_slot.clone(),
            done: done.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "abort-handle runtime startup").await;
    actor.send(AbortMsg::Start).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while actor.stats().outstanding_offloads != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let offload = handle_slot.lock().unwrap().take().unwrap();
    offload.abort();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !offload.is_finished() || actor.stats().outstanding_offloads != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(done.load(Ordering::Relaxed), 0);
    shutdown_runtime(&runtime, "abort-handle runtime shutdown").await;
}

#[derive(Debug)]
enum ReadyAbortMsg {
    Start,
    Done,
}

struct ReadyAbortActor {
    handle: mpsc::UnboundedSender<OffloadHandle>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<()>,
}

impl Actor for ReadyAbortActor {
    type Msg = ReadyAbortMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            ReadyAbortMsg::Start => {
                let handle = ctx.offload(Duration::from_secs(1), async {}, |_| ReadyAbortMsg::Done);
                self.handle.send(handle).unwrap();
                self.release.notified().await;
            }
            ReadyAbortMsg::Done => self.observed.send(()).unwrap(),
        }
        Ok(())
    }
}

#[tokio::test]
async fn abort_suppresses_a_completion_until_the_loop_reaps_it() {
    let (handle_tx, mut handle_rx) = mpsc::unbounded_channel();
    let (observed, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("ReadyAbortActor");
    graph.define(actor_slot, {
        let release = release.clone();
        move || ReadyAbortActor {
            handle: handle_tx.clone(),
            release: release.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "ready-abort runtime startup").await;
    actor.send(ReadyAbortMsg::Start).await.unwrap();
    let offload = recv_test_event(&mut handle_rx, "ready-abort offload handle").await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !offload.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("offload future should finish before the abort");
    offload.abort();
    release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err()
    );
    shutdown_runtime(&runtime, "ready-abort runtime shutdown").await;
}

enum DrainAbortMsg {
    Start,
    Done,
}

struct DrainAbortActor {
    handle: mpsc::UnboundedSender<OffloadHandle>,
    shutdown_seen: Arc<Notify>,
}

impl Actor for DrainAbortActor {
    type Msg = DrainAbortMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            DrainAbortMsg::Start => {
                let shutdown = ctx.shutdown_token().clone();
                let shutdown_seen = self.shutdown_seen.clone();
                let handle = ctx.offload(
                    Duration::from_secs(10),
                    async move {
                        shutdown.cancelled().await;
                        shutdown_seen.notify_one();
                        pending::<()>().await;
                    },
                    |_| DrainAbortMsg::Done,
                );
                self.handle.send(handle).unwrap();
            }
            DrainAbortMsg::Done => panic!("aborted offload completion was delivered"),
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_reaps_an_offload_aborted_during_shutdown() {
    let (handle_tx, mut handle_rx) = mpsc::unbounded_channel();
    let shutdown_seen = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("DrainAbortActor");
    graph.define(actor_slot, {
        let shutdown_seen = shutdown_seen.clone();
        move || DrainAbortActor {
            handle: handle_tx.clone(),
            shutdown_seen: shutdown_seen.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "drain-abort runtime startup").await;
    actor.send(DrainAbortMsg::Start).await.unwrap();
    let offload = recv_test_event(&mut handle_rx, "drain-abort offload handle").await;
    let shutdown = tokio::spawn(async move { runtime.shutdown_and_wait().await });
    wait_notification(&shutdown_seen, "drain-abort offload observing shutdown").await;
    tokio::task::yield_now().await;
    offload.abort();
    tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("drain-abort shutdown task timed out")
        .expect("drain-abort shutdown task should join")
        .expect("drain should finish after the abort");
}

#[derive(Debug)]
enum DrainMsg {
    Start,
    Queued,
    Nested,
    Done,
}

#[derive(Clone)]
struct ShutdownActor {
    policy: DrainPolicy,
    release: Arc<Notify>,
    entered: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for ShutdownActor {
    type Msg = DrainMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            DrainMsg::Start => {
                let release = self.release.clone();
                let entered = self.entered.clone();
                ctx.offload(
                    Duration::from_secs(1),
                    async move {
                        entered.notify_one();
                        release.notified().await;
                    },
                    |_| DrainMsg::Done,
                );
            }
            DrainMsg::Queued => {
                self.observed.send("queued").unwrap();
                ctx.offload(Duration::from_secs(1), async {}, |_| DrainMsg::Nested);
            }
            DrainMsg::Nested => self.observed.send("nested").unwrap(),
            DrainMsg::Done => self.observed.send("done").unwrap(),
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        self.policy
    }
}

async fn shutdown_case(policy: DrainPolicy) -> Vec<&'static str> {
    let release = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (actor_slot, actor) = graph.slot("ShutdownActor");
    graph.define(actor_slot, {
        let release = release.clone();
        let entered = entered.clone();
        move || ShutdownActor {
            policy,
            release: release.clone(),
            entered: entered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "draining-offload runtime startup").await;
    actor.send(DrainMsg::Start).await.unwrap();
    wait_notification(&entered, "draining actor handler entry").await;
    actor.send(DrainMsg::Queued).await.unwrap();
    let shutdown = tokio::spawn(async move { runtime.shutdown_and_wait().await });
    tokio::task::yield_now().await;
    if policy == DrainPolicy::Drain {
        release.notify_waiters();
    }
    tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("draining-offload shutdown task timed out")
        .expect("draining-offload shutdown task should join")
        .expect("draining-offload runtime should shut down cleanly");

    let mut values = Vec::new();
    while let Ok(value) = receiver.try_recv() {
        values.push(value);
    }
    values
}

#[tokio::test]
async fn drain_processes_a_full_mailbox_and_offload_completion() {
    let observed = shutdown_case(DrainPolicy::Drain).await;
    assert_eq!(observed.first(), Some(&"queued"));
    assert!(observed.contains(&"nested"));
    assert!(observed.contains(&"done"));
}

#[tokio::test]
async fn discard_aborts_offloads_at_stop_initiation() {
    assert!(!shutdown_case(DrainPolicy::Discard).await.contains(&"done"));
}

#[derive(Debug)]
enum BackpressureMsg {
    Start,
    Fill,
    Done,
}

#[derive(Clone)]
struct BackpressureActor {
    handler_release: Arc<Notify>,
    offload_release: Arc<Notify>,
    offload_registered: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for BackpressureActor {
    type Msg = BackpressureMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            BackpressureMsg::Start => {
                let release = self.offload_release.clone();
                ctx.offload(
                    Duration::from_secs(1),
                    async move { release.notified().await },
                    |_| BackpressureMsg::Done,
                );
                self.offload_registered.notify_one();
                self.handler_release.notified().await;
            }
            BackpressureMsg::Fill => self.observed.send("fill").unwrap(),
            BackpressureMsg::Done => self.observed.send("done").unwrap(),
        }
        Ok(())
    }
}

#[tokio::test]
async fn offload_completion_bypasses_mailbox_backpressure() {
    let handler_release = Arc::new(Notify::new());
    let offload_release = Arc::new(Notify::new());
    let offload_registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (actor_slot, actor) = graph.slot("BackpressureActor");
    graph.define(actor_slot, {
        let handler_release = handler_release.clone();
        let offload_release = offload_release.clone();
        let offload_registered = offload_registered.clone();
        move || BackpressureActor {
            handler_release: handler_release.clone(),
            offload_release: offload_release.clone(),
            offload_registered: offload_registered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "backpressure runtime startup").await;
    actor.send(BackpressureMsg::Start).await.unwrap();
    wait_notification(&offload_registered, "backpressure offload registration").await;
    actor.send(BackpressureMsg::Fill).await.unwrap();
    offload_release.notify_one();
    tokio::task::yield_now().await;
    let stats = actor.stats();
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.outstanding_offloads, 1);
    assert_eq!(stats.messages_accepted, 2);
    handler_release.notify_one();
    // The completion no longer queues behind the mailbox, so the two markers
    // race and only their set is deterministic.
    let mut received = [
        recv_test_event(&mut receiver, "first backpressure marker").await,
        recv_test_event(&mut receiver, "second backpressure marker").await,
    ];
    received.sort_unstable();
    assert_eq!(received, ["done", "fill"]);
    shutdown_runtime(&runtime, "backpressure runtime shutdown").await;
}

#[tokio::test]
async fn offload_completion_does_not_participate_in_conflation() {
    let handler_release = Arc::new(Notify::new());
    let offload_release = Arc::new(Notify::new());
    let offload_registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot_with(
        "conflating-offload",
        ActorOptions::new().mailbox(MailboxMode::conflate()),
    );
    graph.define(actor_slot, {
        let handler_release = handler_release.clone();
        let offload_release = offload_release.clone();
        let offload_registered = offload_registered.clone();
        move || BackpressureActor {
            handler_release: handler_release.clone(),
            offload_release: offload_release.clone(),
            offload_registered: offload_registered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "conflating completion runtime startup").await;
    actor.send(BackpressureMsg::Start).await.unwrap();
    wait_notification(
        &offload_registered,
        "conflating completion offload registration",
    )
    .await;
    actor.send(BackpressureMsg::Fill).await.unwrap();
    offload_release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(actor.stats().messages_conflated, 0);
    assert_eq!(actor.stats().mailbox_depth, 1);
    assert_eq!(actor.stats().outstanding_offloads, 1);
    handler_release.notify_one();
    // Nothing is conflated away now, so both markers arrive and only their set
    // is deterministic.
    let mut received = [
        recv_test_event(&mut receiver, "first conflating completion marker").await,
        recv_test_event(&mut receiver, "second conflating completion marker").await,
    ];
    received.sort_unstable();
    assert_eq!(received, ["done", "fill"]);
    assert!(receiver.try_recv().is_err());
    shutdown_runtime(&runtime, "conflating completion runtime shutdown").await;
}

#[derive(Debug)]
enum DeadlineDrainMsg {
    Start,
    Done(Result<(), OffloadDeadline>),
}

#[derive(Clone)]
struct DeadlineDrainActor {
    registered: Arc<Notify>,
    observed: mpsc::UnboundedSender<Result<(), OffloadDeadline>>,
}

impl Actor for DeadlineDrainActor {
    type Msg = DeadlineDrainMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            DeadlineDrainMsg::Start => {
                ctx.offload(
                    Duration::from_millis(100),
                    pending::<()>(),
                    DeadlineDrainMsg::Done,
                );
                self.registered.notify_one();
            }
            DeadlineDrainMsg::Done(outcome) => self.observed.send(outcome).unwrap(),
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_waits_for_offload_deadline_and_handles_its_completion() {
    let registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("DeadlineDrainActor");
    graph.define(actor_slot, {
        let registered = registered.clone();
        move || DeadlineDrainActor {
            registered: registered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    wait_runtime_started(&runtime, "deadline-drain runtime startup").await;
    actor.send(DeadlineDrainMsg::Start).await.unwrap();
    wait_notification(&registered, "deadline-drain offload registration").await;
    let shutdown = tokio::spawn(async move { runtime.shutdown_and_wait().await });
    let outcome = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, Err(OffloadDeadline)));
    tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("deadline-drain shutdown task timed out")
        .expect("deadline-drain shutdown task should join")
        .expect("deadline-drain runtime should shut down cleanly");
}

struct RawCompletion {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for RawCompletion {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.offload(Duration::from_secs(1), async {}, |_| "done");
        let message = ctx.recv().await.expect("offload completion");
        self.observed.send(message).unwrap();
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn raw_actor_recv_reaps_offload_completions() {
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (actor_slot, _) = graph.slot("RawCompletion");
    graph.define(actor_slot, move || RawCompletion {
        observed: observed.clone(),
    });
    let runtime = OrderedTree::graph(graph.build().unwrap()).spawn().unwrap();
    assert_eq!(
        recv_test_event(&mut receiver, "raw actor offload completion").await,
        "done"
    );
    shutdown_runtime(&runtime, "raw-completion runtime shutdown").await;
}

enum PanicMsg {
    Start,
}

struct PanicActor;

impl Actor for PanicActor {
    type Msg = PanicMsg;

    async fn handle(
        &mut self,
        _message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        ctx.offload(
            Duration::from_secs(1),
            async { panic!("offload panic") },
            |_| PanicMsg::Start,
        );
        Ok(())
    }
}

#[tokio::test]
async fn offload_panic_fails_the_actor_and_is_supervised() {
    let constructed = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let (actor_slot, actor) = graph.slot("PanicActor");
    graph.define(actor_slot, {
        let constructed = constructed.clone();
        move || {
            constructed.fetch_add(1, Ordering::Relaxed);
            PanicActor
        }
    });
    let runtime = OrderedTree::graph(graph.build().unwrap())
        .default_restart(RestartPolicy::OnFailure)
        .spawn()
        .unwrap();
    wait_runtime_started(&runtime, "offload-panic runtime startup").await;
    actor.send(PanicMsg::Start).await.unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while constructed.load(Ordering::Relaxed) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor restarted after offload panic");
    shutdown_runtime(&runtime, "offload-panic runtime shutdown").await;
}
