use std::{
    future::{Future, pending, poll_fn},
    io,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_otp::{
    Actor, ActorContext, ActorOptions, ActorRef, ActorResult, ActorRunError, BoxError, CallError,
    DEFAULT_SHUTDOWN_BOUND, DrainPolicy, Graph, GraphBuildError, GraphBuilder, LiveContext,
    MessageContext, RawActor, Reply, RestartPolicy, RunnableActor, SendError, StartContext,
    StopContext, TryRecvError, prelude::Continue,
};
use tokio_util::sync::CancellationToken;

fn runnable(graph: &Graph, label: &str) -> RunnableActor {
    graph
        .actors()
        .iter()
        .find(|actor| actor.label() == label)
        .expect("actor exists")
        .clone()
}

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
        Ok(Continue)
    }
}

fn start_graph(
    graph: &Graph,
) -> (
    CancellationToken,
    JoinHandle<Vec<Result<(), ActorRunError>>>,
) {
    let stop = CancellationToken::new();
    let tasks = graph
        .actors()
        .iter()
        .cloned()
        .map(|actor| {
            let stop = stop.clone();
            tokio::spawn(async move {
                actor
                    .run_until(
                        stop.cancelled(),
                        RestartPolicy::Never,
                        DEFAULT_SHUTDOWN_BOUND,
                    )
                    .await
            })
        })
        .collect::<Vec<_>>();
    let task = tokio::spawn(async move {
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await.expect("actor task joined"));
        }
        results
    });
    (stop, task)
}

async fn stop_graph(stop: CancellationToken, task: JoinHandle<Vec<Result<(), ActorRunError>>>) {
    stop.cancel();
    wait_graph(task).await;
}

async fn wait_graph(task: JoinHandle<Vec<Result<(), ActorRunError>>>) {
    let results = timeout(Duration::from_secs(1), task)
        .await
        .expect("graph stopped in time")
        .expect("graph task joined");
    assert!(
        results.iter().all(Result::is_ok),
        "all actors stopped cleanly: {results:?}"
    );
}

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>, message: &str) -> T {
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect(message)
        .expect("message observed")
}

struct Request(&'static str);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Job {
    payload: &'static str,
}

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<Job>,
}

impl RawActor for Frontend {
    type Msg = Request;

    async fn run(&mut self, mut ctx: ActorContext<Request>) -> ActorResult {
        while let Some(Request(payload)) = ctx.recv().await {
            self.worker.send(Job { payload }).await?;
        }
        Ok(Continue)
    }
}

#[derive(Clone)]
struct Worker {
    seen: mpsc::UnboundedSender<Job>,
}

impl RawActor for Worker {
    type Msg = Job;

    async fn run(&mut self, mut ctx: ActorContext<Job>) -> ActorResult {
        while let Some(job) = ctx.recv().await {
            self.seen.send(job).expect("receiver alive");
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn typed_pipeline_end_to_end() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    let (worker_slot, worker) = builder.slot::<Job>("worker");
    let frontend = builder.actor("frontend", move || Frontend {
        worker: worker.clone(),
    });
    builder.define(worker_slot, move || Worker {
        seen: seen_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    frontend.send(Request("hello")).await.expect("send");

    assert_eq!(
        recv(&mut seen_rx, "message arrived").await,
        Job { payload: "hello" }
    );

    stop_graph(stop, task).await;
}

#[derive(Clone)]
struct Echo {
    seen: mpsc::UnboundedSender<u32>,
}

impl RawActor for Echo {
    type Msg = u32;

    async fn run(&mut self, mut ctx: ActorContext<u32>) -> ActorResult {
        while let Some(n) = ctx.recv().await {
            self.seen.send(n).expect("receiver alive");
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn send_to_never_started_graph_waits_until_graph_runs() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    let echo = builder.actor("echo", move || Echo {
        seen: seen_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let echo_for_send = echo.clone();
    let mut send = Box::pin(async move { echo_for_send.send(7).await });
    let first_poll = poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await;
    assert!(
        first_poll.is_pending(),
        "send should wait until the graph binds the actor mailbox"
    );
    let send_task = tokio::spawn(send);

    let (stop, task) = start_graph(&graph);
    assert_eq!(recv(&mut seen_rx, "message after graph start").await, 7);
    send_task
        .await
        .expect("send task joined")
        .expect("send completed after graph start");
    stop_graph(stop, task).await;
}

#[tokio::test]
async fn try_send_reports_unbound_and_terminated_states() {
    let mut builder = GraphBuilder::new();
    let worker = builder.actor("worker", Drain::<()>::new);
    let graph = builder.build().expect("valid graph");

    assert!(matches!(
        worker.try_send(()),
        Err(SendError::ActorNotRunning { actor_id , .. }) if actor_id == "worker"
    ));

    let (stop, task) = start_graph(&graph);
    stop_graph(stop, task).await;

    assert!(matches!(
        worker.try_send(()),
        Err(SendError::ActorTerminated { actor_id , .. }) if actor_id == "worker"
    ));
}

enum CounterMsg {
    Add(u64),
    Total(Reply<u64>),
}

#[derive(Clone)]
struct Counter;

impl RawActor for Counter {
    type Msg = CounterMsg;

    async fn run(&mut self, mut ctx: ActorContext<CounterMsg>) -> ActorResult {
        let mut total = 0;
        while let Some(message) = ctx.recv().await {
            match message {
                CounterMsg::Add(n) => total += n,
                CounterMsg::Total(reply) => reply.send(total),
            }
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn call_reply_roundtrip() {
    let mut builder = GraphBuilder::new();
    let counter = builder.actor("counter", || Counter);
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    counter.send(CounterMsg::Add(1)).await.expect("send");
    counter.send(CounterMsg::Add(2)).await.expect("send");

    assert_eq!(
        counter
            .call(Duration::from_secs(1), CounterMsg::Total)
            .await
            .expect("call"),
        3
    );

    stop_graph(stop, task).await;
}

enum HandlerCounterMsg {
    Add(u64),
    Total(Reply<u64>),
}

#[derive(Clone)]
struct HandlerCounter {
    total: u64,
}

impl Actor for HandlerCounter {
    type Msg = HandlerCounterMsg;

    async fn handle(
        &mut self,
        message: HandlerCounterMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            HandlerCounterMsg::Add(n) => self.total += n,
            HandlerCounterMsg::Total(reply) => reply.send(self.total),
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn handler_receives_messages_in_order_and_preserves_state() {
    let mut builder = GraphBuilder::new();
    let counter = builder.actor("counter", || HandlerCounter { total: 0 });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    counter
        .send(HandlerCounterMsg::Add(2))
        .await
        .expect("first add sent");
    counter
        .send(HandlerCounterMsg::Add(3))
        .await
        .expect("second add sent");

    assert_eq!(
        counter
            .call(Duration::from_secs(1), HandlerCounterMsg::Total)
            .await
            .expect("call"),
        5
    );

    stop_graph(stop, task).await;
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LifecycleEvent {
    Started,
    Handled,
    Stopped,
}

#[derive(Clone)]
struct LifecycleHandler {
    events: mpsc::UnboundedSender<LifecycleEvent>,
}

impl Actor for LifecycleHandler {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.events
            .send(LifecycleEvent::Started)
            .expect("receiver alive");
        Ok(Continue)
    }

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.events
            .send(LifecycleEvent::Handled)
            .expect("receiver alive");
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events
            .send(LifecycleEvent::Stopped)
            .expect("receiver alive");
        Ok(())
    }
}

#[tokio::test]
async fn handler_on_start_runs_before_first_message() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", move || LifecycleHandler {
        events: events_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    assert_eq!(
        recv(&mut events_rx, "handler started").await,
        LifecycleEvent::Started
    );

    actor.send(()).await.expect("message sent");
    assert_eq!(
        recv(&mut events_rx, "handler handled message").await,
        LifecycleEvent::Handled
    );

    stop_graph(stop, task).await;
    assert_eq!(
        recv(&mut events_rx, "handler stopped").await,
        LifecycleEvent::Stopped
    );
}

#[derive(Clone)]
struct FailingStartHandler {
    events: mpsc::UnboundedSender<LifecycleEvent>,
}

impl Actor for FailingStartHandler {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.events
            .send(LifecycleEvent::Started)
            .expect("receiver alive");
        Err(io::Error::other("start failed").into())
    }

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.events
            .send(LifecycleEvent::Handled)
            .expect("receiver alive");
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events
            .send(LifecycleEvent::Stopped)
            .expect("receiver alive");
        Ok(())
    }
}

#[tokio::test]
async fn handler_on_start_error_fails_actor_run_without_handle_or_stop() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    builder.actor("worker", move || FailingStartHandler {
        events: events_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let result = runnable(&graph, "worker")
        .run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await;
    assert!(matches!(
        result,
        Err(ActorRunError::Failed { actor_id, .. }) if actor_id == "worker"
    ));
    assert_eq!(
        recv(&mut events_rx, "handler started").await,
        LifecycleEvent::Started
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[derive(Clone)]
struct FailingHandler;

impl Actor for FailingHandler {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Err(io::Error::other("handle failed").into())
    }
}

#[tokio::test]
async fn handler_error_fails_the_actor_run() {
    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", || FailingHandler);
    let graph = builder.build().expect("valid graph");

    let worker = runnable(&graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });
    actor.send(()).await.expect("message sent");

    let result = timeout(Duration::from_secs(1), task)
        .await
        .expect("graph stopped in time")
        .expect("graph task joined");
    assert!(matches!(
        result,
        Err(ActorRunError::Failed { actor_id, .. }) if actor_id == "worker"
    ));
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GateEvent {
    Handled(u32),
    Stopped(u32),
}

enum GateMsg {
    Hold,
    Stop,
    Add(u32),
    Total(Reply<u32>),
}

#[derive(Clone)]
struct GateHandler {
    total: u32,
    policy: DrainPolicy,
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    events: mpsc::UnboundedSender<GateEvent>,
}

impl Actor for GateHandler {
    type Msg = GateMsg;

    async fn handle(
        &mut self,
        message: GateMsg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            GateMsg::Hold => {
                self.started.send(()).expect("receiver alive");
                self.release.notified().await;
            }
            GateMsg::Stop => {
                self.started.send(()).expect("receiver alive");
                ctx.continue_with(GateMsg::Add(99));
                self.release.notified().await;
                return Ok(tokio_otp::prelude::Stop);
            }
            GateMsg::Add(n) => {
                self.total += n;
                self.events
                    .send(GateEvent::Handled(n))
                    .expect("receiver alive");
            }
            GateMsg::Total(reply) => reply.send(self.total),
        }
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events
            .send(GateEvent::Stopped(self.total))
            .expect("receiver alive");
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        self.policy
    }
}

#[tokio::test]
async fn handler_stop_with_discard_drops_mailbox_and_continuations_then_runs_on_stop() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", {
        let release = release.clone();
        move || GateHandler {
            total: 0,
            policy: DrainPolicy::Discard,
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let worker = runnable(&graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });

    actor.send(GateMsg::Stop).await.expect("stop sent");
    recv(&mut started_rx, "handler entered stop").await;
    actor.send(GateMsg::Add(1)).await.expect("add queued");
    let call_task = queued_total_call(actor).await;
    release.notify_one();

    assert!(matches!(
        call_task.await.expect("call task joined"),
        Err(CallError::ReplyDropped { actor_id, .. }) if actor_id == "worker"
    ));
    assert!(task.await.expect("actor task joined").is_ok());
    assert_eq!(
        recv(&mut events_rx, "handler stopped").await,
        GateEvent::Stopped(0)
    );
    assert!(events_rx.try_recv().is_err());
}

#[tokio::test]
async fn handler_stop_with_drain_handles_mailbox_but_drops_continuations() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", {
        let release = release.clone();
        move || GateHandler {
            total: 0,
            policy: DrainPolicy::Drain,
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let worker = runnable(&graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_until(
                pending::<()>(),
                RestartPolicy::Never,
                DEFAULT_SHUTDOWN_BOUND,
            )
            .await
    });

    actor.send(GateMsg::Stop).await.expect("stop sent");
    recv(&mut started_rx, "handler entered stop").await;
    actor.send(GateMsg::Add(1)).await.expect("add queued");
    let call_task = queued_total_call(actor).await;
    release.notify_one();

    assert_eq!(
        call_task.await.expect("call task joined").expect("reply"),
        1
    );
    assert!(task.await.expect("actor task joined").is_ok());
    assert_eq!(
        recv(&mut events_rx, "drained message").await,
        GateEvent::Handled(1)
    );
    assert_eq!(
        recv(&mut events_rx, "handler stopped").await,
        GateEvent::Stopped(1)
    );
    assert!(events_rx.try_recv().is_err());
}

async fn queued_total_call(actor: ActorRef<GateMsg>) -> JoinHandle<Result<u32, CallError>> {
    let (queued_tx, queued_rx) = oneshot::channel();
    let call_task = tokio::spawn(async move {
        actor
            .call(Duration::from_secs(1), |reply| {
                queued_tx.send(()).expect("receiver alive");
                GateMsg::Total(reply)
            })
            .await
    });
    queued_rx.await.expect("call message constructed");
    call_task
}

#[tokio::test]
async fn handler_discard_drops_queued_messages_and_call_reply() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", {
        let release = release.clone();
        move || GateHandler {
            total: 0,
            policy: DrainPolicy::Discard,
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    actor.send(GateMsg::Hold).await.expect("hold sent");
    recv(&mut started_rx, "handler entered hold").await;

    actor.send(GateMsg::Add(1)).await.expect("add queued");
    let call_task = queued_total_call(actor.clone()).await;

    stop.cancel();
    release.notify_one();
    wait_graph(task).await;

    assert!(matches!(
        call_task.await.expect("call task joined"),
        Err(CallError::ReplyDropped { actor_id , .. }) if actor_id == "worker"
    ));
    assert_eq!(
        recv(&mut events_rx, "handler stopped").await,
        GateEvent::Stopped(0)
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn handler_drain_handles_queued_messages_and_replies_before_stop() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", {
        let release = release.clone();
        move || GateHandler {
            total: 0,
            policy: DrainPolicy::Drain,
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    actor.send(GateMsg::Hold).await.expect("hold sent");
    recv(&mut started_rx, "handler entered hold").await;

    actor.send(GateMsg::Add(1)).await.expect("first add queued");
    actor
        .send(GateMsg::Add(2))
        .await
        .expect("second add queued");
    let call_task = queued_total_call(actor.clone()).await;

    stop.cancel();
    release.notify_one();

    assert_eq!(
        call_task.await.expect("call task joined").expect("reply"),
        3
    );
    wait_graph(task).await;

    assert_eq!(
        recv(&mut events_rx, "first drained message").await,
        GateEvent::Handled(1)
    );
    assert_eq!(
        recv(&mut events_rx, "second drained message").await,
        GateEvent::Handled(2)
    );
    assert_eq!(
        recv(&mut events_rx, "handler stopped").await,
        GateEvent::Stopped(3)
    );
}

enum TryDrainMsg {
    Start,
    Value(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TryDrainEvent {
    Drained(u32),
    Empty,
}

#[derive(Clone)]
struct TryDrainActor {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    events: mpsc::UnboundedSender<TryDrainEvent>,
}

impl RawActor for TryDrainActor {
    type Msg = TryDrainMsg;

    async fn run(&mut self, mut ctx: ActorContext<TryDrainMsg>) -> ActorResult {
        match ctx.recv().await {
            Some(TryDrainMsg::Start) => self.started.send(()).expect("receiver alive"),
            Some(TryDrainMsg::Value(_)) => panic!("expected start message"),
            None => panic!("shutdown before start"),
        }

        self.release.notified().await;
        assert!(ctx.recv().await.is_none());

        loop {
            match ctx.try_recv() {
                Ok(TryDrainMsg::Value(value)) => self
                    .events
                    .send(TryDrainEvent::Drained(value))
                    .expect("receiver alive"),
                Ok(TryDrainMsg::Start) => panic!("unexpected start message"),
                Err(TryRecvError::Empty) => {
                    self.events
                        .send(TryDrainEvent::Empty)
                        .expect("receiver alive");
                    break;
                }
                Err(TryRecvError::Disconnected) => panic!("mailbox disconnected"),
            }
        }

        Ok(Continue)
    }
}

#[tokio::test]
async fn try_recv_drains_messages_after_shutdown_recv_returns_none() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let mut builder = GraphBuilder::new();
    let actor = builder.actor("worker", {
        let release = release.clone();
        move || TryDrainActor {
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    actor.send(TryDrainMsg::Start).await.expect("start sent");
    recv(&mut started_rx, "actor started").await;
    actor
        .send(TryDrainMsg::Value(1))
        .await
        .expect("first value queued");
    actor
        .send(TryDrainMsg::Value(2))
        .await
        .expect("second value queued");

    stop.cancel();
    release.notify_one();
    wait_graph(task).await;

    assert_eq!(
        recv(&mut events_rx, "first drained value").await,
        TryDrainEvent::Drained(1)
    );
    assert_eq!(
        recv(&mut events_rx, "second drained value").await,
        TryDrainEvent::Drained(2)
    );
    assert_eq!(
        recv(&mut events_rx, "empty mailbox observed").await,
        TryDrainEvent::Empty
    );
}

struct Ball {
    bounces_left: u32,
}

#[derive(Clone)]
struct Paddle {
    other: ActorRef<Ball>,
    done: mpsc::UnboundedSender<()>,
}

impl RawActor for Paddle {
    type Msg = Ball;

    async fn run(&mut self, mut ctx: ActorContext<Ball>) -> ActorResult {
        while let Some(ball) = ctx.recv().await {
            if ball.bounces_left == 0 {
                self.done.send(()).expect("receiver alive");
            } else {
                self.other
                    .send(Ball {
                        bounces_left: ball.bounces_left - 1,
                    })
                    .await?;
            }
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn cyclic_wiring_via_slot() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    let (pong_slot, pong) = builder.slot::<Ball>("pong");
    let ping = builder.actor("ping", {
        let done_tx = done_tx.clone();
        move || Paddle {
            other: pong.clone(),
            done: done_tx.clone(),
        }
    });
    builder.define(pong_slot, {
        let ping = ping.clone();
        move || Paddle {
            other: ping.clone(),
            done: done_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let (stop, task) = start_graph(&graph);
    ping.send(Ball { bounces_left: 5 }).await.expect("serve");
    recv(&mut done_rx, "rally finished").await;
    stop_graph(stop, task).await;
}

#[test]
fn graph_preserves_explicit_name() {
    let mut builder = GraphBuilder::new();
    builder.name("orders");
    builder.actor("worker", Drain::<()>::new);
    let graph = builder.build().expect("valid graph");

    assert_eq!(graph.name(), "orders");
}

#[test]
fn graph_generates_unique_anonymous_names() {
    let mut first = GraphBuilder::new();
    first.actor("worker", Drain::<()>::new);
    let first = first.build().expect("valid graph");

    let mut second = GraphBuilder::new();
    second.actor("worker", Drain::<()>::new);
    let second = second.build().expect("valid graph");

    assert_ne!(first.name(), second.name());
    assert!(first.name().starts_with("graph-"));
    assert!(second.name().starts_with("graph-"));
}

#[test]
fn build_rejects_invalid_graph_definitions() {
    let mut missing = GraphBuilder::new();
    let (_slot, _ghost) = missing.slot::<u32>("ghost");
    assert!(matches!(
        missing.build(),
        Err(GraphBuildError::MissingActor { actor_id , .. }) if actor_id == "ghost"
    ));

    let mut duplicate = GraphBuilder::new();
    duplicate.actor("worker", Drain::<u32>::new);
    duplicate.actor("worker", Drain::<u32>::new);
    assert!(matches!(
        duplicate.build(),
        Err(GraphBuildError::DuplicateActorId { actor_id , .. }) if actor_id == "worker"
    ));

    let empty = GraphBuilder::new();
    assert!(matches!(empty.build(), Err(GraphBuildError::EmptyGraph)));

    let mut empty_name = GraphBuilder::new();
    empty_name.name("");
    empty_name.actor("worker", Drain::<()>::new);
    assert!(matches!(
        empty_name.build(),
        Err(GraphBuildError::InvalidConfig(
            "graph name must not be empty"
        ))
    ));

    let mut zero_capacity = GraphBuilder::new();
    zero_capacity.mailbox_capacity(0);
    zero_capacity.actor("worker", Drain::<()>::new);
    assert!(matches!(
        zero_capacity.build(),
        Err(GraphBuildError::InvalidConfig(
            "mailbox capacity must be non-zero"
        ))
    ));

    let mut zero_actor_capacity = GraphBuilder::new();
    zero_actor_capacity.actor_with_options(
        "worker",
        Drain::<()>::new,
        ActorOptions::new().mailbox_capacity(0),
    );
    assert!(matches!(
        zero_actor_capacity.build(),
        Err(GraphBuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        ))
    ));
}

#[tokio::test]
async fn an_actor_can_depart_from_the_graph_wide_mailbox_capacity() {
    let mut builder = GraphBuilder::new();
    builder.mailbox_capacity(2);
    let shallow = builder.actor("shallow", Drain::<()>::new);
    let deep = builder.actor_with_options(
        "deep",
        Drain::<()>::new,
        ActorOptions::new().mailbox_capacity(9),
    );
    let graph = builder.build().expect("graph builds");

    // Capacity is a property of the bound mailbox, so it is observable only
    // once an incarnation is running.
    let stop = CancellationToken::new();
    let tasks: Vec<_> = graph
        .actors()
        .iter()
        .cloned()
        .map(|actor| {
            let stop = stop.clone();
            tokio::spawn(async move {
                actor
                    .run_until(
                        stop.cancelled(),
                        RestartPolicy::Never,
                        DEFAULT_SHUTDOWN_BOUND,
                    )
                    .await
            })
        })
        .collect();

    timeout(Duration::from_secs(5), async {
        while shallow.stats().mailbox_capacity == 0 || deep.stats().mailbox_capacity == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both actors bind");

    assert_eq!(shallow.stats().mailbox_capacity, 2);
    assert_eq!(deep.stats().mailbox_capacity, 9);

    stop.cancel();
    for task in tasks {
        task.await.expect("joined").expect("clean stop");
    }
}

#[derive(Clone)]
struct Fail;

impl RawActor for Fail {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        Err(io::Error::other("boom").into())
    }
}

#[tokio::test]
async fn actor_error_fails_its_run() {
    let mut builder = GraphBuilder::new();
    builder.actor("healthy", Drain::<()>::new);
    builder.actor("bad", || Fail);
    let graph = builder.build().expect("valid graph");

    let result = runnable(&graph, "bad")
        .run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await;
    assert!(matches!(
        result,
        Err(ActorRunError::Failed { actor_id, .. }) if actor_id == "bad"
    ));
}

#[derive(Clone)]
struct Quit;

impl RawActor for Quit {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        Ok(Continue)
    }
}

#[tokio::test]
async fn early_clean_exit_is_a_clean_actor_run() {
    let mut builder = GraphBuilder::new();
    builder.actor("quitter", || Quit);
    let graph = builder.build().expect("valid graph");

    runnable(&graph, "quitter")
        .run_until(
            pending::<()>(),
            RestartPolicy::Never,
            DEFAULT_SHUTDOWN_BOUND,
        )
        .await
        .expect("clean early exit is ordinary completion");
}

#[tokio::test]
async fn standalone_shutdown_bound_aborts_uncooperative_actor() {
    struct LiveGuard(Arc<AtomicBool>);

    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    #[derive(Clone)]
    struct Stubborn {
        started: Arc<Notify>,
        live: Arc<AtomicBool>,
    }

    impl RawActor for Stubborn {
        type Msg = ();

        async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
            self.live.store(true, Ordering::Release);
            let _guard = LiveGuard(self.live.clone());
            self.started.notify_one();
            pending::<()>().await;
            Ok(Continue)
        }
    }

    let started = Arc::new(Notify::new());
    let live = Arc::new(AtomicBool::new(false));
    let mut builder = GraphBuilder::new();
    builder.actor("worker", {
        let started = started.clone();
        let live = live.clone();
        move || Stubborn {
            started: started.clone(),
            live: live.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let worker = runnable(&graph, "worker");
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move {
            worker
                .run_until(
                    stop.cancelled(),
                    RestartPolicy::Never,
                    Duration::from_millis(50),
                )
                .await
        }
    });

    started.notified().await;
    stop.cancel();
    assert!(matches!(
        task.await.expect("actor task joined"),
        Err(ActorRunError::ShutdownTimedOut { actor_id, .. }) if actor_id == "worker"
    ));
    assert!(
        !live.load(Ordering::Acquire),
        "uncooperative actor should be aborted before graph shutdown returns"
    );
}

#[test]
fn actors_are_exposed_in_declaration_order() {
    let mut builder = GraphBuilder::new();
    builder.actor("first", Drain::<()>::new);
    let (slot, _second) = builder.slot::<u32>("second");
    builder.actor("third", Drain::<()>::new);
    builder.define(slot, Drain::<u32>::new);
    let graph = builder.build().expect("valid graph");

    let labels: Vec<&str> = graph.actors().iter().map(|actor| actor.label()).collect();
    assert_eq!(labels, ["first", "second", "third"]);
}

#[tokio::test]
async fn send_to_dropped_never_started_graph_returns_actor_terminated() {
    let mut builder = GraphBuilder::new();
    let echo = builder.actor("echo", Drain::<u32>::new);
    let graph = builder.build().expect("valid graph");

    drop(graph);
    assert!(matches!(
        echo.send(1).await,
        Err(SendError::ActorTerminated { actor_id , .. }) if actor_id == "echo"
    ));
}

mod runnable_actor {
    use std::{
        future::{Future, pending, poll_fn},
        marker::PhantomData,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };

    use tokio::{
        sync::{Notify, mpsc},
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tokio_otp::{
        Actor, ActorContext, ActorOptions, ActorRef, ActorResult, ActorRunError, BoxError,
        ControlError, DEFAULT_SHUTDOWN_BOUND, DrainPolicy, DynamicActorOptions, Graph,
        GraphBuilder, MessageContext, MessageSize, RawActor, RestartPolicy, RunnableActor,
        RunnableActorBuilder, SendError, StartContext, SupervisionTree, prelude::Continue,
    };
    use tokio_util::sync::CancellationToken;

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
            Ok(Continue)
        }
    }

    #[derive(Clone)]
    struct StartSignallingDrain {
        started: mpsc::UnboundedSender<()>,
    }

    impl RawActor for StartSignallingDrain {
        type Msg = ();

        async fn run(&mut self, mut ctx: ActorContext<()>) -> ActorResult {
            self.started.send(()).expect("start receiver alive");
            while ctx.recv().await.is_some() {}
            Ok(Continue)
        }
    }

    #[derive(Clone)]
    struct NeverStops;

    impl RawActor for NeverStops {
        type Msg = ();

        async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
            pending::<ActorResult>().await
        }
    }

    #[derive(Clone)]
    struct StopsOnShutdown;

    impl RawActor for StopsOnShutdown {
        type Msg = ();

        async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
            ctx.shutdown_token().cancelled().await;
            Ok(Continue)
        }
    }

    #[derive(Clone)]
    struct GatedDrain {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        received: mpsc::UnboundedSender<()>,
    }

    #[derive(Debug)]
    struct SizedPayload(Vec<u8>);

    impl MessageSize for SizedPayload {
        fn size_hint(&self) -> usize {
            self.0.len()
        }
    }

    #[derive(Clone)]
    struct GatedSizedDrain {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        received: mpsc::UnboundedSender<()>,
    }

    impl RawActor for GatedSizedDrain {
        type Msg = SizedPayload;

        async fn run(&mut self, mut ctx: ActorContext<SizedPayload>) -> ActorResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            while let Some(_message) = ctx.recv().await {
                self.received.send(()).expect("receiver alive");
            }
            Ok(Continue)
        }
    }

    #[test]
    fn failed_sized_registration_returns_a_sized_detached_ref() {
        let mut builder = GraphBuilder::new();
        builder.actor_with_options(
            "worker",
            Drain::<SizedPayload>::new,
            ActorOptions::new().message_size(),
        );
        let detached = builder.actor_with_options(
            "worker",
            Drain::<SizedPayload>::new,
            ActorOptions::new().message_size(),
        );

        assert_eq!(detached.stats().message_bytes_accepted, Some(0));

        let mut slot_builder = GraphBuilder::new();
        slot_builder
            .slot_with_options::<SizedPayload>("worker", ActorOptions::new().message_size());
        let (_detached_slot, detached) = slot_builder
            .slot_with_options::<SizedPayload>("worker", ActorOptions::new().message_size());
        assert_eq!(detached.stats().message_bytes_accepted, Some(0));
        assert!(matches!(
            detached.try_send(SizedPayload(Vec::new())),
            Err(SendError::ActorTerminated { actor_id , .. }) if actor_id == "worker"
        ));
    }

    impl RawActor for GatedDrain {
        type Msg = u32;

        async fn run(&mut self, mut ctx: ActorContext<u32>) -> ActorResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            while let Some(_message) = ctx.recv().await {
                self.received.send(()).expect("receiver alive");
            }
            Ok(Continue)
        }
    }

    fn start_actor(
        actor: RunnableActor,
    ) -> (CancellationToken, JoinHandle<Result<(), ActorRunError>>) {
        start_actor_with_policy(actor, RestartPolicy::Never)
    }

    fn start_actor_with_policy(
        actor: RunnableActor,
        restart: RestartPolicy,
    ) -> (CancellationToken, JoinHandle<Result<(), ActorRunError>>) {
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                actor
                    .run_until(stop.cancelled(), restart, DEFAULT_SHUTDOWN_BOUND)
                    .await
            }
        });
        (stop, task)
    }

    async fn stop_actor(
        stop: CancellationToken,
        task: JoinHandle<Result<(), ActorRunError>>,
    ) -> Result<(), ActorRunError> {
        stop.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("actor stopped in time")
            .expect("actor task joined")
    }

    fn single_actor(graph: &Graph, id: &str) -> RunnableActor {
        graph
            .actors()
            .iter()
            .find(|actor| actor.label() == id)
            .expect("actor exists")
            .clone()
    }

    #[tokio::test]
    async fn actor_stats_track_send_receive_and_bounded_mailbox() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let mut builder = GraphBuilder::new();
        builder.mailbox_capacity(2);
        let worker_ref = builder.actor("worker", {
            let release = release.clone();
            move || GatedDrain {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        });
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");
        let (stop, task) = start_actor(worker);

        started_rx.recv().await.expect("actor started");
        worker_ref.send(1).await.expect("send accepted");
        worker_ref.try_send(2).expect("try_send accepted");
        assert!(matches!(
            worker_ref.try_send(3),
            Err(SendError::MailboxFull { .. })
        ));

        let stats = worker_ref.stats();
        assert_eq!(stats.actor_id, "worker");
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.messages_accepted, 2);
        assert_eq!(stats.messages_conflated, 0);
        assert_eq!(stats.message_bytes_accepted, None);
        assert_eq!(stats.sends_rejected, 1);
        assert_eq!(stats.mailbox_depth, 2);
        assert_eq!(stats.mailbox_capacity, 2);
        assert_eq!(graph.stats(), vec![worker_ref.stats()]);

        release.notify_one();
        received_rx.recv().await.expect("first message received");
        received_rx.recv().await.expect("second message received");
        let stats = worker_ref.stats();
        assert_eq!(stats.messages_received, 2);
        assert_eq!(stats.mailbox_depth, 0);
        assert_eq!(stats.mailbox_capacity, 2);

        stop_actor(stop, task).await.expect("actor stops");
    }

    #[tokio::test]
    async fn message_size_observation_counts_only_accepted_messages() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let mut builder = GraphBuilder::new();
        builder.mailbox_capacity(1);
        let worker_ref = builder.actor_with_options(
            "worker",
            {
                let release = release.clone();
                move || GatedSizedDrain {
                    started: started_tx.clone(),
                    release: release.clone(),
                    received: received_tx.clone(),
                }
            },
            ActorOptions::new().message_size(),
        );
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");
        let (stop, task) = start_actor(worker);

        started_rx.recv().await.expect("actor started");
        worker_ref
            .try_send(SizedPayload(vec![0; 4]))
            .expect("first message accepted");
        assert!(matches!(
            worker_ref.try_send(SizedPayload(vec![0; 100])),
            Err(SendError::MailboxFull { .. })
        ));
        assert_eq!(worker_ref.stats().message_bytes_accepted, Some(4));

        release.notify_one();
        received_rx.recv().await.expect("first message received");
        worker_ref
            .send(SizedPayload(vec![0; 3]))
            .await
            .expect("second message accepted");
        assert_eq!(worker_ref.stats().message_bytes_accepted, Some(7));

        stop_actor(stop, task).await.expect("actor stops");
    }

    #[tokio::test(start_paused = true)]
    async fn standalone_shutdown_timeout_is_reported_as_an_error() {
        let mut builder = GraphBuilder::new();
        builder.actor("worker", || NeverStops);
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");

        assert!(matches!(
            worker
                .run_until(
                    async {},
                    RestartPolicy::Never,
                    Duration::from_millis(100),
                )
                .await,
            Err(ActorRunError::ShutdownTimedOut { actor_id, .. }) if actor_id == "worker"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn standalone_shutdown_bound_leaves_cooperative_actor_clean() {
        let mut builder = GraphBuilder::new();
        builder.actor("worker", || StopsOnShutdown);
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");

        worker
            .run_until(async {}, RestartPolicy::Never, Duration::from_secs(30))
            .await
            .expect("cooperative shutdown completes cleanly");
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_actor_uses_its_supervisor_child_shutdown_grace() {
        let mut builder = GraphBuilder::new();
        builder.actor("factory-template", Drain::<()>::new);
        let graph = builder.build().expect("valid graph");
        let handle = SupervisionTree::dynamic()
            .dynamic_defaults(&graph)
            .build()
            .expect("dynamic runtime builds")
            .spawn();
        handle
            .add_actor(
                "worker",
                || NeverStops,
                DynamicActorOptions::default().shutdown(
                    tokio_supervisor::ShutdownPolicy::cooperative_strict(Duration::from_millis(
                        100,
                    )),
                ),
            )
            .await
            .expect("dynamic actor added");
        handle.wait_started().await.expect("dynamic actor started");
        assert!(matches!(
            handle.remove_child("worker").await,
            Err(ControlError::ShutdownTimedOut(actor_id)) if actor_id == "worker"
        ));
        handle.shutdown_and_wait().await.expect("clean shutdown");
    }

    async fn wait_for_stale_mailbox(actor_ref: &ActorRef<String>) {
        timeout(Duration::from_secs(1), async {
            loop {
                match actor_ref.try_send("probe".to_owned()) {
                    Err(SendError::MailboxClosed { .. }) => break,
                    Err(SendError::ActorNotRunning { .. }) => {
                        panic!("binding cleared before stale mailbox was observed");
                    }
                    Err(SendError::ActorTerminated { .. }) => {
                        panic!("binding terminated before stale mailbox was observed");
                    }
                    Ok(()) | Err(SendError::MailboxFull { .. }) => {
                        sleep(Duration::from_millis(1)).await;
                    }
                    Err(_) => panic!("unexpected send error while observing stale mailbox"),
                }
            }
        })
        .await
        .expect("stale mailbox observed in time");
    }

    #[derive(Clone)]
    struct RebindActor {
        runs: Arc<AtomicUsize>,
        entered_stale_window: mpsc::UnboundedSender<()>,
        release_first_run: Arc<Notify>,
        observed: mpsc::UnboundedSender<String>,
    }

    impl RawActor for RebindActor {
        type Msg = String;

        async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
            let run = self.runs.fetch_add(1, Ordering::SeqCst);
            if run == 0 {
                drop(ctx);
                self.entered_stale_window.send(()).expect("receiver alive");
                self.release_first_run.notified().await;
                return Ok(Continue);
            }

            while let Some(message) = ctx.recv().await {
                self.observed.send(message).expect("receiver alive");
            }
            Ok(Continue)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_ref_send_waits_for_stale_binding_to_change() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = GraphBuilder::new();
        let actor_ref = builder.actor("worker", {
            let runs = Arc::new(AtomicUsize::new(0));
            let release = release.clone();
            move || RebindActor {
                runs: runs.clone(),
                entered_stale_window: entered_tx.clone(),
                release_first_run: release.clone(),
                observed: observed_tx.clone(),
            }
        });
        let graph = builder.build().expect("valid graph");

        let worker = single_actor(&graph, "worker");
        let (_first_stop, first_task) =
            start_actor_with_policy(worker.clone(), RestartPolicy::Always);

        timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("actor entered stale window")
            .expect("actor reported stale window");
        wait_for_stale_mailbox(&actor_ref).await;

        // The stale mailbox is closed but still bound, so the first poll of
        // a send deterministically traverses the closed-mailbox branch. It
        // must park waiting for the binding to change: completing would mean
        // delivery into a dead mailbox, and erroring would break restart
        // ride-through.
        let sending_ref = actor_ref.clone();
        let mut send = Box::pin(async move { sending_ref.send("held".to_owned()).await });
        let first_poll = poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await;
        assert!(
            first_poll.is_pending(),
            "send should wait for a new binding instead of resolving against the stale mailbox"
        );
        let send_task = tokio::spawn(send);

        release.notify_one();
        first_task
            .await
            .expect("first actor task joined")
            .expect("first actor run completed cleanly");

        let (second_stop, second_task) = start_actor_with_policy(worker, RestartPolicy::Always);
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("held message delivered")
                .expect("message observed"),
            "held"
        );
        send_task
            .await
            .expect("send task joined")
            .expect("send completed after rebind");

        stop_actor(second_stop, second_task)
            .await
            .expect("second actor stopped cleanly");
    }

    #[tokio::test]
    async fn runnable_actor_rejects_concurrent_runs() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let mut builder = GraphBuilder::new();
        builder.actor("worker", move || StartSignallingDrain {
            started: started_tx.clone(),
        });
        let graph = builder.build().expect("valid graph");

        let worker = single_actor(&graph, "worker");
        let (stop, task) = start_actor(worker.clone());
        timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("first run should bind the actor promptly")
            .expect("start sender remains alive");

        assert!(matches!(
            worker
                .run_until(
                    pending::<()>(),
                    RestartPolicy::Never,
                    DEFAULT_SHUTDOWN_BOUND,
                )
                .await,
            Err(ActorRunError::AlreadyRunning { actor_id , .. }) if actor_id == "worker"
        ));

        stop_actor(stop, task)
            .await
            .expect("worker stopped cleanly");
    }

    #[derive(Clone)]
    struct FailsOnMessage;

    impl RawActor for FailsOnMessage {
        type Msg = ();

        async fn run(&mut self, mut ctx: ActorContext<()>) -> ActorResult {
            ctx.recv().await;
            Err(std::io::Error::other("commanded failure").into())
        }
    }

    #[tokio::test]
    async fn restart_policy_default_is_on_failure_for_run_until() {
        assert_eq!(RestartPolicy::default(), RestartPolicy::OnFailure);

        let mut builder = GraphBuilder::new();
        let worker_ref = builder.actor("worker", || FailsOnMessage);
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");
        let task = tokio::spawn(async move {
            worker
                .run_until(pending::<()>(), Default::default(), DEFAULT_SHUTDOWN_BOUND)
                .await
        });

        worker_ref.send(()).await.expect("send accepted");
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("run exits in time")
            .expect("actor task joined");
        assert!(matches!(result, Err(ActorRunError::Failed { .. })));
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError::ActorNotRunning { actor_id, .. }) if actor_id == "worker"
        ));
    }

    #[tokio::test]
    async fn dropped_run_follows_restart_policy() {
        for (policy, expect_terminated) in [
            (RestartPolicy::Always, false),
            (RestartPolicy::OnFailure, false),
            (RestartPolicy::Never, true),
        ] {
            let mut builder = GraphBuilder::new();
            let worker_ref = builder.actor("worker", Drain::<()>::new);
            let graph = builder.build().expect("valid graph");
            let worker = single_actor(&graph, "worker");
            let task = tokio::spawn(async move {
                worker
                    .run_until(pending::<()>(), policy, DEFAULT_SHUTDOWN_BOUND)
                    .await
            });

            worker_ref.send(()).await.expect("run bound its mailbox");
            task.abort();
            assert!(
                task.await
                    .expect_err("aborted run task is cancelled")
                    .is_cancelled()
            );

            let disposition = timeout(Duration::from_secs(1), async {
                loop {
                    match worker_ref.try_send(()) {
                        Err(error @ SendError::ActorNotRunning { .. })
                        | Err(error @ SendError::ActorTerminated { .. }) => break error,
                        Ok(())
                        | Err(SendError::MailboxFull { .. })
                        | Err(SendError::MailboxClosed { .. }) => tokio::task::yield_now().await,
                        Err(error) => panic!("unexpected send error after dropped run: {error}"),
                    }
                }
            })
            .await
            .expect("binding reached its dropped-run disposition");

            if expect_terminated {
                assert!(matches!(disposition, SendError::ActorTerminated { .. }));
            } else {
                assert!(matches!(disposition, SendError::ActorNotRunning { .. }));
            }
        }
    }

    #[tokio::test]
    async fn restart_policy_is_per_run_not_sticky() {
        let mut builder = GraphBuilder::new();
        let worker_ref = builder.actor("worker", || FailsOnMessage);
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");

        // First run declares OnFailure: the failed exit leaves the binding
        // waiting to rebind.
        let (_stop, task) = start_actor_with_policy(worker.clone(), RestartPolicy::OnFailure);
        worker_ref.send(()).await.expect("send accepted");
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("first run exits in time")
            .expect("first actor task joined");
        assert!(matches!(result, Err(ActorRunError::Failed { .. })));
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError::ActorNotRunning { actor_id , .. }) if actor_id == "worker"
        ));

        // A second run of the same actor declares Never: the same failed exit
        // now terminates the binding — this run's argument wins over any state
        // left behind by the first run.
        let (_stop, task) = start_actor_with_policy(worker, RestartPolicy::Never);
        worker_ref
            .send(())
            .await
            .expect("send accepted after rebind");
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("second run exits in time")
            .expect("second actor task joined");
        assert!(matches!(result, Err(ActorRunError::Failed { .. })));
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError::ActorTerminated { actor_id , .. }) if actor_id == "worker"
        ));
    }

    struct Work(&'static str);

    #[derive(Clone)]
    struct Forwarder {
        worker: ActorRef<Work>,
    }

    impl RawActor for Forwarder {
        type Msg = Work;

        async fn run(&mut self, mut ctx: ActorContext<Work>) -> ActorResult {
            while let Some(work) = ctx.recv().await {
                let worker = self.worker.clone();
                worker.send(work).await?;
            }
            Ok(Continue)
        }
    }

    #[derive(Clone)]
    struct RestartingWorker {
        runs: Arc<AtomicUsize>,
        observed: mpsc::UnboundedSender<&'static str>,
    }

    impl RawActor for RestartingWorker {
        type Msg = Work;

        async fn run(&mut self, mut ctx: ActorContext<Work>) -> ActorResult {
            let run = self.runs.fetch_add(1, Ordering::SeqCst);
            while let Some(Work(payload)) = ctx.recv().await {
                self.observed.send(payload).expect("receiver alive");
                if run == 0 {
                    return Err::<_, BoxError>(std::io::Error::other("boom").into());
                }
            }
            Ok(Continue)
        }
    }

    #[tokio::test]
    async fn graph_refs_survive_individual_actor_restarts() {
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

        let mut builder = GraphBuilder::new();
        let (worker_slot, worker_ref) = builder.slot::<Work>("worker");
        let frontend_ref = builder.actor("frontend", move || Forwarder {
            worker: worker_ref.clone(),
        });
        let runs = Arc::new(AtomicUsize::new(0));
        builder.define(worker_slot, move || RestartingWorker {
            runs: runs.clone(),
            observed: observed_tx.clone(),
        });
        let graph = builder.build().expect("valid graph");

        let frontend = single_actor(&graph, "frontend");
        let worker = single_actor(&graph, "worker");

        let (frontend_stop, frontend_task) = start_actor(frontend);
        let (_first_worker_stop, first_worker_task) =
            start_actor_with_policy(worker.clone(), RestartPolicy::OnFailure);

        frontend_ref.send(Work("first")).await.expect("first send");
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("first observed")
                .expect("message observed"),
            "first"
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), first_worker_task)
                .await
                .expect("first worker exited")
                .expect("first worker task joined"),
            Err(ActorRunError::Failed { ref actor_id, .. }) if actor_id == "worker"
        ));

        frontend_ref
            .send(Work("second"))
            .await
            .expect("second send");
        timeout(Duration::from_secs(1), async {
            while frontend_ref.stats().messages_received < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("frontend should receive the message while the worker is unbound");
        assert!(matches!(
            observed_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        let (second_worker_stop, second_worker_task) =
            start_actor_with_policy(worker, RestartPolicy::OnFailure);
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("second observed")
                .expect("message observed"),
            "second"
        );

        stop_actor(frontend_stop, frontend_task)
            .await
            .expect("frontend stopped cleanly");
        stop_actor(second_worker_stop, second_worker_task)
            .await
            .expect("worker stopped cleanly");
    }

    #[derive(Clone)]
    struct Forward {
        out: mpsc::UnboundedSender<String>,
    }

    impl RawActor for Forward {
        type Msg = String;

        async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
            while let Some(message) = ctx.recv().await {
                if message == "quit" {
                    break;
                }
                self.out.send(message).expect("receiver alive");
            }
            Ok(Continue)
        }
    }

    #[tokio::test]
    async fn factory_minted_ref_is_live_across_runs_and_dies_with_the_binding() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let (worker, worker_ref) = RunnableActorBuilder::new().actor("worker", move || Forward {
            out: out_tx.clone(),
        });

        // Typed at creation: before any run the binding is unbound, not
        // terminated.
        assert!(matches!(
            worker_ref.try_send("early".to_owned()),
            Err(SendError::ActorNotRunning { actor_id , .. }) if actor_id == "worker"
        ));

        // Each run ends by clean early exit (not requested shutdown), so the
        // `Always` policy leaves the binding unbound and rebindable.
        let (_first_stop, first_task) =
            start_actor_with_policy(worker.clone(), RestartPolicy::Always);
        worker_ref
            .send("first".to_owned())
            .await
            .expect("first send");
        assert_eq!(out_rx.recv().await.as_deref(), Some("first"));
        worker_ref.send("quit".to_owned()).await.expect("quit send");
        timeout(Duration::from_secs(1), first_task)
            .await
            .expect("first run ended in time")
            .expect("first run task joined")
            .expect("first run exited cleanly");

        // The same ref rides into the next incarnation without re-minting.
        let (_second_stop, second_task) =
            start_actor_with_policy(worker.clone(), RestartPolicy::Always);
        worker_ref
            .send("second".to_owned())
            .await
            .expect("second send");
        assert_eq!(out_rx.recv().await.as_deref(), Some("second"));
        worker_ref.send("quit".to_owned()).await.expect("quit send");
        timeout(Duration::from_secs(1), second_task)
            .await
            .expect("second run ended in time")
            .expect("second run task joined")
            .expect("second run exited cleanly");

        // Hand-drivers own the terminate-binding obligation when no further
        // run is coming.
        worker.terminate_binding();
        assert!(matches!(
            worker_ref.try_send("late".to_owned()),
            Err(SendError::ActorTerminated { actor_id , .. }) if actor_id == "worker"
        ));
    }

    #[derive(Clone)]
    struct PoisonableWorker {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        seen: mpsc::UnboundedSender<(usize, u32)>,
        incarnation: Arc<AtomicUsize>,
    }

    impl RawActor for PoisonableWorker {
        type Msg = u32;

        async fn run(&mut self, mut ctx: ActorContext<u32>) -> ActorResult {
            let incarnation = self.incarnation.fetch_add(1, Ordering::SeqCst);
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            while let Some(message) = ctx.recv().await {
                if message == 0 {
                    return Err("poison".into());
                }
                self.seen
                    .send((incarnation, message))
                    .expect("receiver alive");
            }
            Ok(Continue)
        }
    }

    /// D10: mailboxes are incarnation-owned. Messages accepted by an
    /// incarnation that dies before reading them are lost with it — the next
    /// incarnation binds a fresh mailbox and never sees them.
    #[tokio::test]
    async fn messages_accepted_by_a_dead_incarnation_are_lost_at_restart() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = GraphBuilder::new();
        builder.mailbox_capacity(4);
        let worker_ref = builder.actor("worker", {
            let release = release.clone();
            let incarnation = Arc::new(AtomicUsize::new(0));
            move || PoisonableWorker {
                started: started_tx.clone(),
                release: release.clone(),
                seen: seen_tx.clone(),
                incarnation: incarnation.clone(),
            }
        });
        let graph = builder.build().expect("valid graph");
        let worker = single_actor(&graph, "worker");

        // Run 1: a poison message plus two messages queued behind it, all
        // accepted by the first incarnation's mailbox before it reads any.
        let (_first_stop, first_task) =
            start_actor_with_policy(worker.clone(), RestartPolicy::OnFailure);
        started_rx.recv().await.expect("first incarnation started");
        worker_ref.send(0).await.expect("poison accepted");
        worker_ref
            .send(1)
            .await
            .expect("first queued send accepted");
        worker_ref
            .send(2)
            .await
            .expect("second queued send accepted");
        release.notify_one();

        let result = timeout(Duration::from_secs(1), first_task)
            .await
            .expect("first run ended in time")
            .expect("first run task joined");
        assert!(matches!(
            result,
            Err(ActorRunError::Failed { actor_id, .. }) if actor_id == "worker"
        ));

        // Run 2 binds a fresh mailbox: the accepted-but-unread messages died
        // with the first incarnation.
        let (second_stop, second_task) = start_actor_with_policy(worker, RestartPolicy::OnFailure);
        started_rx.recv().await.expect("second incarnation started");
        release.notify_one();
        worker_ref
            .send(3)
            .await
            .expect("send to second incarnation");
        assert_eq!(
            timeout(Duration::from_secs(1), seen_rx.recv())
                .await
                .expect("second incarnation processed a message"),
            Some((1, 3))
        );

        stop_actor(second_stop, second_task)
            .await
            .expect("second run stopped cleanly");
        assert!(
            seen_rx.try_recv().is_err(),
            "messages queued behind the poison were never delivered"
        );
    }

    #[derive(Clone)]
    struct DrainForwarder {
        sink: ActorRef<u32>,
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        outcomes: mpsc::UnboundedSender<Result<(), SendError>>,
    }

    impl Actor for DrainForwarder {
        type Msg = u32;

        async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            Ok(Continue)
        }

        async fn handle(
            &mut self,
            message: u32,
            _ctx: &mut MessageContext<'_, Self>,
        ) -> ActorResult {
            // D10: shutdown is concurrent, so a sibling may already be gone.
            // A drain must treat its SendError as skippable, not fatal.
            let outcome = self.sink.send(message).await;
            self.outcomes.send(outcome).expect("receiver alive");
            Ok(Continue)
        }

        fn drain_policy(&self) -> DrainPolicy {
            DrainPolicy::Drain
        }
    }

    /// D10: siblings stop concurrently during shutdown, so a draining actor
    /// observes `SendError` from already-stopped siblings; tolerating it
    /// lets the drain and the actor finish cleanly.
    #[tokio::test]
    async fn drain_tolerates_send_errors_from_a_stopped_sibling() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (outcomes_tx, mut outcomes_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = GraphBuilder::new();
        let (sink_slot, sink_ref) = builder.slot::<u32>("sink");
        let forwarder_ref = builder.actor("forwarder", {
            let release = release.clone();
            move || DrainForwarder {
                sink: sink_ref.clone(),
                started: started_tx.clone(),
                release: release.clone(),
                outcomes: outcomes_tx.clone(),
            }
        });
        builder.define(sink_slot, Drain::<u32>::new);
        let graph = builder.build().expect("valid graph");

        // The sink runs and stops first; its binding is terminated, exactly
        // as when a supervisor stops siblings concurrently at shutdown.
        let (sink_stop, sink_task) = start_actor(single_actor(&graph, "sink"));
        stop_actor(sink_stop, sink_task)
            .await
            .expect("sink stopped cleanly");

        let (forwarder_stop, forwarder_task) = start_actor(single_actor(&graph, "forwarder"));
        started_rx.recv().await.expect("forwarder started");
        forwarder_ref.send(1).await.expect("first message queued");
        forwarder_ref.send(2).await.expect("second message queued");

        forwarder_stop.cancel();
        release.notify_one();

        timeout(Duration::from_secs(1), forwarder_task)
            .await
            .expect("forwarder stopped in time")
            .expect("forwarder task joined")
            .expect("drain finished cleanly despite sibling send errors");

        for expected in 1..=2 {
            let outcome = outcomes_rx
                .try_recv()
                .unwrap_or_else(|_| panic!("drained message {expected} produced an outcome"));
            assert!(
                matches!(outcome, Err(SendError::ActorTerminated { ref actor_id , .. }) if actor_id == "sink"),
                "drained send observed the stopped sibling: {outcome:?}"
            );
        }
    }

    /// Sets no `drain_policy`, so it drains by default, and propagates the
    /// sibling send with `?` instead of tolerating it.
    #[derive(Clone)]
    struct StrictDrainForwarder {
        sink: ActorRef<u32>,
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
    }

    impl Actor for StrictDrainForwarder {
        type Msg = u32;

        async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            Ok(Continue)
        }

        async fn handle(
            &mut self,
            message: u32,
            _ctx: &mut MessageContext<'_, Self>,
        ) -> ActorResult {
            self.sink.send(message).await?;
            Ok(Continue)
        }
    }

    /// The cost of drain-by-default: an actor that never mentions
    /// `drain_policy` still drains, so a `handle` that propagates a sibling
    /// `SendError` turns what would have been a clean stop into a failed run.
    /// This is the counterpart to
    /// [`drain_tolerates_send_errors_from_a_stopped_sibling`] and the reason
    /// the ordering rules on `DrainPolicy` require drain handlers to tolerate
    /// a stopped sibling.
    #[tokio::test]
    async fn a_drain_that_propagates_a_sibling_send_error_fails_the_actor() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = GraphBuilder::new();
        let (sink_slot, sink_ref) = builder.slot::<u32>("sink");
        let forwarder_ref = builder.actor("forwarder", {
            let release = release.clone();
            move || StrictDrainForwarder {
                sink: sink_ref.clone(),
                started: started_tx.clone(),
                release: release.clone(),
            }
        });
        builder.define(sink_slot, Drain::<u32>::new);
        let graph = builder.build().expect("valid graph");

        let (sink_stop, sink_task) = start_actor(single_actor(&graph, "sink"));
        stop_actor(sink_stop, sink_task)
            .await
            .expect("sink stopped cleanly");

        let (forwarder_stop, forwarder_task) = start_actor(single_actor(&graph, "forwarder"));
        started_rx.recv().await.expect("forwarder started");
        forwarder_ref.send(1).await.expect("message queued");

        forwarder_stop.cancel();
        release.notify_one();

        let outcome = timeout(Duration::from_secs(1), forwarder_task)
            .await
            .expect("forwarder stopped in time")
            .expect("forwarder task joined");
        assert!(
            matches!(
                outcome,
                Err(ActorRunError::Failed { ref actor_id, .. }) if actor_id == "forwarder"
            ),
            "propagating the drain send error failed the run: {outcome:?}"
        );
    }
}
