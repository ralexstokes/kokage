mod support;

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
use support::{ActorHostBuilder, ActorHosts};

use kokage::{
    Actor, ActorFactory, ActorRef, ActorSlot, ActorSpec, BoxError, CallError, Context, ExitResult,
    MailboxShutdown, Reply, SendError, SendErrorKind, Shutdown, StopContext,
    raw::{ActorHost, ActorRunError, DEFAULT_SHUTDOWN_BOUND, RawActor, RawContext},
};

use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

fn add_actor<M, F>(builder: &mut ActorHostBuilder, id: &str, factory: F) -> ActorRef<M>
where
    M: Send + 'static,
    F: ActorFactory,
    F::Actor: RawActor<Msg = M>,
{
    builder.actor(ActorSpec::new(id, factory))
}

fn host(graph: ActorHosts, label: &str) -> ActorHost {
    graph
        .into_nodes()
        .into_iter()
        .find(|actor| actor.label() == label)
        .expect("actor exists")
        .into_host()
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

    async fn run(&mut self, mut ctx: RawContext<M>) -> ExitResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

fn start_graph(
    graph: ActorHosts,
) -> (
    CancellationToken,
    JoinHandle<Vec<Result<(), ActorRunError>>>,
) {
    let stop = CancellationToken::new();
    let tasks = graph
        .into_nodes()
        .into_iter()
        .map(|actor| {
            let actor = actor.into_host();
            let stop = stop.clone();
            tokio::spawn(async move {
                actor
                    .run_once(
                        stop.cancelled(),
                        Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

fn start_graph_with_shutdown(
    graph: ActorHosts,
    shutdown: Shutdown,
) -> (
    CancellationToken,
    JoinHandle<Vec<Result<(), ActorRunError>>>,
) {
    let stop = CancellationToken::new();
    let tasks = graph
        .into_nodes()
        .into_iter()
        .map(|actor| {
            let actor = actor.into_host();
            let stop = stop.clone();
            tokio::spawn(async move { actor.run_once(stop.cancelled(), shutdown).await })
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

    async fn run(&mut self, mut ctx: RawContext<Request>) -> ExitResult {
        while let Some(Request(payload)) = ctx.recv().await {
            self.worker.send(Job { payload }).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    seen: mpsc::UnboundedSender<Job>,
}

impl RawActor for Worker {
    type Msg = Job;

    async fn run(&mut self, mut ctx: RawContext<Job>) -> ExitResult {
        while let Some(job) = ctx.recv().await {
            self.seen.send(job).expect("receiver alive");
        }
        Ok(())
    }
}

#[tokio::test]
async fn typed_pipeline_end_to_end() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();

    let mut builder = ActorHostBuilder::new();
    let worker_slot = ActorSlot::<Job>::new("worker");
    let worker = worker_slot.actor_ref();
    let frontend_slot = ActorSlot::new("frontend");
    let frontend = builder.define(frontend_slot, move || Frontend {
        worker: worker.clone(),
    });
    builder.define(worker_slot, move || Worker {
        seen: seen_tx.clone(),
    });
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
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

    async fn run(&mut self, mut ctx: RawContext<u32>) -> ExitResult {
        while let Some(n) = ctx.recv().await {
            self.seen.send(n).expect("receiver alive");
        }
        Ok(())
    }
}

#[tokio::test]
async fn send_to_never_started_graph_waits_until_graph_runs() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();

    let mut builder = ActorHostBuilder::new();
    let echo = add_actor(&mut builder, "echo", move || Echo {
        seen: seen_tx.clone(),
    });
    let graph = builder.build();

    let echo_for_send = echo.clone();
    let mut send = Box::pin(async move { echo_for_send.send(7).await });
    let first_poll = poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await;
    assert!(
        first_poll.is_pending(),
        "send should wait until the graph binds the actor mailbox"
    );
    let send_task = tokio::spawn(send);

    let (stop, task) = start_graph(graph);
    assert_eq!(recv(&mut seen_rx, "message after graph start").await, 7);
    send_task
        .await
        .expect("send task joined")
        .expect("send completed after graph start");
    stop_graph(stop, task).await;
}

#[tokio::test]
async fn try_send_reports_unbound_and_terminated_states() {
    let mut builder = ActorHostBuilder::new();
    let worker = add_actor(&mut builder, "worker", Drain::<()>::new);
    let graph = builder.build();

    assert!(matches!(
        worker.try_send(()),
        Err(SendError { actor_id, kind: SendErrorKind::NotRunning, .. }) if actor_id == "worker"
    ));

    let (stop, task) = start_graph(graph);
    stop_graph(stop, task).await;

    assert!(matches!(
        worker.try_send(()),
        Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "worker"
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

    async fn run(&mut self, mut ctx: RawContext<CounterMsg>) -> ExitResult {
        let mut total = 0;
        while let Some(message) = ctx.recv().await {
            match message {
                CounterMsg::Add(n) => total += n,
                CounterMsg::Total(reply) => reply.send(total),
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn call_reply_roundtrip() {
    let mut builder = ActorHostBuilder::new();
    let counter = add_actor(&mut builder, "counter", || Counter);
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
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
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        match message {
            HandlerCounterMsg::Add(n) => self.total += n,
            HandlerCounterMsg::Total(reply) => reply.send(self.total),
        }
        Ok(())
    }
}

#[tokio::test]
async fn handler_receives_messages_in_order_and_preserves_state() {
    let mut builder = ActorHostBuilder::new();
    let counter = add_actor(&mut builder, "counter", || HandlerCounter { total: 0 });
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.events
            .send(LifecycleEvent::Started)
            .expect("receiver alive");
        Ok(())
    }

    async fn handle(&mut self, _message: (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.events
            .send(LifecycleEvent::Handled)
            .expect("receiver alive");
        Ok(())
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
    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", move || LifecycleHandler {
        events: events_tx.clone(),
    });
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.events
            .send(LifecycleEvent::Started)
            .expect("receiver alive");
        Err(io::Error::other("start failed").into())
    }

    async fn handle(&mut self, _message: (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.events
            .send(LifecycleEvent::Handled)
            .expect("receiver alive");
        Ok(())
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
    let mut builder = ActorHostBuilder::new();
    add_actor(&mut builder, "worker", move || FailingStartHandler {
        events: events_tx.clone(),
    });
    let graph = builder.build();

    let result = host(graph, "worker")
        .run_once(
            pending::<()>(),
            Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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
    assert!(events_rx.try_recv().is_err());
}

#[derive(Clone)]
struct FailingHandler;

impl Actor for FailingHandler {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Err(io::Error::other("handle failed").into())
    }
}

#[tokio::test]
async fn handler_error_fails_the_actor_run() {
    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", || FailingHandler);
    let graph = builder.build();

    let worker = host(graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

#[derive(Clone)]
struct ContextStop {
    stopped: mpsc::UnboundedSender<()>,
}

impl Actor for ContextStop {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
        assert!(!ctx.is_draining());
        ctx.stop();
        assert!(!ctx.is_draining());
        ctx.stop();
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.stopped.send(()).expect("receiver alive");
        Ok(())
    }
}

#[tokio::test]
async fn context_stop_is_idempotent_and_exits_normally() {
    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", move || ContextStop {
        stopped: stopped_tx.clone(),
    });
    let graph = builder.build();
    let worker = host(graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
            )
            .await
    });

    actor.send(()).await.expect("stop message sent");
    recv(&mut stopped_rx, "on_stop ran").await;
    assert!(task.await.expect("actor task joined").is_ok());
}

struct ContextStopThenFail;

impl Actor for ContextStopThenFail {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.stop();
        Err("failure wins over stop".into())
    }
}

#[tokio::test]
async fn context_error_takes_precedence_over_a_stop_request() {
    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", || ContextStopThenFail);
    let graph = builder.build();
    let worker = host(graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
            )
            .await
    });

    actor.send(()).await.expect("message sent");
    assert!(matches!(
        task.await.expect("actor task joined"),
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
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    events: mpsc::UnboundedSender<GateEvent>,
}

impl Actor for GateHandler {
    type Msg = GateMsg;

    async fn handle(&mut self, message: GateMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            GateMsg::Hold => {
                self.started.send(()).expect("receiver alive");
                self.release.notified().await;
            }
            GateMsg::Stop => {
                self.started.send(()).expect("receiver alive");
                ctx.continue_with(GateMsg::Add(99));
                self.release.notified().await;
                ctx.stop();
                return Ok(());
            }
            GateMsg::Add(n) => {
                self.total += n;
                self.events
                    .send(GateEvent::Handled(n))
                    .expect("receiver alive");
            }
            GateMsg::Total(reply) => reply.send(self.total),
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events
            .send(GateEvent::Stopped(self.total))
            .expect("receiver alive");
        Ok(())
    }
}

#[tokio::test]
async fn handler_stop_with_discard_drops_mailbox_and_continuations_then_runs_on_stop() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = ActorHostBuilder::new();
    let actor = builder.actor(
        ActorSpec::new("worker", {
            let release = release.clone();
            move || GateHandler {
                total: 0,
                started: started_tx.clone(),
                release: release.clone(),
                events: events_tx.clone(),
            }
        })
        .mailbox_shutdown(MailboxShutdown::Discard),
    );
    let graph = builder.build();
    let worker = host(graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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
    let mut builder = ActorHostBuilder::new();
    let actor = builder.actor(
        ActorSpec::new("worker", {
            let release = release.clone();
            move || GateHandler {
                total: 0,
                started: started_tx.clone(),
                release: release.clone(),
                events: events_tx.clone(),
            }
        })
        .mailbox_shutdown(MailboxShutdown::Drain),
    );
    let graph = builder.build();
    let worker = host(graph, "worker");
    let task = tokio::spawn(async move {
        worker
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

    let mut builder = ActorHostBuilder::new();
    let actor = builder.actor(
        ActorSpec::new("worker", {
            let release = release.clone();
            move || GateHandler {
                total: 0,
                started: started_tx.clone(),
                release: release.clone(),
                events: events_tx.clone(),
            }
        })
        .mailbox_shutdown(MailboxShutdown::Discard),
    );
    let graph = builder.build();

    let (stop, task) =
        start_graph_with_shutdown(graph, Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND));
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
    assert!(events_rx.try_recv().is_err());
}

#[tokio::test]
async fn handler_drain_handles_queued_messages_and_replies_before_stop() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", {
        let release = release.clone();
        move || GateHandler {
            total: 0,
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build();

    let (stop, task) =
        start_graph_with_shutdown(graph, Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND));
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

    async fn run(&mut self, mut ctx: RawContext<TryDrainMsg>) -> ExitResult {
        match ctx.recv().await {
            Some(TryDrainMsg::Start) => self.started.send(()).expect("receiver alive"),
            Some(TryDrainMsg::Value(_)) => panic!("expected start message"),
            None => panic!("shutdown before start"),
        }

        self.release.notified().await;
        assert!(ctx.recv().await.is_none());

        loop {
            match ctx.try_recv() {
                Some(TryDrainMsg::Value(value)) => self
                    .events
                    .send(TryDrainEvent::Drained(value))
                    .expect("receiver alive"),
                Some(TryDrainMsg::Start) => panic!("unexpected start message"),
                None => {
                    self.events
                        .send(TryDrainEvent::Empty)
                        .expect("receiver alive");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn try_recv_drains_messages_after_shutdown_recv_returns_none() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let mut builder = ActorHostBuilder::new();
    let actor = add_actor(&mut builder, "worker", {
        let release = release.clone();
        move || TryDrainActor {
            started: started_tx.clone(),
            release: release.clone(),
            events: events_tx.clone(),
        }
    });
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
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

    async fn run(&mut self, mut ctx: RawContext<Ball>) -> ExitResult {
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
        Ok(())
    }
}

#[tokio::test]
async fn cyclic_wiring_via_slot() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();

    let mut builder = ActorHostBuilder::new();
    let pong_slot = ActorSlot::<Ball>::new("pong");
    let pong = pong_slot.actor_ref();
    let ping_slot = ActorSlot::new("ping");
    let ping = ping_slot.actor_ref();
    builder.define(ping_slot, {
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
    let graph = builder.build();

    let (stop, task) = start_graph(graph);
    ping.send(Ball { bounces_left: 5 }).await.expect("serve");
    recv(&mut done_rx, "rally finished").await;
    stop_graph(stop, task).await;
}

#[test]
fn dropping_an_unfilled_actor_slot_terminates_its_ref() {
    let slot = ActorSlot::<u32>::new("ghost");
    let ghost = slot.actor_ref();
    drop(slot);

    assert!(matches!(
        ghost.try_send(1),
        Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "ghost"
    ));
}

#[test]
fn dropping_an_unplaced_actor_spec_terminates_its_ref() {
    let spec = ActorSpec::new("ghost", Drain::<u32>::new);
    let ghost = spec.actor_ref();
    drop(spec);

    assert!(matches!(
        ghost.try_send(1),
        Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "ghost"
    ));
}

#[tokio::test]
async fn actors_can_declare_distinct_mailbox_capacities() {
    let mut builder = ActorHostBuilder::new();
    let shallow = builder.actor(ActorSpec::new("shallow", Drain::<()>::new).mailbox_capacity(2));
    let deep = builder.actor(ActorSpec::new("deep", Drain::<()>::new).mailbox_capacity(9));
    let graph = builder.build();

    // Capacity is a property of the bound mailbox, so it is observable only
    // once an incarnation is running.
    let stop = CancellationToken::new();
    let tasks: Vec<_> = graph
        .into_nodes()
        .into_iter()
        .map(|actor| {
            let actor = actor.into_host();
            let stop = stop.clone();
            tokio::spawn(async move {
                actor
                    .run_once(
                        stop.cancelled(),
                        Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

    async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
        Err(io::Error::other("boom").into())
    }
}

#[tokio::test]
async fn actor_error_fails_its_run() {
    let mut builder = ActorHostBuilder::new();
    add_actor(&mut builder, "healthy", Drain::<()>::new);
    add_actor(&mut builder, "bad", || Fail);
    let graph = builder.build();

    let result = host(graph, "bad")
        .run_once(
            pending::<()>(),
            Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

    async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn early_clean_exit_is_a_clean_actor_run() {
    let mut builder = ActorHostBuilder::new();
    add_actor(&mut builder, "quitter", || Quit);
    let graph = builder.build();

    host(graph, "quitter")
        .run_once(
            pending::<()>(),
            Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
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

        async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
            self.live.store(true, Ordering::Release);
            let _guard = LiveGuard(self.live.clone());
            self.started.notify_one();
            pending::<()>().await;
            Ok(())
        }
    }

    let started = Arc::new(Notify::new());
    let live = Arc::new(AtomicBool::new(false));
    let mut builder = ActorHostBuilder::new();
    add_actor(&mut builder, "worker", {
        let started = started.clone();
        let live = live.clone();
        move || Stubborn {
            started: started.clone(),
            live: live.clone(),
        }
    });
    let graph = builder.build();
    let worker = host(graph, "worker");
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move {
            worker
                .run_once(
                    stop.cancelled(),
                    Shutdown::graceful_for(Duration::from_millis(50)),
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

#[tokio::test]
async fn send_to_dropped_never_started_graph_returns_actor_terminated() {
    let mut builder = ActorHostBuilder::new();
    let echo = add_actor(&mut builder, "echo", Drain::<u32>::new);
    let graph = builder.build();

    drop(graph);
    assert!(matches!(
        echo.send(1).await,
        Err(SendError { actor_id , .. }) if actor_id == "echo"
    ));
}

mod actor_host {
    use super::{ActorHostBuilder, ActorHosts, add_actor};

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

    use kokage::{
        Actor, ActorRef, ActorSlot, ActorSpec, BoxError, Context, ControlError, DynamicTree,
        ExitResult, SendError, SendErrorKind, Shutdown, SupervisorError,
        raw::{
            ActorHost, ActorRunError, DEFAULT_SHUTDOWN_BOUND, IncarnationExit, RawActor, RawContext,
        },
    };
    use tokio::{
        sync::{Notify, mpsc},
        task::JoinHandle,
        time::timeout,
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

        async fn run(&mut self, mut ctx: RawContext<M>) -> ExitResult {
            while ctx.recv().await.is_some() {}
            Ok(())
        }
    }

    #[derive(Clone)]
    struct StartSignallingDrain {
        started: mpsc::UnboundedSender<()>,
    }

    impl RawActor for StartSignallingDrain {
        type Msg = ();

        async fn run(&mut self, mut ctx: RawContext<()>) -> ExitResult {
            self.started.send(()).expect("start receiver alive");
            while ctx.recv().await.is_some() {}
            Ok(())
        }
    }

    #[derive(Clone)]
    struct NeverStops;

    impl RawActor for NeverStops {
        type Msg = ();

        async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
            pending::<ExitResult>().await
        }
    }

    #[derive(Clone)]
    struct StopsOnShutdown;

    impl RawActor for StopsOnShutdown {
        type Msg = ();

        async fn run(&mut self, ctx: RawContext<()>) -> ExitResult {
            ctx.shutdown_token().cancelled().await;
            Ok(())
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

    fn sized_payload_size(message: &SizedPayload) -> usize {
        message.0.len()
    }

    #[derive(Clone)]
    struct GatedSizedDrain {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
        received: mpsc::UnboundedSender<()>,
    }

    impl RawActor for GatedSizedDrain {
        type Msg = SizedPayload;

        async fn run(&mut self, mut ctx: RawContext<SizedPayload>) -> ExitResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            while let Some(_message) = ctx.recv().await {
                self.received.send(()).expect("receiver alive");
            }
            Ok(())
        }
    }

    #[test]
    fn failed_sized_registration_returns_a_sized_detached_ref() {
        let mut builder = ActorHostBuilder::new();
        let actor_slot = ActorSlot::new("worker");
        builder.actor(
            actor_slot
                .define(Drain::<SizedPayload>::new)
                .message_size(sized_payload_size),
        );
        let detached_slot = ActorSlot::new("worker");
        let detached = detached_slot.actor_ref();
        builder.actor(
            detached_slot
                .define(Drain::<SizedPayload>::new)
                .message_size(sized_payload_size),
        );

        assert_eq!(detached.stats().message_bytes_accepted, Some(0));

        let detached_slot = ActorSlot::<SizedPayload>::new("worker");
        let detached = detached_slot.actor_ref();
        let detached_spec = detached_slot
            .define(Drain::<SizedPayload>::new)
            .message_size(sized_payload_size);
        drop(detached_spec);
        // The option becomes observable only when the completed declaration is
        // materialized; dropping the spec leaves its ref detached and unsized.
        assert_eq!(detached.stats().message_bytes_accepted, None);
        assert!(matches!(
            detached.try_send(SizedPayload(Vec::new())),
            Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "worker"
        ));
    }

    impl RawActor for GatedDrain {
        type Msg = u32;

        async fn run(&mut self, mut ctx: RawContext<u32>) -> ExitResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            while let Some(_message) = ctx.recv().await {
                self.received.send(()).expect("receiver alive");
            }
            Ok(())
        }
    }

    fn start_actor(actor: ActorHost) -> (CancellationToken, JoinHandle<Result<(), ActorRunError>>) {
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                actor
                    .run_once(
                        stop.cancelled(),
                        Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
                    )
                    .await
            }
        });
        (stop, task)
    }

    fn start_incarnation(mut actor: ActorHost) -> JoinHandle<(ActorHost, IncarnationExit)> {
        tokio::spawn(async move {
            let exit = actor
                .run_incarnation(
                    pending::<()>(),
                    Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
                )
                .await;
            (actor, exit)
        })
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

    fn single_actor(graph: ActorHosts, id: &str) -> ActorHost {
        graph
            .into_nodes()
            .into_iter()
            .find(|actor| actor.label() == id)
            .expect("actor exists")
            .into_host()
    }

    #[tokio::test]
    async fn actor_stats_track_send_receive_and_bounded_mailbox() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let mut builder = ActorHostBuilder::new();
        let worker_spec = ActorSpec::new("worker", {
            let release = release.clone();
            move || GatedDrain {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        })
        .mailbox_capacity(2);
        let worker_ref = builder.actor(worker_spec);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");
        let (stop, task) = start_actor(worker);

        started_rx.recv().await.expect("actor started");
        worker_ref.send(1).await.expect("send accepted");
        worker_ref.try_send(2).expect("try_send accepted");
        assert!(matches!(
            worker_ref.try_send(3),
            Err(SendError {
                kind: SendErrorKind::Full,
                ..
            })
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
        let mut builder = ActorHostBuilder::new();
        let worker_spec = ActorSpec::new("worker", {
            let release = release.clone();
            move || GatedSizedDrain {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        })
        .mailbox_capacity(1);
        let worker_ref = worker_spec.actor_ref();
        let second_ref = worker_spec.actor_ref();
        let worker_spec = worker_spec.message_size(sized_payload_size);
        builder.actor(worker_spec);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");
        let (stop, task) = start_actor(worker);

        started_rx.recv().await.expect("actor started");
        assert_eq!(worker_ref.id(), second_ref.id());
        worker_ref
            .try_send(SizedPayload(vec![0; 4]))
            .expect("first message accepted");
        assert!(matches!(
            worker_ref.try_send(SizedPayload(vec![0; 100])),
            Err(SendError {
                kind: SendErrorKind::Full,
                ..
            })
        ));
        assert_eq!(worker_ref.stats().message_bytes_accepted, Some(4));

        release.notify_one();
        received_rx.recv().await.expect("first message received");
        worker_ref
            .send(SizedPayload(vec![0; 3]))
            .await
            .expect("second message accepted");
        assert_eq!(worker_ref.stats().message_bytes_accepted, Some(7));
        assert_eq!(second_ref.stats().message_bytes_accepted, Some(7));

        stop_actor(stop, task).await.expect("actor stops");
    }

    #[tokio::test]
    async fn pending_send_observes_message_size_configured_before_materialization() {
        let spec = ActorSpec::new("worker", Drain::<SizedPayload>::new);
        let worker_ref = spec.actor_ref();
        let send = tokio::spawn({
            let worker_ref = worker_ref.clone();
            async move { worker_ref.send(SizedPayload(vec![0; 9])).await }
        });
        tokio::task::yield_now().await;

        let actor = spec.message_size(sized_payload_size).into_host();
        let (stop, task) = start_actor(actor);
        send.await
            .expect("send task does not panic")
            .expect("message is accepted after the first mailbox binds");

        assert_eq!(worker_ref.stats().message_bytes_accepted, Some(9));
        stop_actor(stop, task).await.expect("actor stops");
    }

    #[tokio::test(start_paused = true)]
    async fn standalone_shutdown_timeout_is_reported_as_an_error() {
        let mut builder = ActorHostBuilder::new();
        add_actor(&mut builder, "worker", || NeverStops);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");

        assert!(matches!(
            worker
                .run_once(
                    async {},
                    Shutdown::graceful_for(Duration::from_millis(100)),
                )
                .await,
            Err(ActorRunError::ShutdownTimedOut { actor_id, .. }) if actor_id == "worker"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn standalone_shutdown_bound_leaves_cooperative_actor_clean() {
        let mut builder = ActorHostBuilder::new();
        add_actor(&mut builder, "worker", || StopsOnShutdown);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");

        worker
            .run_once(async {}, Shutdown::graceful_for(Duration::from_secs(30)))
            .await
            .expect("cooperative shutdown completes cleanly");
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_actor_uses_its_supervisor_child_shutdown_grace() {
        let runtime = DynamicTree::new().spawn().expect("dynamic runtime builds");
        crate::support::dynamic_root(&runtime)
            .add_actor_spec(
                ActorSpec::new("worker", || NeverStops)
                    .shutdown(Shutdown::graceful_for(Duration::from_millis(100))),
            )
            .await
            .expect("dynamic actor added");
        runtime
            .scope()
            .wait_started()
            .await
            .expect("dynamic actor started");
        assert!(matches!(
            crate::support::dynamic_root(&runtime)
                .remove_child("worker")
                .await,
            Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(actor_id)))
                if actor_id == "worker"
        ));
        runtime.shutdown_and_wait().await.expect("clean shutdown");
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

        async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
            let run = self.runs.fetch_add(1, Ordering::SeqCst);
            if run == 0 {
                drop(ctx);
                self.entered_stale_window.send(()).expect("receiver alive");
                self.release_first_run.notified().await;
                return Ok(());
            }

            while let Some(message) = ctx.recv().await {
                self.observed.send(message).expect("receiver alive");
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_ref_send_waits_for_stale_binding_to_change() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = ActorHostBuilder::new();
        let actor_ref = add_actor(&mut builder, "worker", {
            let runs = Arc::new(AtomicUsize::new(0));
            let release = release.clone();
            move || RebindActor {
                runs: runs.clone(),
                entered_stale_window: entered_tx.clone(),
                release_first_run: release.clone(),
                observed: observed_tx.clone(),
            }
        });
        let graph = builder.build();

        let worker = single_actor(graph, "worker");
        let first_task = start_incarnation(worker);

        timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("actor entered stale window")
            .expect("actor reported stale window");
        assert!(matches!(
            actor_ref.try_send("probe".to_owned()),
            Err(SendError {
                kind: SendErrorKind::NotRunning,
                ..
            })
        ));

        // RebindActor reports this point after dropping RawContext while its
        // run deliberately stays alive, so the current incarnation remains
        // bound to a closed mailbox. The public try_send error intentionally
        // projects that state to NotRunning. An awaited send must still park
        // until the binding changes: completing would mean delivery into a
        // dead mailbox, and erroring would break restart ride-through.
        let sending_ref = actor_ref.clone();
        let mut send = Box::pin(async move { sending_ref.send("held".to_owned()).await });
        let first_poll = poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await;
        assert!(
            first_poll.is_pending(),
            "send should wait for a new binding instead of resolving against the stale mailbox"
        );
        let send_task = tokio::spawn(send);

        release.notify_one();
        let (worker, first_exit) = first_task.await.expect("first actor task joined");
        assert!(matches!(first_exit, IncarnationExit::Stopped));

        let (second_stop, second_task) = start_actor(worker);
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
    async fn run_once_consumes_the_host_and_terminates_its_binding() {
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", || StopsOnShutdown);
        let graph = builder.build();

        let worker = single_actor(graph, "worker");
        worker
            .run_once(async {}, Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND))
            .await
            .expect("worker stopped cleanly");
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. })
                if actor_id == "worker"
        ));
    }

    #[derive(Clone)]
    struct FailsOnMessage;

    impl RawActor for FailsOnMessage {
        type Msg = ();

        async fn run(&mut self, mut ctx: RawContext<()>) -> ExitResult {
            ctx.recv().await;
            Err(std::io::Error::other("commanded failure").into())
        }
    }

    #[tokio::test]
    async fn failed_run_once_terminates_the_binding() {
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", || FailsOnMessage);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");
        let task = tokio::spawn(async move {
            worker
                .run_once(
                    pending::<()>(),
                    Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
                )
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
            Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "worker"
        ));
    }

    #[tokio::test]
    async fn dropping_a_run_once_future_terminates_the_binding() {
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", Drain::<()>::new);
        let worker = single_actor(builder.build(), "worker");
        let task = tokio::spawn(worker.run_once(
            pending::<()>(),
            Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
        ));

        worker_ref.send(()).await.expect("run bound its mailbox");
        task.abort();
        assert!(
            task.await
                .expect_err("run task is cancelled")
                .is_cancelled()
        );
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError {
                kind: SendErrorKind::Terminated,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn dropping_a_run_incarnation_future_keeps_the_host_reusable() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", move || StartSignallingDrain {
            started: started_tx.clone(),
        });
        let mut worker = single_actor(builder.build(), "worker");
        let mut run = Box::pin(worker.run_incarnation(
            pending::<()>(),
            Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
        ));

        tokio::select! {
            exit = &mut run => panic!("incarnation exited unexpectedly: {exit:?}"),
            started = started_rx.recv() => started.expect("first incarnation started"),
        }
        drop(run);

        timeout(Duration::from_secs(1), async {
            loop {
                match worker_ref.try_send(()) {
                    Err(SendError {
                        kind: SendErrorKind::NotRunning,
                        ..
                    }) => break,
                    Ok(())
                    | Err(SendError {
                        kind: SendErrorKind::Full,
                        ..
                    }) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected binding state after cancellation: {error}"),
                }
            }
        })
        .await
        .expect("cancelled incarnation becomes unbound");

        let (stop, task) = start_actor(worker);
        started_rx.recv().await.expect("second incarnation started");
        worker_ref
            .send(())
            .await
            .expect("replacement mailbox bound");
        stop_actor(stop, task)
            .await
            .expect("replacement incarnation stopped cleanly");
    }

    #[tokio::test]
    async fn run_incarnation_keeps_the_binding_rebindable_until_host_drop() {
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", || FailsOnMessage);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");

        let task = start_incarnation(worker);
        worker_ref.send(()).await.expect("send accepted");
        let (worker, exit) = timeout(Duration::from_secs(1), task)
            .await
            .expect("incarnation exits in time")
            .expect("actor task joined");
        assert!(matches!(
            exit,
            IncarnationExit::Failed(ActorRunError::Failed { .. })
        ));
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError { actor_id, kind: SendErrorKind::NotRunning, .. }) if actor_id == "worker"
        ));

        drop(worker);
        assert!(matches!(
            worker_ref.try_send(()),
            Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "worker"
        ));
    }

    struct Work(&'static str);

    #[derive(Clone)]
    struct Forwarder {
        worker: ActorRef<Work>,
    }

    impl RawActor for Forwarder {
        type Msg = Work;

        async fn run(&mut self, mut ctx: RawContext<Work>) -> ExitResult {
            while let Some(work) = ctx.recv().await {
                let worker = self.worker.clone();
                worker.send(work).await?;
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RestartingWorker {
        runs: Arc<AtomicUsize>,
        observed: mpsc::UnboundedSender<&'static str>,
    }

    impl RawActor for RestartingWorker {
        type Msg = Work;

        async fn run(&mut self, mut ctx: RawContext<Work>) -> ExitResult {
            let run = self.runs.fetch_add(1, Ordering::SeqCst);
            while let Some(Work(payload)) = ctx.recv().await {
                self.observed.send(payload).expect("receiver alive");
                if run == 0 {
                    return Err::<_, BoxError>(std::io::Error::other("boom").into());
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn graph_refs_survive_individual_actor_restarts() {
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

        let mut builder = ActorHostBuilder::new();
        let worker_slot = ActorSlot::<Work>::new("worker");
        let worker_ref = worker_slot.actor_ref();
        let frontend_ref_slot = ActorSlot::new("frontend");
        let frontend_ref = builder.define(frontend_ref_slot, move || Forwarder {
            worker: worker_ref.clone(),
        });
        let runs = Arc::new(AtomicUsize::new(0));
        builder.define(worker_slot, move || RestartingWorker {
            runs: runs.clone(),
            observed: observed_tx.clone(),
        });
        let graph = builder.build();

        let mut actors = graph.into_nodes().into_iter().map(|node| node.into_host());
        let frontend = actors.next().expect("frontend exists");
        let worker = actors.next().expect("worker exists");

        let (frontend_stop, frontend_task) = start_actor(frontend);
        let first_worker_task = start_incarnation(worker);

        frontend_ref.send(Work("first")).await.expect("first send");
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("first observed")
                .expect("message observed"),
            "first"
        );
        let (worker, first_exit) = timeout(Duration::from_secs(1), first_worker_task)
            .await
            .expect("first worker exited")
            .expect("first worker task joined");
        assert!(matches!(
            first_exit,
            IncarnationExit::Failed(ActorRunError::Failed { ref actor_id, .. })
                if actor_id == "worker"
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

        let (second_worker_stop, second_worker_task) = start_actor(worker);
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

        async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
            while let Some(message) = ctx.recv().await {
                if message == "quit" {
                    break;
                }
                self.out.send(message).expect("receiver alive");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn factory_minted_ref_is_live_across_runs_and_dies_with_the_binding() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let mut builder = ActorHostBuilder::new();
        let worker_ref = add_actor(&mut builder, "worker", move || Forward {
            out: out_tx.clone(),
        });
        let graph = builder.build();
        let worker = single_actor(graph, "worker");

        // Typed at creation: before any run the binding is unbound, not
        // terminated.
        assert!(matches!(
            worker_ref.try_send("early".to_owned()),
            Err(SendError { actor_id, kind: SendErrorKind::NotRunning, .. }) if actor_id == "worker"
        ));

        // Each incarnation ends by clean early exit while the owning host
        // keeps the binding unbound and rebindable.
        let first_task = start_incarnation(worker);
        worker_ref
            .send("first".to_owned())
            .await
            .expect("first send");
        assert_eq!(out_rx.recv().await.as_deref(), Some("first"));
        worker_ref.send("quit".to_owned()).await.expect("quit send");
        let (worker, first_exit) = timeout(Duration::from_secs(1), first_task)
            .await
            .expect("first run ended in time")
            .expect("first run task joined");
        assert!(matches!(first_exit, IncarnationExit::Stopped));

        // The same ref rides into the next incarnation without re-minting.
        let second_task = start_incarnation(worker);
        worker_ref
            .send("second".to_owned())
            .await
            .expect("second send");
        assert_eq!(out_rx.recv().await.as_deref(), Some("second"));
        worker_ref.send("quit".to_owned()).await.expect("quit send");
        let (worker, second_exit) = timeout(Duration::from_secs(1), second_task)
            .await
            .expect("second run ended in time")
            .expect("second run task joined");
        assert!(matches!(second_exit, IncarnationExit::Stopped));

        // Dropping the owner makes the binding terminal when no further
        // incarnation is coming.
        drop(worker);
        assert!(matches!(
            worker_ref.try_send("late".to_owned()),
            Err(SendError { actor_id, kind: SendErrorKind::Terminated, .. }) if actor_id == "worker"
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

        async fn run(&mut self, mut ctx: RawContext<u32>) -> ExitResult {
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
            Ok(())
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

        let mut builder = ActorHostBuilder::new();
        let worker_spec = ActorSpec::new("worker", {
            let release = release.clone();
            let incarnation = Arc::new(AtomicUsize::new(0));
            move || PoisonableWorker {
                started: started_tx.clone(),
                release: release.clone(),
                seen: seen_tx.clone(),
                incarnation: incarnation.clone(),
            }
        })
        .mailbox_capacity(4);
        let worker_ref = builder.actor(worker_spec);
        let graph = builder.build();
        let worker = single_actor(graph, "worker");

        // Run 1: a poison message plus two messages queued behind it, all
        // accepted by the first incarnation's mailbox before it reads any.
        let first_task = start_incarnation(worker);
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

        let (worker, first_exit) = timeout(Duration::from_secs(1), first_task)
            .await
            .expect("first run ended in time")
            .expect("first run task joined");
        assert!(matches!(
            first_exit,
            IncarnationExit::Failed(ActorRunError::Failed { actor_id, .. })
                if actor_id == "worker"
        ));

        // Run 2 binds a fresh mailbox: the accepted-but-unread messages died
        // with the first incarnation.
        let (second_stop, second_task) = start_actor(worker);
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
        outcomes: mpsc::UnboundedSender<Result<(), SendError<u32>>>,
    }

    impl Actor for DrainForwarder {
        type Msg = u32;

        async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            Ok(())
        }

        async fn handle(&mut self, message: u32, _ctx: &mut Context<'_, Self>) -> ExitResult {
            // D10: shutdown is concurrent, so a sibling may already be gone.
            // A drain must treat its SendError as skippable, not fatal.
            let outcome = self.sink.send(message).await;
            self.outcomes.send(outcome).expect("receiver alive");
            Ok(())
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

        let mut builder = ActorHostBuilder::new();
        let sink_ref = add_actor(&mut builder, "sink", Drain::<u32>::new);
        let forwarder_ref = add_actor(&mut builder, "forwarder", {
            let release = release.clone();
            move || DrainForwarder {
                sink: sink_ref.clone(),
                started: started_tx.clone(),
                release: release.clone(),
                outcomes: outcomes_tx.clone(),
            }
        });
        let graph = builder.build();
        let mut actors = graph.into_nodes().into_iter().map(|node| node.into_host());
        let sink = actors.next().expect("sink exists");
        let forwarder = actors.next().expect("forwarder exists");

        // The sink runs and stops first; its binding is terminated, exactly
        // as when a supervisor stops siblings concurrently at shutdown.
        let (sink_stop, sink_task) = start_actor(sink);
        stop_actor(sink_stop, sink_task)
            .await
            .expect("sink stopped cleanly");

        let (forwarder_stop, forwarder_task) = start_actor(forwarder);
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
                matches!(outcome, Err(SendError { ref actor_id , .. }) if actor_id == "sink"),
                "drained send observed the stopped sibling: {outcome:?}"
            );
        }
    }

    /// Sets no shutdown override, so it drains by default, and propagates the
    /// sibling send with `?` instead of tolerating it.
    #[derive(Clone)]
    struct StrictDrainForwarder {
        sink: ActorRef<u32>,
        started: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
    }

    impl Actor for StrictDrainForwarder {
        type Msg = u32;

        async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
            self.started.send(()).expect("receiver alive");
            self.release.notified().await;
            Ok(())
        }

        async fn handle(&mut self, message: u32, _ctx: &mut Context<'_, Self>) -> ExitResult {
            self.sink.send(message).await?;
            Ok(())
        }
    }

    /// The cost of drain-by-default: an actor that never mentions
    /// The default shutdown still drains, so a `handle` that propagates a sibling
    /// `SendError` turns what would have been a clean stop into a failed run.
    /// This is the counterpart to
    /// [`drain_tolerates_send_errors_from_a_stopped_sibling`] and the reason
    /// the ordering rules on `Shutdown` require drain handlers to tolerate
    /// a stopped sibling.
    #[tokio::test]
    async fn a_drain_that_propagates_a_sibling_send_error_fails_the_actor() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());

        let mut builder = ActorHostBuilder::new();
        let sink_ref = add_actor(&mut builder, "sink", Drain::<u32>::new);
        let forwarder_ref = add_actor(&mut builder, "forwarder", {
            let release = release.clone();
            move || StrictDrainForwarder {
                sink: sink_ref.clone(),
                started: started_tx.clone(),
                release: release.clone(),
            }
        });
        let graph = builder.build();
        let mut actors = graph.into_nodes().into_iter().map(|node| node.into_host());
        let sink = actors.next().expect("sink exists");
        let forwarder = actors.next().expect("forwarder exists");

        let (sink_stop, sink_task) = start_actor(sink);
        stop_actor(sink_stop, sink_task)
            .await
            .expect("sink stopped cleanly");

        let (forwarder_stop, forwarder_task) = start_actor(forwarder);
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
