use std::{
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use tokio::{
    sync::{Notify, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_otp::{
    ActorContext, ActorResult, ActorRunError, DEFAULT_SHUTDOWN_BOUND, Graph, GraphBuilder,
    RawActor, RestartPolicy,
};
use tokio_util::sync::CancellationToken;

type SenderSlot<T> = Arc<Mutex<Option<oneshot::Sender<T>>>>;

fn sender_slot<T>(sender: oneshot::Sender<T>) -> SenderSlot<T> {
    Arc::new(Mutex::new(Some(sender)))
}

fn send_once<T>(slot: &SenderSlot<T>, value: T) {
    if let Some(sender) = slot.lock().expect("sender mutex poisoned").take() {
        let _ = sender.send(value);
    }
}

fn start_graph(graph: &Graph) -> (CancellationToken, JoinHandle<Result<(), ActorRunError>>) {
    let stop = CancellationToken::new();
    let actor = graph.actors().first().expect("actor exists").clone();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move {
            actor
                .run_until(
                    stop.cancelled(),
                    RestartPolicy::Never,
                    DEFAULT_SHUTDOWN_BOUND,
                )
                .await
        }
    });
    (stop, task)
}

async fn stop_graph(stop: CancellationToken, task: JoinHandle<Result<(), ActorRunError>>) {
    stop.cancel();
    timeout(Duration::from_secs(1), task)
        .await
        .expect("graph stopped in time")
        .expect("graph task joined")
        .expect("graph stopped cleanly");
}

#[derive(Clone)]
struct ReturnsResult {
    observed: SenderSlot<Result<Result<u32, &'static str>, tokio_otp::BlockingCancelled>>,
}

impl RawActor for ReturnsResult {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<()>) -> ActorResult {
        let result = ctx.run_blocking(|_token| Ok::<_, &'static str>(42)).await;
        send_once(&self.observed, result);
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn run_blocking_returns_the_closure_result() {
    let (observed_tx, observed_rx) = oneshot::channel();
    let observed = sender_slot(observed_tx);
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("worker");
    builder.define(actor_slot, move || ReturnsResult {
        observed: observed.clone(),
    });
    let graph = builder.build().expect("valid graph");
    let (stop, task) = start_graph(&graph);

    assert_eq!(observed_rx.await.expect("result observed"), Ok(Ok(42)));
    stop_graph(stop, task).await;
}

#[derive(Clone)]
struct WaitsForShutdown {
    started: Arc<Notify>,
    cancelled: SenderSlot<()>,
}

impl RawActor for WaitsForShutdown {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        let started = self.started.clone();
        let cancelled = self.cancelled.clone();
        ctx.run_blocking(move |token| {
            started.notify_one();
            while !token.is_cancelled() {
                thread::sleep(Duration::from_millis(1));
            }
            send_once(&cancelled, ());
        })
        .await
        .expect("blocking task completes");
        Ok(())
    }
}

#[tokio::test]
async fn run_blocking_token_is_cancelled_on_actor_shutdown() {
    let started = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let cancelled = sender_slot(cancelled_tx);
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("worker");
    builder.define(actor_slot, {
        let started = started.clone();
        move || WaitsForShutdown {
            started: started.clone(),
            cancelled: cancelled.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let (stop, task) = start_graph(&graph);

    started.notified().await;
    stop.cancel();
    timeout(Duration::from_secs(1), cancelled_rx)
        .await
        .expect("blocking closure observed shutdown")
        .expect("cancellation sender remained alive");
    timeout(Duration::from_secs(1), task)
        .await
        .expect("graph stopped in time")
        .expect("graph task joined")
        .expect("graph stopped cleanly");
}

#[derive(Clone)]
struct DropsFuture {
    started: Arc<Notify>,
    drop_future: Arc<Notify>,
    cancelled: SenderSlot<()>,
}

impl RawActor for DropsFuture {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<()>) -> ActorResult {
        let started = self.started.clone();
        let cancelled = self.cancelled.clone();
        {
            let blocking = ctx.run_blocking(move |token| {
                started.notify_one();
                while !token.is_cancelled() {
                    thread::sleep(Duration::from_millis(1));
                }
                send_once(&cancelled, ());
            });
            tokio::pin!(blocking);
            tokio::select! {
                result = &mut blocking => panic!("blocking closure returned before cancellation: {result:?}"),
                () = self.drop_future.notified() => {}
            }
        }

        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[tokio::test]
async fn dropping_run_blocking_future_cancels_its_token() {
    let started = Arc::new(Notify::new());
    let drop_future = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let cancelled = sender_slot(cancelled_tx);
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("worker");
    builder.define(actor_slot, {
        let started = started.clone();
        let drop_future = drop_future.clone();
        move || DropsFuture {
            started: started.clone(),
            drop_future: drop_future.clone(),
            cancelled: cancelled.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let (stop, task) = start_graph(&graph);

    started.notified().await;
    drop_future.notify_one();
    timeout(Duration::from_secs(1), cancelled_rx)
        .await
        .expect("blocking closure observed future drop")
        .expect("cancellation sender remained alive");
    stop_graph(stop, task).await;
}

#[derive(Clone)]
struct IgnoresCancellation {
    started: Arc<Notify>,
    release: Arc<AtomicBool>,
    finished: SenderSlot<()>,
}

impl RawActor for IgnoresCancellation {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        let started = self.started.clone();
        let release = self.release.clone();
        let finished = self.finished.clone();
        ctx.run_blocking(move |_token| {
            started.notify_one();
            while !release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            send_once(&finished, ());
        })
        .await
        .expect("blocking task completes");
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_timeout_backstops_a_closure_that_ignores_cancellation() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(AtomicBool::new(false));
    let (finished_tx, finished_rx) = oneshot::channel();
    let finished = sender_slot(finished_tx);
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("worker");
    builder.define(actor_slot, {
        let started = started.clone();
        let release = release.clone();
        move || IgnoresCancellation {
            started: started.clone(),
            release: release.clone(),
            finished: finished.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors().first().expect("actor exists").clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move {
            actor
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
    let result = timeout(Duration::from_secs(1), task)
        .await
        .expect("graph terminated despite the stuck closure")
        .expect("graph task joined");

    release.store(true, Ordering::Release);
    assert!(matches!(
        result,
        Err(ActorRunError::ShutdownTimedOut { actor_id, .. }) if actor_id == "worker"
    ));
    timeout(Duration::from_secs(1), finished_rx)
        .await
        .expect("detached closure ran to completion")
        .expect("finished sender remained alive");
}

#[derive(Clone)]
struct Panics;

impl RawActor for Panics {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        ctx.run_blocking(|_token| -> () { panic!("blocking panic") })
            .await
            .expect("blocking task should panic before returning");
        Ok(())
    }
}

#[tokio::test]
async fn blocking_panic_propagates_as_actor_panic() {
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("worker");
    builder.define(actor_slot, || Panics);
    let graph = builder.build().expect("valid graph");

    let actor = graph.actors()[0].clone();
    let result = timeout(
        Duration::from_secs(1),
        tokio::spawn(async move {
            actor
                .run_until(
                    pending::<()>(),
                    RestartPolicy::Never,
                    DEFAULT_SHUTDOWN_BOUND,
                )
                .await
        }),
    )
    .await
    .expect("actor panic observed")
    .expect_err("actor task panicked");
    assert!(result.is_panic());
}
