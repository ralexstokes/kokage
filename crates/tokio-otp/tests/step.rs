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

use tokio::sync::{Notify, mpsc};
use tokio_otp::prelude::*;

#[derive(Debug)]
enum OutcomeMsg {
    Success(Result<u32, StepDeadline>),
    Timeout(Result<(), StepDeadline>),
    OrSuccess(u32),
    OrFallback(u32),
}

#[derive(Clone)]
struct Outcomes {
    observed: mpsc::UnboundedSender<OutcomeMsg>,
}

impl Actor for Outcomes {
    type Msg = OutcomeMsg;

    async fn on_start(&mut self, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        ctx.step(Duration::from_secs(1), async { 42 }, OutcomeMsg::Success);
        ctx.step(
            Duration::from_millis(10),
            pending::<()>(),
            OutcomeMsg::Timeout,
        );
        ctx.step_or(
            Duration::from_secs(1),
            async { 42 },
            0,
            OutcomeMsg::OrSuccess,
        );
        ctx.step_or(
            Duration::from_millis(10),
            pending::<u32>(),
            7,
            OutcomeMsg::OrFallback,
        );
        Ok(Continue)
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &ActorContext<Self::Msg>) -> ActorResult {
        self.observed.send(message).unwrap();
        Ok(Continue)
    }
}

#[tokio::test]
async fn step_and_step_or_post_total_and_fallback_outcomes() {
    let (observed, mut outcomes) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.add(move || Outcomes {
        observed: observed.clone(),
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();

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
            .any(|message| matches!(message, OutcomeMsg::Timeout(Err(StepDeadline))))
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
    runtime.shutdown_and_wait().await.unwrap();
}

#[derive(Debug)]
enum StaleMsg {
    Start,
    Done,
}

struct StaleActor {
    incarnation: usize,
    drop_started: Arc<AtomicBool>,
    release_drop: Arc<AtomicBool>,
    done: Arc<AtomicUsize>,
}

struct SlowDropFuture {
    drop_started: Arc<AtomicBool>,
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

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            StaleMsg::Start => {
                assert_eq!(self.incarnation, 0);
                ctx.step(
                    Duration::from_secs(1),
                    SlowDropFuture {
                        drop_started: self.drop_started.clone(),
                        release_drop: self.release_drop.clone(),
                    },
                    |_| StaleMsg::Done,
                );
                return Err(std::io::Error::other("restart after starting step").into());
            }
            StaleMsg::Done => {
                self.done.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Continue)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn step_is_aborted_and_never_posts_to_a_fresh_incarnation() {
    let constructed = Arc::new(AtomicUsize::new(0));
    let drop_started = Arc::new(AtomicBool::new(false));
    let release_drop = Arc::new(AtomicBool::new(false));
    let _release_on_drop = ReleaseOnDrop(release_drop.clone());
    let done = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let actor = graph.add({
        let constructed = constructed.clone();
        let drop_started = drop_started.clone();
        let release_drop = release_drop.clone();
        let done = done.clone();
        move || StaleActor {
            incarnation: constructed.fetch_add(1, Ordering::Relaxed),
            drop_started: drop_started.clone(),
            release_drop: release_drop.clone(),
            done: done.clone(),
        }
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .restart(RestartPolicy::OnFailure)
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(StaleMsg::Start).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while constructed.load(Ordering::Relaxed) < 2 || !drop_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(actor.stats().outstanding_steps, 0);
    release_drop.store(true, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(done.load(Ordering::Relaxed), 0);
    runtime.shutdown_and_wait().await.unwrap();
}

#[derive(Debug)]
enum AbortMsg {
    Start,
    Done,
}

#[derive(Clone)]
struct AbortActor {
    handle: Arc<Mutex<Option<StepHandle>>>,
    done: Arc<AtomicUsize>,
}

impl Actor for AbortActor {
    type Msg = AbortMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            AbortMsg::Start => {
                let handle = ctx.step(Duration::from_secs(1), pending::<()>(), |_| AbortMsg::Done);
                *self.handle.lock().unwrap() = Some(handle);
            }
            AbortMsg::Done => {
                self.done.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn step_handle_aborts_and_updates_the_outstanding_gauge() {
    let handle_slot = Arc::new(Mutex::new(None));
    let done = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let actor = graph.add({
        let handle_slot = handle_slot.clone();
        let done = done.clone();
        move || AbortActor {
            handle: handle_slot.clone(),
            done: done.clone(),
        }
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(AbortMsg::Start).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while actor.stats().outstanding_steps != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let step = handle_slot.lock().unwrap().take().unwrap();
    step.abort();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !step.is_finished() || actor.stats().outstanding_steps != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(done.load(Ordering::Relaxed), 0);
    runtime.shutdown_and_wait().await.unwrap();
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

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            DrainMsg::Start => {
                let release = self.release.clone();
                let entered = self.entered.clone();
                ctx.step(
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
                ctx.step(Duration::from_secs(1), async {}, |_| DrainMsg::Nested);
            }
            DrainMsg::Nested => self.observed.send("nested").unwrap(),
            DrainMsg::Done => self.observed.send("done").unwrap(),
        }
        Ok(Continue)
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
    let actor = graph.add({
        let release = release.clone();
        let entered = entered.clone();
        move || ShutdownActor {
            policy,
            release: release.clone(),
            entered: entered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(DrainMsg::Start).await.unwrap();
    entered.notified().await;
    actor.send(DrainMsg::Queued).await.unwrap();
    let shutdown = tokio::spawn(async move { runtime.shutdown_and_wait().await });
    tokio::task::yield_now().await;
    if policy == DrainPolicy::Drain {
        release.notify_waiters();
    }
    shutdown.await.unwrap().unwrap();

    let mut values = Vec::new();
    while let Ok(value) = receiver.try_recv() {
        values.push(value);
    }
    values
}

#[tokio::test]
async fn drain_interleaves_a_full_mailbox_with_step_completion() {
    let observed = shutdown_case(DrainPolicy::Drain).await;
    assert_eq!(observed.first(), Some(&"queued"));
    assert!(observed.contains(&"nested"));
    assert!(observed.contains(&"done"));
}

#[tokio::test]
async fn discard_aborts_steps_at_stop_initiation() {
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
    step_release: Arc<Notify>,
    step_registered: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for BackpressureActor {
    type Msg = BackpressureMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            BackpressureMsg::Start => {
                let release = self.step_release.clone();
                ctx.step(
                    Duration::from_secs(1),
                    async move { release.notified().await },
                    |_| BackpressureMsg::Done,
                );
                self.step_registered.notify_one();
                self.handler_release.notified().await;
            }
            BackpressureMsg::Fill => self.observed.send("fill").unwrap(),
            BackpressureMsg::Done => self.observed.send("done").unwrap(),
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn step_postback_uses_mailbox_backpressure() {
    let handler_release = Arc::new(Notify::new());
    let step_release = Arc::new(Notify::new());
    let step_registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let actor = graph.add({
        let handler_release = handler_release.clone();
        let step_release = step_release.clone();
        let step_registered = step_registered.clone();
        move || BackpressureActor {
            handler_release: handler_release.clone(),
            step_release: step_release.clone(),
            step_registered: step_registered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(BackpressureMsg::Start).await.unwrap();
    step_registered.notified().await;
    actor.send(BackpressureMsg::Fill).await.unwrap();
    step_release.notify_one();
    tokio::task::yield_now().await;
    let stats = actor.stats();
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.outstanding_steps, 1);
    handler_release.notify_one();
    assert_eq!(receiver.recv().await, Some("fill"));
    assert_eq!(receiver.recv().await, Some("done"));
    runtime.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn step_postback_uses_conflating_mailbox_policy() {
    let handler_release = Arc::new(Notify::new());
    let step_release = Arc::new(Notify::new());
    let step_registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let actor = graph.actor_with_options(
        "conflating-step",
        {
            let handler_release = handler_release.clone();
            let step_release = step_release.clone();
            let step_registered = step_registered.clone();
            move || BackpressureActor {
                handler_release: handler_release.clone(),
                step_release: step_release.clone(),
                step_registered: step_registered.clone(),
                observed: observed.clone(),
            }
        },
        ActorOptions::new().mailbox(MailboxMode::Conflate),
    );
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(BackpressureMsg::Start).await.unwrap();
    step_registered.notified().await;
    actor.send(BackpressureMsg::Fill).await.unwrap();
    step_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while actor.stats().messages_conflated != 1 || actor.stats().outstanding_steps != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handler_release.notify_one();
    assert_eq!(receiver.recv().await, Some("done"));
    assert!(receiver.try_recv().is_err());
    runtime.shutdown_and_wait().await.unwrap();
}

#[derive(Debug)]
enum DeadlineDrainMsg {
    Start,
    Done(Result<(), StepDeadline>),
}

#[derive(Clone)]
struct DeadlineDrainActor {
    registered: Arc<Notify>,
    observed: mpsc::UnboundedSender<Result<(), StepDeadline>>,
}

impl Actor for DeadlineDrainActor {
    type Msg = DeadlineDrainMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            DeadlineDrainMsg::Start => {
                ctx.step(
                    Duration::from_millis(100),
                    pending::<()>(),
                    DeadlineDrainMsg::Done,
                );
                self.registered.notify_one();
            }
            DeadlineDrainMsg::Done(outcome) => self.observed.send(outcome).unwrap(),
        }
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_waits_for_step_deadline_and_handles_its_postback() {
    let registered = Arc::new(Notify::new());
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let actor = graph.add({
        let registered = registered.clone();
        move || DeadlineDrainActor {
            registered: registered.clone(),
            observed: observed.clone(),
        }
    });
    let runtime = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    runtime.wait_started().await.unwrap();
    actor.send(DeadlineDrainMsg::Start).await.unwrap();
    registered.notified().await;
    let shutdown = tokio::spawn(async move { runtime.shutdown_and_wait().await });
    let outcome = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, Err(StepDeadline)));
    shutdown.await.unwrap().unwrap();
}
