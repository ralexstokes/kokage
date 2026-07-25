use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc},
    time::{Instant, advance, timeout},
};
use tokio_otp::{
    ActorContext, ActorFactory, ActorRef, ActorResult, BoxError, GraphBuilder, RawActor, Runtime,
    prelude::Continue,
};
use tokio_supervisor::Strategy;

fn build_runtime<F>(factory: F) -> (Runtime, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor("timer", factory);
    let graph = builder.build().expect("valid graph");
    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    (runtime, actor_ref)
}

#[derive(Clone)]
struct OneShot {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for OneShot {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let _timer = ctx.send_after("tick", Duration::from_millis(20));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn send_after_fires_once() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || OneShot {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("timer fired"),
        Some("tick")
    );
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "one-shot timer must not fire twice"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct CancelledTimer {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for CancelledTimer {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let timer = ctx.send_after("cancelled", Duration::from_millis(20));
        timer.cancel();
        assert!(timer.is_cancelled());
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_send_after_prevents_delivery() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || CancelledTimer {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "cancelled timer delivered a message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct StateTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for StateTimeout {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let _timer = ctx.state_timeout("timeout", Duration::from_millis(20));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn state_timeout_fires() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || StateTimeout {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("state timeout fired"),
        Some("timeout")
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct ReplacedStateTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for ReplacedStateTimeout {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let old = ctx.state_timeout("old", Duration::from_millis(20));
        let _new = ctx.state_timeout("new", Duration::from_millis(40));
        assert!(old.is_cancelled());

        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn new_state_timeout_cancels_previous_timeout() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || ReplacedStateTimeout {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("replacement state timeout fired"),
        Some("new")
    );
    assert!(
        timeout(Duration::from_millis(40), observed_rx.recv())
            .await
            .is_err(),
        "replaced state timeout delivered a stale message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct QueuedStateTimeoutReplacement {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for QueuedStateTimeoutReplacement {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.state_timeout("old", Duration::from_millis(20));
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;

        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
            if message == "replace" {
                ctx.state_timeout("new", Duration::from_millis(40));
            }
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn replacing_state_timeout_filters_already_queued_message() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (runtime, actor_ref) = build_runtime({
        let release = release.clone();
        move || QueuedStateTimeoutReplacement {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
        }
    });
    let handle = runtime.spawn();
    started_rx.recv().await.expect("actor started");

    actor_ref.send("replace").await.expect("replacement queued");
    advance(Duration::from_millis(20)).await;
    for _ in 0..10 {
        if actor_ref.stats().messages_accepted >= 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(actor_ref.stats().messages_accepted, 2);
    release.notify_one();

    assert_eq!(observed_rx.recv().await, Some("replace"));
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("replacement timeout fired"),
        Some("new")
    );
    assert!(observed_rx.try_recv().is_err(), "stale timeout was handled");

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct ClearedStateTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for ClearedStateTimeout {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let timer = ctx.state_timeout("stale", Duration::from_millis(20));
        ctx.clear_state_timeout();
        assert!(timer.is_cancelled());

        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn clearing_state_timeout_prevents_delivery() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || ClearedStateTimeout {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "cleared state timeout delivered a message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct SequentialStateTimeouts {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for SequentialStateTimeouts {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.state_timeout("first", Duration::from_millis(20));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
            if message == "first" {
                ctx.state_timeout("second", Duration::from_millis(20));
            }
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn state_timeout_can_be_rescheduled_after_firing() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || SequentialStateTimeouts {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("first state timeout fired"),
        Some("first")
    );
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("second state timeout fired"),
        Some("second")
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct ClearsEmptyStateTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for ClearsEmptyStateTimeout {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.clear_state_timeout();
        ctx.state_timeout("timeout", Duration::from_millis(20));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn clearing_empty_state_timeout_is_a_noop() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || ClearsEmptyStateTimeout {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("state timeout fired after empty clear"),
        Some("timeout")
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct Interval {
    observed: mpsc::UnboundedSender<usize>,
}

impl RawActor for Interval {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let timer = ctx.interval((), Duration::from_millis(10));
        let mut ticks = 0;
        while ctx.recv().await.is_some() {
            ticks += 1;
            self.observed.send(ticks).expect("observer alive");
            if ticks == 3 {
                timer.cancel();
            }
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn interval_repeats_until_cancelled() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || Interval {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    for expected in 1..=3 {
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("interval ticked"),
            Some(expected)
        );
    }
    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "interval continued after cancellation"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct RestartingTimer {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for RestartingTimer {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            let _old_timer = ctx.send_after("old", Duration::from_millis(150));
            return Err::<_, BoxError>(Box::new(io::Error::other("restart")));
        }

        let _new_timer = ctx.send_after("new", Duration::from_millis(10));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn restart_cancels_previous_incarnations_timers() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let runs = Arc::new(AtomicUsize::new(0));
    let (runtime, _) = build_runtime(move || RestartingTimer {
        runs: runs.clone(),
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("new incarnation timer fired"),
        Some("new")
    );
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous incarnation delivered a stale timer message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct RestartingStateTimeout {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for RestartingStateTimeout {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            let _old = ctx.state_timeout("old", Duration::from_millis(150));
            return Err::<_, BoxError>(Box::new(io::Error::other("restart")));
        }

        let _new = ctx.state_timeout("new", Duration::from_millis(10));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn restart_cancels_previous_incarnations_state_timeout() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let runs = Arc::new(AtomicUsize::new(0));
    let (runtime, _) = build_runtime(move || RestartingStateTimeout {
        runs: runs.clone(),
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("new incarnation state timeout fired"),
        Some("new")
    );
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous incarnation delivered a stale state timeout message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackpressureMsg {
    Occupied,
    Tick,
}

#[derive(Clone)]
struct BackpressureInterval {
    ready: Arc<Notify>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<Vec<(BackpressureMsg, Duration)>>,
}

impl RawActor for BackpressureInterval {
    type Msg = BackpressureMsg;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let started = Instant::now();
        let _timer = ctx.interval(BackpressureMsg::Tick, Duration::from_millis(10));
        self.ready.notify_one();
        self.release.notified().await;

        let mut messages = Vec::new();
        for _ in 0..4 {
            let message = ctx.recv().await.expect("message before shutdown");
            messages.push((message, Instant::now().duration_since(started)));
        }
        self.observed.send(messages).expect("observer alive");

        while ctx.recv().await.is_some() {}
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn interval_waits_for_mailbox_capacity_and_skips_missed_ticks() {
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    builder.mailbox_capacity(1);
    let actor_ref = builder.actor("timer", {
        let ready = ready.clone();
        let release = release.clone();
        move || BackpressureInterval {
            ready: ready.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();

    ready.notified().await;
    actor_ref
        .send(BackpressureMsg::Occupied)
        .await
        .expect("mailbox filled");
    advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;

    let stats = actor_ref.stats();
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.messages_accepted, 1, "timer must await capacity");

    release.notify_one();
    let messages = observed_rx.recv().await.expect("actor reported messages");
    assert_eq!(
        messages
            .iter()
            .map(|(message, _)| *message)
            .collect::<Vec<_>>(),
        vec![
            BackpressureMsg::Occupied,
            BackpressureMsg::Tick,
            BackpressureMsg::Tick,
            BackpressureMsg::Tick,
        ]
    );
    assert_eq!(messages[0].1, Duration::from_millis(100));
    assert_eq!(messages[1].1, Duration::from_millis(100));
    assert_eq!(
        messages[2].1,
        Duration::from_millis(110),
        "missed ticks must not burst after capacity becomes available"
    );
    assert_eq!(messages[3].1, Duration::from_millis(120));

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct Sink {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for Sink {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        Ok(Continue)
    }
}

fn build_cross_runtime<F>(
    scheduler: impl FnOnce(ActorRef<&'static str>) -> F,
) -> (
    Runtime,
    ActorRef<<F::Actor as RawActor>::Msg>,
    mpsc::UnboundedReceiver<&'static str>,
)
where
    F: ActorFactory,
{
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let sink_ref = builder.actor("sink", move || Sink {
        observed: observed_tx.clone(),
    });
    let scheduler_ref = builder.actor("scheduler", scheduler(sink_ref));
    let graph = builder.build().expect("valid graph");
    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    (runtime, scheduler_ref, observed_rx)
}

#[derive(Clone)]
struct CrossScheduler {
    target: ActorRef<&'static str>,
}

impl RawActor for CrossScheduler {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let _timer = ctx.send_after_to(&self.target, "cross", Duration::from_millis(20));
        while ctx.recv().await.is_some() {}
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn send_after_to_delivers_to_the_target_actor() {
    let (runtime, _, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossScheduler {
            target: target.clone(),
        }
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("cross-actor timer fired"),
        Some("cross")
    );
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "one-shot cross-actor timer must not fire twice"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct RestartingCrossScheduler {
    target: ActorRef<&'static str>,
    runs: Arc<AtomicUsize>,
}

impl RawActor for RestartingCrossScheduler {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            let _old = ctx.send_after_to(&self.target, "old", Duration::from_millis(150));
            return Err::<_, BoxError>(Box::new(io::Error::other("restart")));
        }

        let _new = ctx.send_after_to(&self.target, "new", Duration::from_millis(10));
        while ctx.recv().await.is_some() {}
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn restart_cancels_previous_incarnations_cross_actor_timer() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (runtime, _, mut observed_rx) = build_cross_runtime(|target| {
        move || RestartingCrossScheduler {
            target: target.clone(),
            runs: runs.clone(),
        }
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("new incarnation cross-actor timer fired"),
        Some("new")
    );
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous scheduler incarnation delivered a stale cross-actor message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone)]
struct CrossInterval {
    target: ActorRef<&'static str>,
}

impl RawActor for CrossInterval {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let timer = ctx.interval_to(&self.target, "tick", Duration::from_millis(10));
        while ctx.recv().await.is_some() {
            timer.cancel();
        }
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn interval_to_repeats_until_cancelled() {
    let (runtime, scheduler_ref, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossInterval {
            target: target.clone(),
        }
    });
    let handle = runtime.spawn();

    for _ in 1..=3 {
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("cross-actor interval ticked"),
            Some("tick")
        );
    }
    scheduler_ref.send(()).await.expect("scheduler alive");
    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "cross-actor interval continued after cancellation"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
