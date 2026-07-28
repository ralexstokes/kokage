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
    Actor, ActorFactory, ActorRef, ActorResult, BoxError, CancellationHandle, GraphBuilder,
    LiveContext, MessageContext, RawActor, Runtime, StartContext, SupervisionTree, TimerKey,
    timers,
};
use tokio_supervisor::Strategy;

fn build_runtime<F>(factory: F) -> (Runtime, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = GraphBuilder::new();
    let (actor_ref_slot, actor_ref) = builder.slot("timer");
    builder.define(actor_ref_slot, factory);
    let graph = builder.build().expect("valid graph");
    let runtime = SupervisionTree::graph(&graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    (runtime, actor_ref)
}

struct OneShot {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for OneShot {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let _timer = ctx.send_after("tick", Duration::from_millis(20));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn send_after_fires_once_without_using_mailbox_capacity() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, actor_ref) = build_runtime(move || OneShot {
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
    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 0);
    assert_eq!(stats.messages_received, 1);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct CancelledTimer {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for CancelledTimer {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let timer = ctx.send_after("cancelled", Duration::from_millis(20));
        timer.cancel();
        assert!(timer.is_cancelled());
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
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

const REPLACEABLE: TimerKey = TimerKey::new("replaceable");

struct ReplaceableTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for ReplaceableTimeout {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        assert!(!ctx.timeout_armed(REPLACEABLE));
        ctx.clear_timeout(REPLACEABLE);
        ctx.set_timeout(REPLACEABLE, "old", Duration::from_millis(20));
        assert!(ctx.timeout_armed(REPLACEABLE));
        ctx.set_timeout(REPLACEABLE, "new", Duration::from_millis(40));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        assert!(!ctx.timeout_armed(REPLACEABLE));
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn setting_a_timeout_replaces_the_previous_entry_at_its_key() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || ReplaceableTimeout {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("replacement timeout fired"),
        Some("new")
    );
    assert!(observed_rx.try_recv().is_err());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrderedMsg {
    Clear,
    Replace,
    Old,
    New,
}

const ORDERED: TimerKey = TimerKey::new("ordered");

struct OrderedTimeout {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<OrderedMsg>,
    replace: bool,
}

impl Actor for OrderedTimeout {
    type Msg = OrderedMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.set_timeout(ORDERED, OrderedMsg::Old, Duration::from_millis(20));
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        match message {
            OrderedMsg::Clear => ctx.clear_timeout(ORDERED),
            OrderedMsg::Replace if self.replace => {
                ctx.set_timeout(ORDERED, OrderedMsg::New, Duration::from_millis(40));
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn queued_pre_fire_message_retracts_an_elapsed_timeout() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (runtime, actor_ref) = build_runtime({
        let release = release.clone();
        move || OrderedTimeout {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
            replace: false,
        }
    });
    let handle = runtime.spawn();
    started_rx.recv().await.expect("actor started");

    actor_ref
        .send(OrderedMsg::Clear)
        .await
        .expect("clear queued");
    advance(Duration::from_millis(20)).await;
    release.notify_one();

    assert_eq!(observed_rx.recv().await, Some(OrderedMsg::Clear));
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "elapsed timeout survived a pre-fire clear"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn rearming_during_the_pre_fire_prefix_suppresses_the_old_entry() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (runtime, actor_ref) = build_runtime({
        let release = release.clone();
        move || OrderedTimeout {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
            replace: true,
        }
    });
    let handle = runtime.spawn();
    started_rx.recv().await.expect("actor started");

    actor_ref
        .send(OrderedMsg::Replace)
        .await
        .expect("replacement queued");
    advance(Duration::from_millis(20)).await;
    release.notify_one();

    assert_eq!(observed_rx.recv().await, Some(OrderedMsg::Replace));
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("replacement fired"),
        Some(OrderedMsg::New)
    );
    assert!(observed_rx.try_recv().is_err());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct KeyedTimeouts {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for KeyedTimeouts {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let first = TimerKey::new("first");
        let second = TimerKey::new("second");
        assert!(!ctx.timeout_armed(first));
        ctx.set_timeout(first, "stale", Duration::from_millis(10));
        ctx.set_timeout(first, "first", Duration::from_millis(20));
        ctx.set_timeout(second, "second", Duration::from_millis(40));
        assert!(ctx.timeout_armed(first));
        assert!(ctx.timeout_armed(second));
        let absent = TimerKey::new("absent");
        ctx.clear_timeout(absent);
        assert!(!ctx.timeout_armed(absent));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        // Delivered payloads intentionally match their timer-key names so this
        // post-delivery check can reconstruct the key.
        let key = TimerKey::new(message);
        assert!(!ctx.timeout_armed(key));
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn keyed_timeouts_replace_per_key_and_remain_independent() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || KeyedTimeouts {
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(observed_rx.recv().await, Some("first"));
    assert_eq!(observed_rx.recv().await, Some("second"));
    assert!(observed_rx.try_recv().is_err());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct FarFutureTimers {
    timers: Vec<CancellationHandle>,
    observed: mpsc::UnboundedSender<&'static str>,
}

const FAR_FUTURE_TIMEOUT: TimerKey = TimerKey::new("far-future");

impl Actor for FarFutureTimers {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.set_timeout(FAR_FUTURE_TIMEOUT, "never-timeout", Duration::MAX);
        self.timers
            .push(ctx.send_after("never-after", Duration::MAX));
        self.timers
            .push(ctx.interval("never-interval", Duration::MAX));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn far_future_delays_saturate_instead_of_panicking() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, actor_ref) = build_runtime(move || FarFutureTimers {
        timers: Vec::new(),
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    actor_ref.send("ping").await.expect("actor alive");
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("ordinary message handled"),
        Some("ping")
    );
    assert!(observed_rx.try_recv().is_err());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct ElapsedCancellation {
    timer: mpsc::UnboundedSender<CancellationHandle>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for ElapsedCancellation {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let timer = ctx.send_after("stale", Duration::from_millis(20));
        self.timer.send(timer).expect("test receives timer");
        self.release.notified().await;
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn send_after_can_be_cancelled_after_its_deadline_until_delivery() {
    let (timer_tx, mut timer_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (runtime, _) = build_runtime({
        let release = release.clone();
        move || ElapsedCancellation {
            timer: timer_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
        }
    });
    let handle = runtime.spawn();

    let timer = timer_rx.recv().await.expect("timer armed");
    advance(Duration::from_millis(20)).await;
    let waiter = timer.clone();
    let cancelled = tokio::spawn(async move { waiter.cancelled().await });
    timer.cancel();
    cancelled.await.expect("cancellation waiter completed");
    release.notify_one();
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "cancelled elapsed timer was delivered"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct IntervalActor {
    observed: mpsc::UnboundedSender<usize>,
    timer: Option<CancellationHandle>,
    ticks: usize,
}

impl Actor for IntervalActor {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.timer = Some(ctx.interval((), Duration::from_millis(10)));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.ticks += 1;
        self.observed.send(self.ticks).expect("observer alive");
        if self.ticks == 3 {
            self.timer.as_ref().expect("timer armed").cancel();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn interval_repeats_until_cancelled() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || IntervalActor {
        observed: observed_tx.clone(),
        timer: None,
        ticks: 0,
    });
    let handle = runtime.spawn();

    for expected in 1..=3 {
        assert_eq!(observed_rx.recv().await, Some(expected));
    }
    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "interval continued after cancellation"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct SlowInterval {
    observed: mpsc::UnboundedSender<Duration>,
    release: Arc<Notify>,
    started: Option<Instant>,
    ticks: usize,
    timer: Option<CancellationHandle>,
}

impl Actor for SlowInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.started = Some(Instant::now());
        self.timer = Some(ctx.interval((), Duration::from_millis(10)));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.ticks += 1;
        self.observed
            .send(Instant::now().duration_since(self.started.expect("started")))
            .expect("observer alive");
        if self.ticks == 1 {
            self.release.notified().await;
        } else if self.ticks == 3 {
            self.timer.as_ref().expect("timer armed").cancel();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn interval_skips_missed_ticks_while_the_handler_is_slow() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (runtime, _) = build_runtime({
        let release = release.clone();
        move || SlowInterval {
            observed: observed_tx.clone(),
            release: release.clone(),
            started: None,
            ticks: 0,
            timer: None,
        }
    });
    let handle = runtime.spawn();

    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(10)));
    advance(Duration::from_millis(100)).await;
    release.notify_one();
    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(110)));
    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(120)));
    assert!(observed_rx.try_recv().is_err(), "missed ticks piled up");

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct RestartingTimer {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<&'static str>,
}

const RESTART_TIMEOUT: TimerKey = TimerKey::new("restart");

impl Actor for RestartingTimer {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            ctx.set_timeout(RESTART_TIMEOUT, "old", Duration::from_millis(150));
            ctx.continue_with("crash");
        } else {
            ctx.set_timeout(RESTART_TIMEOUT, "new", Duration::from_millis(10));
        }
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        if message == "crash" {
            return Err::<_, BoxError>(Box::new(io::Error::other("restart")));
        }
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn restart_drops_the_previous_incarnations_timer_table() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let runs = Arc::new(AtomicUsize::new(0));
    let (runtime, _) = build_runtime(move || RestartingTimer {
        runs: runs.clone(),
        observed: observed_tx.clone(),
    });
    let handle = runtime.spawn();

    assert_eq!(observed_rx.recv().await, Some("new"));
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous incarnation delivered a stale timer"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct Sink {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for Sink {
    type Msg = &'static str;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
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
    let (sink_ref_slot, sink_ref) = builder.slot("sink");
    builder.define(sink_ref_slot, move || Sink {
        observed: observed_tx.clone(),
    });
    let (scheduler_ref_slot, scheduler_ref) = builder.slot("scheduler");
    builder.define(scheduler_ref_slot, scheduler(sink_ref));
    let graph = builder.build().expect("valid graph");
    let runtime = SupervisionTree::graph(&graph)
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    (runtime, scheduler_ref, observed_rx)
}

struct CrossScheduler {
    target: ActorRef<&'static str>,
}

impl Actor for CrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let lifetime = ctx.lifetime();
        let _timer =
            timers::send_after_to(&lifetime, &self.target, "cross", Duration::from_millis(20));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn send_after_to_delivers_through_the_public_target_ref() {
    let (runtime, _, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossScheduler {
            target: target.clone(),
        }
    });
    let handle = runtime.spawn();

    assert_eq!(observed_rx.recv().await, Some("cross"));
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "one-shot cross-actor timer fired twice"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct RestartingCrossScheduler {
    target: ActorRef<&'static str>,
    runs: Arc<AtomicUsize>,
}

impl Actor for RestartingCrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let lifetime = ctx.lifetime();
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            let _old =
                timers::send_after_to(&lifetime, &self.target, "old", Duration::from_millis(150));
            ctx.continue_with(());
        } else {
            let _new =
                timers::send_after_to(&lifetime, &self.target, "new", Duration::from_millis(10));
        }
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Err::<_, BoxError>(Box::new(io::Error::other("restart")))
    }
}

#[tokio::test(start_paused = true)]
async fn restart_ends_cross_actor_timer_lifetime() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (runtime, _, mut observed_rx) = build_cross_runtime(|target| {
        move || RestartingCrossScheduler {
            target: target.clone(),
            runs: runs.clone(),
        }
    });
    let handle = runtime.spawn();

    assert_eq!(observed_rx.recv().await, Some("new"));
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous scheduler incarnation delivered a stale message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct CrossInterval {
    target: ActorRef<&'static str>,
    timer: Option<CancellationHandle>,
}

impl Actor for CrossInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.timer = Some(timers::interval_to(
            &ctx.lifetime(),
            &self.target,
            "tick",
            Duration::from_millis(10),
        ));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.timer.as_ref().expect("timer armed").cancel();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn interval_to_repeats_until_cancelled() {
    let (runtime, scheduler_ref, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossInterval {
            target: target.clone(),
            timer: None,
        }
    });
    let handle = runtime.spawn();

    for _ in 1..=3 {
        assert_eq!(observed_rx.recv().await, Some("tick"));
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
