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
    LiveContext, MessageContext, RawActor, Runtime, StartContext, TimerKey, prelude::Continue,
    timers,
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

struct OneShot {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for OneShot {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let _timer = ctx.send_after("tick", Duration::from_millis(20));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(Continue)
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let timer = ctx.send_after("cancelled", Duration::from_millis(20));
        timer.cancel();
        assert!(timer.is_cancelled());
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
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

struct DefaultTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for DefaultTimeout {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        assert!(!ctx.timeout_armed());
        ctx.clear_timeout();
        ctx.set_timeout("old", Duration::from_millis(20));
        assert!(ctx.timeout_armed());
        ctx.set_timeout("new", Duration::from_millis(40));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        assert!(!ctx.timeout_armed());
        self.observed.send(message).expect("observer alive");
        Ok(Continue)
    }
}

#[tokio::test(start_paused = true)]
async fn setting_default_timeout_replaces_the_previous_entry() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (runtime, _) = build_runtime(move || DefaultTimeout {
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

struct OrderedTimeout {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<OrderedMsg>,
    replace: bool,
}

impl Actor for OrderedTimeout {
    type Msg = OrderedMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        ctx.set_timeout(OrderedMsg::Old, Duration::from_millis(20));
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        match message {
            OrderedMsg::Clear => ctx.clear_timeout(),
            OrderedMsg::Replace if self.replace => {
                ctx.set_timeout(OrderedMsg::New, Duration::from_millis(40));
            }
            _ => {}
        }
        Ok(Continue)
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let first = TimerKey::new("first");
        let second = TimerKey::new("second");
        ctx.set_timeout_keyed(first, "stale", Duration::from_millis(10));
        ctx.set_timeout_keyed(first, "first", Duration::from_millis(20));
        ctx.set_timeout_keyed(second, "second", Duration::from_millis(40));
        ctx.clear_timeout_keyed(TimerKey::new("absent"));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(Continue)
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

struct ElapsedCancellation {
    timer: mpsc::UnboundedSender<CancellationHandle>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for ElapsedCancellation {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let timer = ctx.send_after("stale", Duration::from_millis(20));
        self.timer.send(timer).expect("test receives timer");
        self.release.notified().await;
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
        Ok(Continue)
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
    timer.cancel();
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.timer = Some(ctx.interval((), Duration::from_millis(10)));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        (): Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.ticks += 1;
        self.observed.send(self.ticks).expect("observer alive");
        if self.ticks == 3 {
            self.timer.as_ref().expect("timer armed").cancel();
        }
        Ok(Continue)
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.started = Some(Instant::now());
        self.timer = Some(ctx.interval((), Duration::from_millis(10)));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        (): Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.ticks += 1;
        self.observed
            .send(Instant::now().duration_since(self.started.expect("started")))
            .expect("observer alive");
        if self.ticks == 1 {
            self.release.notified().await;
        } else if self.ticks == 3 {
            self.timer.as_ref().expect("timer armed").cancel();
        }
        Ok(Continue)
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

impl Actor for RestartingTimer {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            ctx.set_timeout("old", Duration::from_millis(150));
            ctx.continue_with("crash");
        } else {
            ctx.set_timeout("new", Duration::from_millis(10));
        }
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        if message == "crash" {
            return Err::<_, BoxError>(Box::new(io::Error::other("restart")));
        }
        self.observed.send(message).expect("observer alive");
        Ok(Continue)
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
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.observed.send(message).expect("observer alive");
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

struct CrossScheduler {
    target: ActorRef<&'static str>,
}

impl Actor for CrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let lifetime = ctx.lifetime();
        let _timer =
            timers::send_after_to(&lifetime, &self.target, "cross", Duration::from_millis(20));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        (): Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        Ok(Continue)
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        let lifetime = ctx.lifetime();
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            let _old =
                timers::send_after_to(&lifetime, &self.target, "old", Duration::from_millis(150));
            ctx.continue_with(());
        } else {
            let _new =
                timers::send_after_to(&lifetime, &self.target, "new", Duration::from_millis(10));
        }
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        (): Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        self.timer = Some(timers::interval_to(
            &ctx.lifetime(),
            &self.target,
            "tick",
            Duration::from_millis(10),
        ));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        (): Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        self.timer.as_ref().expect("timer armed").cancel();
        Ok(Continue)
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

#[tokio::test]
async fn public_cancellation_handle_can_be_awaited() {
    let cancellation = CancellationHandle::new();
    let waiter = cancellation.clone();
    let joined = tokio::spawn(async move { waiter.cancelled().await });
    cancellation.cancel();
    joined.await.expect("waiter completed");
}
