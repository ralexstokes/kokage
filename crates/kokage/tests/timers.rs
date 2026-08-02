use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorFactory, ActorRef, ActorSlot, ActorSpec, BoxError, Context, ExitResult, Guard,
    Strategy, TimerKey, Tree,
    raw::{RawActor, RawContext},
};
use tokio::{
    sync::{Notify, mpsc},
    time::{Instant, advance, timeout},
};

fn build_runtime<F>(factory: F) -> (Tree, ActorRef<<F::Actor as RawActor>::Msg>)
where
    F: ActorFactory,
{
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new("timer", factory));
    let tree = builder.strategy(Strategy::OneForOne);
    (tree, actor_ref)
}

struct OneShot {
    observed: mpsc::UnboundedSender<&'static str>,
}

const ONE_SHOT: TimerKey = TimerKey::new("one-shot");

impl Actor for OneShot {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(ONE_SHOT, "tick", Duration::from_millis(20));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn keyed_timeout_fires_once_without_using_mailbox_capacity() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || OneShot {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

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

    running_tree.shutdown().await.expect("clean shutdown");
}

struct MailboxOneShot {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for MailboxOneShot {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.send_after_to(&ctx.myself(), "tick", Duration::from_millis(20))
            .detach();
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn context_send_after_to_can_deliver_once_through_the_self_mailbox() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || MailboxOneShot {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("tick"));
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "one-shot mailbox timer must not fire twice"
    );
    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 1);
    assert_eq!(stats.messages_received, 1);

    running_tree.shutdown().await.expect("clean shutdown");
}

struct ChainedTimeout {
    ticks: usize,
    observed: mpsc::UnboundedSender<usize>,
}

const CHAINED: TimerKey = TimerKey::new("chained");

impl Actor for ChainedTimeout {
    type Msg = usize;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(CHAINED, 1, Duration::from_millis(20));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        if message < self.ticks {
            ctx.set_timeout(CHAINED, message + 1, Duration::from_millis(20));
        }
        Ok(())
    }
}

/// Repeated one-shot self scheduling re-arms the same key from inside the
/// handler the fired timeout delivered to.
#[tokio::test(start_paused = true)]
async fn rearming_a_timeout_from_its_own_handler_fires_it_again() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || ChainedTimeout {
        ticks: 3,
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    for expected in 1..=3 {
        assert_eq!(
            timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .expect("chained timeout fired"),
            Some(expected)
        );
    }
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "the chain must stop once the handler stops re-arming"
    );
    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 0);
    assert_eq!(stats.messages_received, 3);

    running_tree.shutdown().await.expect("clean shutdown");
}

struct CancelledTimer {
    observed: mpsc::UnboundedSender<&'static str>,
}

const CANCELLED: TimerKey = TimerKey::new("cancelled");

impl Actor for CancelledTimer {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(CANCELLED, "cancelled", Duration::from_millis(20));
        ctx.clear_timer(CANCELLED);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn clearing_a_keyed_timeout_prevents_delivery() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, _) = build_runtime(move || CancelledTimer {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "cancelled timer delivered a message"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

const REPLACEABLE: TimerKey = TimerKey::new("replaceable");

struct ReplaceableTimeout {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for ReplaceableTimeout {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.clear_timer(REPLACEABLE);
        ctx.set_timeout(REPLACEABLE, "old", Duration::from_millis(20));
        ctx.set_timeout(REPLACEABLE, "new", Duration::from_millis(40));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn setting_a_timeout_replaces_the_previous_entry_at_its_key() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, _) = build_runtime(move || ReplaceableTimeout {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("replacement timeout fired"),
        Some("new")
    );
    assert!(observed_rx.try_recv().is_err());

    running_tree.shutdown().await.expect("clean shutdown");
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
    repeat: bool,
}

impl Actor for OrderedTimeout {
    type Msg = OrderedMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.repeat {
            ctx.set_interval(ORDERED, OrderedMsg::Old, Duration::from_millis(20));
        } else {
            ctx.set_timeout(ORDERED, OrderedMsg::Old, Duration::from_millis(20));
        }
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        match message {
            OrderedMsg::Clear => ctx.clear_timer(ORDERED),
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
    let (tree, actor_ref) = build_runtime({
        let release = release.clone();
        move || OrderedTimeout {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
            replace: false,
            repeat: false,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");
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

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn queued_pre_fire_message_retracts_an_elapsed_interval() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (tree, actor_ref) = build_runtime({
        let release = release.clone();
        move || OrderedTimeout {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
            replace: false,
            repeat: true,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");
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
        "elapsed interval survived a pre-fire clear"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn rearming_during_the_pre_fire_prefix_suppresses_the_old_entry() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (tree, actor_ref) = build_runtime({
        let release = release.clone();
        move || OrderedTimeout {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
            replace: true,
            repeat: false,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");
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

    running_tree.shutdown().await.expect("clean shutdown");
}

struct KeyedTimeouts {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for KeyedTimeouts {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let first = TimerKey::new("first");
        let second = TimerKey::new("second");
        ctx.set_timeout(first, "stale", Duration::from_millis(10));
        ctx.set_timeout(first, "first", Duration::from_millis(20));
        ctx.set_timeout(second, "second", Duration::from_millis(40));
        let absent = TimerKey::new("absent");
        ctx.clear_timer(absent);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn keyed_timeouts_replace_per_key_and_remain_independent() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, _) = build_runtime(move || KeyedTimeouts {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("first"));
    assert_eq!(observed_rx.recv().await, Some("second"));
    assert!(observed_rx.try_recv().is_err());

    running_tree.shutdown().await.expect("clean shutdown");
}

struct FarFutureTimers {
    observed: mpsc::UnboundedSender<&'static str>,
}

const FAR_FUTURE_TIMEOUT: TimerKey = TimerKey::new("far-future");
const FAR_FUTURE_INTERVAL: TimerKey = TimerKey::new("far-future-interval");

impl Actor for FarFutureTimers {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(FAR_FUTURE_TIMEOUT, "never-timeout", Duration::MAX);
        ctx.set_interval(FAR_FUTURE_INTERVAL, "never-interval", Duration::MAX);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn far_future_delays_saturate_instead_of_panicking() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || FarFutureTimers {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    actor_ref.send("ping").await.expect("actor alive");
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("ordinary message handled"),
        Some("ping")
    );
    assert!(observed_rx.try_recv().is_err());

    running_tree.shutdown().await.expect("clean shutdown");
}

struct IntervalActor {
    observed: mpsc::UnboundedSender<usize>,
    ticks: usize,
}

const SELF_INTERVAL: TimerKey = TimerKey::new("self-interval");

impl Actor for IntervalActor {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_interval(SELF_INTERVAL, (), Duration::from_millis(10));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.ticks += 1;
        self.observed.send(self.ticks).expect("observer alive");
        if self.ticks == 3 {
            ctx.clear_timer(SELF_INTERVAL);
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn keyed_interval_repeats_until_cleared_without_mailbox_acceptance() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || IntervalActor {
        observed: observed_tx.clone(),
        ticks: 0,
    });
    let running_tree = tree.spawn().expect("runtime builds");

    for expected in 1..=3 {
        assert_eq!(observed_rx.recv().await, Some(expected));
    }
    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "interval continued after cancellation"
    );
    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 0);
    assert_eq!(stats.messages_received, 3);

    running_tree.shutdown().await.expect("clean shutdown");
}

struct ReplacedTimerKinds {
    observed: mpsc::UnboundedSender<&'static str>,
}

const REPLACED_TIMER_KIND: TimerKey = TimerKey::new("replaced-timer-kind");

impl Actor for ReplacedTimerKinds {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(REPLACED_TIMER_KIND, "old-timeout", Duration::from_millis(5));
        ctx.set_interval(REPLACED_TIMER_KIND, "tick", Duration::from_millis(10));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        if message == "tick" {
            ctx.set_timeout(
                REPLACED_TIMER_KIND,
                "final-timeout",
                Duration::from_millis(20),
            );
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_and_interval_replace_each_other_at_the_same_key() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, _) = build_runtime(move || ReplacedTimerKinds {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("tick"));
    assert_eq!(observed_rx.recv().await, Some("final-timeout"));
    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "a replaced timer survived at the shared key"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntervalPhaseMsg {
    Replace,
    Old,
    New,
}

struct ReplaceableInterval {
    started: mpsc::UnboundedSender<()>,
    observed: mpsc::UnboundedSender<IntervalPhaseMsg>,
}

const REPLACEABLE_INTERVAL: TimerKey = TimerKey::new("replaceable-interval");

impl Actor for ReplaceableInterval {
    type Msg = IntervalPhaseMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_interval(
            REPLACEABLE_INTERVAL,
            IntervalPhaseMsg::Old,
            Duration::from_millis(20),
        );
        self.started.send(()).expect("test receives start signal");
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            IntervalPhaseMsg::Replace => ctx.set_interval(
                REPLACEABLE_INTERVAL,
                IntervalPhaseMsg::New,
                Duration::from_millis(20),
            ),
            IntervalPhaseMsg::Old | IntervalPhaseMsg::New => {
                ctx.clear_timer(REPLACEABLE_INTERVAL);
            }
        }
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn replacing_an_interval_resets_its_phase() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || ReplaceableInterval {
        started: started_tx.clone(),
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");
    started_rx.recv().await.expect("actor started");

    advance(Duration::from_millis(10)).await;
    actor_ref
        .send(IntervalPhaseMsg::Replace)
        .await
        .expect("replacement queued");
    assert_eq!(observed_rx.recv().await, Some(IntervalPhaseMsg::Replace));
    assert!(
        timeout(Duration::from_millis(19), observed_rx.recv())
            .await
            .is_err(),
        "replacement interval retained the old phase"
    );
    assert_eq!(observed_rx.recv().await, Some(IntervalPhaseMsg::New));

    running_tree.shutdown().await.expect("clean shutdown");
}

struct ZeroPeriodKeyedInterval {
    observed: mpsc::UnboundedSender<&'static str>,
}

const ZERO_PERIOD_KEYED_INTERVAL: TimerKey = TimerKey::new("zero-period-keyed-interval");

impl Actor for ZeroPeriodKeyedInterval {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(
            ZERO_PERIOD_KEYED_INTERVAL,
            "old-timeout",
            Duration::from_millis(10),
        );
        ctx.set_interval(ZERO_PERIOD_KEYED_INTERVAL, "never", Duration::ZERO);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn zero_period_keyed_interval_clears_the_existing_timer() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, actor_ref) = build_runtime(move || ZeroPeriodKeyedInterval {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "zero-period interval left a timer armed"
    );
    actor_ref.send("ping").await.expect("actor remains alive");
    assert_eq!(observed_rx.recv().await, Some("ping"));

    running_tree.shutdown().await.expect("clean shutdown");
}

struct SlowInterval {
    observed: mpsc::UnboundedSender<Duration>,
    release: Arc<Notify>,
    started: Option<Instant>,
    ticks: usize,
}

impl Actor for SlowInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.started = Some(Instant::now());
        ctx.set_interval(SELF_INTERVAL, (), Duration::from_millis(10));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.ticks += 1;
        self.observed
            .send(Instant::now().duration_since(self.started.expect("started")))
            .expect("observer alive");
        if self.ticks == 1 {
            self.release.notified().await;
        } else if self.ticks == 3 {
            ctx.clear_timer(SELF_INTERVAL);
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn interval_skips_missed_ticks_while_the_handler_is_slow() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (tree, _) = build_runtime({
        let release = release.clone();
        move || SlowInterval {
            observed: observed_tx.clone(),
            release: release.clone(),
            started: None,
            ticks: 0,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(10)));
    advance(Duration::from_millis(100)).await;
    release.notify_one();
    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(110)));
    assert_eq!(observed_rx.recv().await, Some(Duration::from_millis(120)));
    assert!(observed_rx.try_recv().is_err(), "missed ticks piled up");

    running_tree.shutdown().await.expect("clean shutdown");
}

struct RestartingTimer {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<&'static str>,
}

const RESTART_TIMEOUT: TimerKey = TimerKey::new("restart-timeout");
const RESTART_INTERVAL: TimerKey = TimerKey::new("restart-interval");

impl Actor for RestartingTimer {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            ctx.set_timeout(RESTART_TIMEOUT, "old-timeout", Duration::from_millis(150));
            ctx.set_interval(RESTART_INTERVAL, "old-interval", Duration::from_millis(150));
            ctx.continue_with("crash");
        } else {
            ctx.set_timeout(RESTART_TIMEOUT, "new", Duration::from_millis(10));
        }
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
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
    let (tree, _) = build_runtime(move || RestartingTimer {
        runs: runs.clone(),
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("new"));
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous incarnation delivered a stale timer"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

struct Sink {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for Sink {
    type Msg = &'static str;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer alive");
        Ok(())
    }
}

fn build_cross_runtime<F>(
    scheduler: impl FnOnce(ActorRef<&'static str>) -> F,
) -> (
    Tree,
    ActorRef<<F::Actor as RawActor>::Msg>,
    mpsc::UnboundedReceiver<&'static str>,
)
where
    F: ActorFactory,
{
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let mut builder = Tree::new();
    let sink_ref = builder.add_actor_spec(ActorSpec::new("sink", move || Sink {
        observed: observed_tx.clone(),
    }));
    let scheduler_ref = builder.add_actor_spec(ActorSpec::new("scheduler", scheduler(sink_ref)));
    let tree = builder.strategy(Strategy::OneForOne);
    (tree, scheduler_ref, observed_rx)
}

struct CrossScheduler {
    target: ActorRef<&'static str>,
}

impl Actor for CrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.send_after_to(&self.target, "cross", Duration::from_millis(20))
            .detach();
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct DroppedCrossScheduler {
    target: ActorRef<&'static str>,
}

impl Actor for DroppedCrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        drop(ctx.send_after_to(&self.target, "dropped", Duration::from_millis(20)));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct RawCrossScheduler {
    target: ActorRef<&'static str>,
}

impl RawActor for RawCrossScheduler {
    type Msg = ();

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        let timer = ctx.send_after_to(&self.target, "raw-cross", Duration::from_millis(20));
        let interval = ctx.interval_to(&self.target, "raw-cross-tick", Duration::from_millis(30));
        assert!(
            !timer.is_finished(),
            "a pending delay has not finished before it elapses"
        );
        while ctx.recv().await.is_some() {}
        // Hold the interval guard for the life of the receive loop.
        drop(interval);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn raw_context_can_schedule_cross_actor_timers() {
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || RawCrossScheduler {
            target: target.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("raw-cross"));
    assert_eq!(observed_rx.recv().await, Some("raw-cross-tick"));
    running_tree.shutdown().await.expect("clean shutdown");
}

struct RawSelfScheduler {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for RawSelfScheduler {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        let one_shot = ctx.send_after("raw-self", Duration::from_millis(20));
        let interval = ctx.interval("raw-self-tick", Duration::from_millis(30));
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("observer alive");
        }
        // Hold both guards for the life of the receive loop.
        drop((one_shot, interval));
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn raw_context_can_schedule_self_timers() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (tree, _) = build_runtime(move || RawSelfScheduler {
        observed: observed_tx.clone(),
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("raw-self"));
    assert_eq!(observed_rx.recv().await, Some("raw-self-tick"));
    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn detached_send_after_delivers_through_the_public_target_ref() {
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossScheduler {
            target: target.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("cross"));
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "one-shot cross-actor timer fired twice"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn dropped_send_after_guard_cancels_delivery() {
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || DroppedCrossScheduler {
            target: target.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "a dropped send-after guard delivered its message"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

struct RestartingCrossScheduler {
    target: ActorRef<&'static str>,
    runs: Arc<AtomicUsize>,
}

impl Actor for RestartingCrossScheduler {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            ctx.send_after_to(&self.target, "old", Duration::from_millis(150))
                .detach();
            ctx.continue_with(());
        } else {
            ctx.send_after_to(&self.target, "new", Duration::from_millis(10))
                .detach();
        }
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Err::<_, BoxError>(Box::new(io::Error::other("restart")))
    }
}

#[tokio::test(start_paused = true)]
async fn restart_ends_cross_actor_timer_lifetime() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || RestartingCrossScheduler {
            target: target.clone(),
            runs: runs.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert_eq!(observed_rx.recv().await, Some("new"));
    assert!(
        timeout(Duration::from_millis(200), observed_rx.recv())
            .await
            .is_err(),
        "a previous scheduler incarnation delivered a stale message"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

struct CrossInterval {
    target: ActorRef<&'static str>,
    timer: Option<Guard>,
}

struct DroppedCrossInterval {
    target: ActorRef<&'static str>,
}

struct DetachedCrossInterval {
    target: ActorRef<&'static str>,
}

struct ReportedCrossInterval {
    target: ActorRef<&'static str>,
    guards: mpsc::UnboundedSender<Guard>,
    period: Duration,
}

impl Actor for DroppedCrossInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        drop(ctx.interval_to(&self.target, "dropped-tick", Duration::from_millis(10)));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

impl Actor for DetachedCrossInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.interval_to(&self.target, "detached-tick", Duration::from_millis(10))
            .detach();
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

impl Actor for ReportedCrossInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let guard = ctx.interval_to(&self.target, "tick", self.period);
        self.guards.send(guard).expect("guard receiver alive");
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

impl Actor for CrossInterval {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.timer = Some(ctx.interval_to(&self.target, "tick", Duration::from_millis(10)));
        Ok(())
    }

    async fn handle(&mut self, (): Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.timer.as_ref().expect("timer armed").cancel();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn cross_actor_interval_repeats_until_cancelled() {
    let (tree, scheduler_ref, mut observed_rx) = build_cross_runtime(|target| {
        move || CrossInterval {
            target: target.clone(),
            timer: None,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

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

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn detached_interval_keeps_delivering() {
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || DetachedCrossInterval {
            target: target.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    for _ in 1..=3 {
        assert_eq!(observed_rx.recv().await, Some("detached-tick"));
    }

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn target_termination_finishes_interval_without_cancelling_it() {
    let (guard_tx, mut guard_rx) = mpsc::unbounded_channel();
    let target_slot = ActorSlot::new("terminated-target");
    let target = target_slot.actor_ref();
    drop(target_slot);
    let mut builder = Tree::new();
    builder.add_actor_spec(ActorSpec::new("scheduler", {
        let target = target.clone();
        move || ReportedCrossInterval {
            target: target.clone(),
            guards: guard_tx.clone(),
            period: Duration::from_millis(10),
        }
    }));
    let running_tree = builder
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");
    let guard = guard_rx.recv().await.expect("scheduler reports guard");
    assert!(
        !guard.is_finished(),
        "interval stays live until the target terminates"
    );

    advance(Duration::from_millis(10)).await;
    timeout(Duration::from_secs(1), guard.finished())
        .await
        .expect("target termination finishes interval");
    assert!(
        !guard.is_cancelled(),
        "target termination is environmental, not explicit cancellation"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn zero_period_interval_is_finished_without_cancellation() {
    let (guard_tx, mut guard_rx) = mpsc::unbounded_channel();
    let (tree, _, _observed_rx) = build_cross_runtime(|target| {
        move || ReportedCrossInterval {
            target: target.clone(),
            guards: guard_tx.clone(),
            period: Duration::ZERO,
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");
    let guard = guard_rx.recv().await.expect("scheduler reports guard");

    assert!(guard.is_finished());
    assert!(
        !guard.is_cancelled(),
        "invalid interval period did not explicitly cancel the guard"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn dropped_interval_guard_cancels_delivery() {
    let (tree, _, mut observed_rx) = build_cross_runtime(|target| {
        move || DroppedCrossInterval {
            target: target.clone(),
        }
    });
    let running_tree = tree.spawn().expect("runtime builds");

    assert!(
        timeout(Duration::from_millis(50), observed_rx.recv())
            .await
            .is_err(),
        "a dropped interval guard delivered a tick"
    );

    running_tree.shutdown().await.expect("clean shutdown");
}
