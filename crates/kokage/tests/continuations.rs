use std::{sync::Arc, time::Duration};

use kokage::{Actor, Context, ExitResult, TimerKey, Tree};
use tokio::sync::{Notify, mpsc};

const CHAIN_STEPS: u16 = 1_000;
const TICK: TimerKey = TimerKey::new("tick");

enum Msg {
    Start,
    Continue(u8),
    External,
}

struct FairContinuation {
    observed: mpsc::UnboundedSender<&'static str>,
    first_continuation: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for FairContinuation {
    type Msg = Msg;

    async fn handle(&mut self, message: Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Msg::Start => ctx.continue_with(Msg::Continue(2)),
            Msg::Continue(remaining) => {
                self.observed
                    .send("continuation")
                    .expect("receiver remains live");
                if remaining == 2 {
                    self.first_continuation.notify_one();
                    self.release.notified().await;
                }
                if remaining > 0 {
                    ctx.continue_with(Msg::Continue(remaining - 1));
                }
            }
            Msg::External => {
                self.observed
                    .send("external")
                    .expect("receiver remains live");
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_continuation_chain_gives_ready_mailbox_input_a_turn() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let first_continuation = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut tree = Tree::new();
    let actor = tree.add_actor("worker", {
        let first_continuation = Arc::clone(&first_continuation);
        let release = Arc::clone(&release);
        move || FairContinuation {
            observed: observed_tx.clone(),
            first_continuation: Arc::clone(&first_continuation),
            release: Arc::clone(&release),
        }
    });
    let running_tree = tree.spawn().expect("tree builds");

    actor.send(Msg::Start).await.expect("chain starts");
    first_continuation.notified().await;
    actor
        .send(Msg::External)
        .await
        .expect("external input is queued");
    release.notify_one();

    assert_eq!(observed_rx.recv().await, Some("continuation"));
    assert_eq!(observed_rx.recv().await, Some("external"));
    assert_eq!(observed_rx.recv().await, Some("continuation"));

    running_tree.shutdown().await.expect("tree stops");
}

enum CooperateMsg {
    Start,
    Continue(u16),
    External,
}

struct CooperatingContinuation {
    observed: mpsc::UnboundedSender<&'static str>,
    started: Arc<Notify>,
}

impl Actor for CooperatingContinuation {
    type Msg = CooperateMsg;

    async fn handle(&mut self, message: CooperateMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CooperateMsg::Start => ctx.continue_with(CooperateMsg::Continue(CHAIN_STEPS)),
            CooperateMsg::Continue(remaining) => {
                if remaining == CHAIN_STEPS {
                    self.started.notify_one();
                }
                if remaining == 0 {
                    self.observed.send("done").expect("receiver remains live");
                } else {
                    ctx.continue_with(CooperateMsg::Continue(remaining - 1));
                }
            }
            CooperateMsg::External => {
                self.observed
                    .send("external")
                    .expect("receiver remains live");
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_immediate_continuation_chain_cooperates_with_senders() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let started = Arc::new(Notify::new());
    let mut tree = Tree::new();
    let actor = tree.add_actor("worker", {
        let started = Arc::clone(&started);
        move || CooperatingContinuation {
            observed: observed_tx.clone(),
            started: Arc::clone(&started),
        }
    });
    let running_tree = tree.spawn().expect("tree builds");

    let sender = {
        let actor = actor.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            started.notified().await;
            actor
                .send(CooperateMsg::External)
                .await
                .expect("external input is queued");
        })
    };
    actor.send(CooperateMsg::Start).await.expect("chain starts");

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed_rx.recv()).await,
        Ok(Some("external"))
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed_rx.recv()).await,
        Ok(Some("done"))
    );
    sender.await.expect("sender task completes");

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test(flavor = "current_thread")]
async fn an_immediate_continuation_chain_cooperates_with_shutdown() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let started = Arc::new(Notify::new());
    let mut tree = Tree::new();
    let actor = tree.add_actor("worker", {
        let started = Arc::clone(&started);
        move || CooperatingContinuation {
            observed: observed_tx.clone(),
            started: Arc::clone(&started),
        }
    });
    let running_tree = tree.spawn().expect("tree builds");

    let stopper = {
        let scope = running_tree.scope();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            started.notified().await;
            scope.request_shutdown();
        })
    };
    actor.send(CooperateMsg::Start).await.expect("chain starts");

    tokio::time::timeout(Duration::from_secs(1), running_tree.wait())
        .await
        .expect("tree stops without draining the continuation chain")
        .expect("tree stops cleanly");
    stopper.await.expect("stopper task completes");
    assert!(
        observed_rx.try_recv().is_err(),
        "shutdown must preempt the continuation chain before it completes"
    );
}

enum TimerMsg {
    Arm,
    Prefix,
    Continue(u16),
    External,
    Fired,
}

struct TimerContinuation {
    observed: mpsc::UnboundedSender<&'static str>,
    started: Arc<Notify>,
    armed: Arc<Notify>,
    release_arm: Arc<Notify>,
}

impl Actor for TimerContinuation {
    type Msg = TimerMsg;

    async fn handle(&mut self, message: TimerMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            TimerMsg::Arm => {
                ctx.set_timeout(TICK, TimerMsg::Fired, Duration::ZERO);
                self.armed.notify_one();
                self.release_arm.notified().await;
            }
            TimerMsg::Prefix => ctx.continue_with(TimerMsg::Continue(CHAIN_STEPS)),
            TimerMsg::Continue(remaining) => {
                if remaining == CHAIN_STEPS {
                    self.started.notify_one();
                }
                if remaining == 0 {
                    self.observed.send("done").expect("receiver remains live");
                } else {
                    ctx.continue_with(TimerMsg::Continue(remaining - 1));
                }
            }
            TimerMsg::External => {
                self.observed
                    .send("external")
                    .expect("receiver remains live");
            }
            TimerMsg::Fired => {
                self.observed.send("fired").expect("receiver remains live");
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_timer_prefix_continuation_chain_checks_new_deliveries() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let started = Arc::new(Notify::new());
    let armed = Arc::new(Notify::new());
    let release_arm = Arc::new(Notify::new());
    let mut tree = Tree::new();
    let actor = tree.add_actor("worker", {
        let started = Arc::clone(&started);
        let armed = Arc::clone(&armed);
        let release_arm = Arc::clone(&release_arm);
        move || TimerContinuation {
            observed: observed_tx.clone(),
            started: Arc::clone(&started),
            armed: Arc::clone(&armed),
            release_arm: Arc::clone(&release_arm),
        }
    });
    let running_tree = tree.spawn().expect("tree builds");

    let sender = {
        let actor = actor.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            started.notified().await;
            actor
                .send(TimerMsg::External)
                .await
                .expect("external input is queued");
        })
    };
    actor.send(TimerMsg::Arm).await.expect("timer is armed");
    armed.notified().await;
    actor
        .send(TimerMsg::Prefix)
        .await
        .expect("prefix input is queued before the timer fires");
    release_arm.notify_one();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed_rx.recv()).await,
        Ok(Some("external"))
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed_rx.recv()).await,
        Ok(Some("done"))
    );
    sender.await.expect("sender task completes");

    running_tree.shutdown().await.expect("tree stops");
}
