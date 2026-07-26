#![allow(deprecated)]

use std::{io, time::Duration};

use std::sync::Arc;

use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_otp::{
    Actor, ActorContext, ActorRef, ActorResult, DynamicActorOptions, GraphBuilder, LifecycleWatch,
    MailboxMode, RestartPolicy, Runtime, RuntimeHandle, prelude::Continue,
};

struct Sink {
    observed: mpsc::UnboundedSender<u64>,
}

impl Actor for Sink {
    type Msg = u64;

    async fn handle(&mut self, count: u64, _ctx: &mut ActorContext<u64>) -> ActorResult {
        if count == 0 {
            return Err(io::Error::other("sink crash requested").into());
        }
        self.observed.send(count).expect("observer alive");
        Ok(Continue)
    }
}

struct Crasher;

impl Actor for Crasher {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut ActorContext<()>) -> ActorResult {
        Err(io::Error::other("crash requested").into())
    }
}

async fn runtime_with_sink() -> (
    RuntimeHandle,
    ActorRef<u64>,
    ActorRef<()>,
    mpsc::UnboundedReceiver<u64>,
) {
    let runtime = Runtime::dynamic().build().expect("runtime builds");
    let handle = runtime.spawn();
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let sink = handle
        .add_actor(
            "sink",
            move || Sink {
                observed: observed_tx.clone(),
            },
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("sink added");
    let crasher = handle
        .add_actor(
            "crasher",
            || Crasher,
            DynamicActorOptions::new().restart(RestartPolicy::OnFailure),
        )
        .await
        .expect("crasher added");
    handle.wait_started().await.expect("actors started");

    (handle, sink, crasher, observed_rx)
}

async fn crash_once(handle: &RuntimeHandle, crasher: &ActorRef<()>) {
    let (lifecycle, baseline) = restart_observer(handle, "crasher");
    crasher.send(()).await.expect("crash request delivered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "crasher", baseline),
    )
    .await
    .expect("crasher restarted in time");
}

fn restart_observer(handle: &RuntimeHandle, id: &str) -> (LifecycleWatch, u64) {
    let lifecycle = handle.watch_lifecycle();
    let baseline = handle
        .snapshot()
        .child(id)
        .unwrap_or_else(|| panic!("{id} is a direct child"))
        .generation;
    (lifecycle, baseline)
}

async fn await_restart(mut lifecycle: LifecycleWatch, id: &str, baseline: u64) -> u64 {
    lifecycle
        .wait_started(id, baseline)
        .await
        .expect("runtime remains live")
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(1), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition became true");
}

enum BlockingSinkMsg {
    Block,
    RestartTotal(u64),
}

struct BlockingSink {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<u64>,
    crash_after_release: bool,
}

impl Actor for BlockingSink {
    type Msg = BlockingSinkMsg;

    async fn handle(
        &mut self,
        message: BlockingSinkMsg,
        _ctx: &mut ActorContext<BlockingSinkMsg>,
    ) -> ActorResult {
        match message {
            BlockingSinkMsg::Block => {
                self.entered.notify_one();
                self.release.notified().await;
                if self.crash_after_release {
                    return Err(io::Error::other("sink crash requested").into());
                }
            }
            BlockingSinkMsg::RestartTotal(total) => {
                self.observed.send(total).expect("observer alive");
            }
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn runtime_watch_restarts_to_forwards_counts() {
    let (handle, sink, crasher, mut observed) = runtime_with_sink().await;
    let watch = handle.watch_restarts_to(&sink, |count| count);

    crash_once(&handle, &crasher).await;
    assert_eq!(
        timeout(Duration::from_secs(1), observed.recv())
            .await
            .expect("restart count delivered"),
        Some(1)
    );

    assert!(!watch.is_cancelled());
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn dropping_restart_watch_stops_delivery() {
    let (handle, sink, crasher, mut observed) = runtime_with_sink().await;
    let watch = handle.watch_restarts_to(&sink, |count| count);
    drop(watch);

    crash_once(&handle, &crasher).await;
    assert!(
        timeout(Duration::from_millis(100), observed.recv())
            .await
            .is_err(),
        "dropped restart watch delivered a message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn cancelling_restart_watch_stops_delivery() {
    let (handle, sink, crasher, mut observed) = runtime_with_sink().await;
    let watch = handle.watch_restarts_to(&sink, |count| count);
    watch.cancel();
    assert!(watch.is_cancelled());

    crash_once(&handle, &crasher).await;
    assert!(
        timeout(Duration::from_millis(100), observed.recv())
            .await
            .is_err(),
        "cancelled restart watch delivered a message"
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restart_watch_follows_target_restarts() {
    let (handle, sink, _crasher, mut observed) = runtime_with_sink().await;
    let watch = handle.watch_restarts_to(&sink, |count| count);
    let (lifecycle, baseline) = restart_observer(&handle, "sink");

    sink.send(0).await.expect("sink crash request delivered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "sink", baseline),
    )
    .await
    .expect("sink restarted in time");
    assert_eq!(
        timeout(Duration::from_secs(1), observed.recv())
            .await
            .expect("restart count delivered to new incarnation"),
        Some(1)
    );
    assert!(!watch.is_cancelled());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restart_watch_stops_when_target_terminates() {
    let (handle, sink, _crasher, _observed) = runtime_with_sink().await;
    let watch = handle.watch_restarts_to(&sink, |count| count);

    handle.remove_child("sink").await.expect("sink removed");
    timeout(Duration::from_secs(1), async {
        while !watch.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watch stopped after target termination");

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn cumulative_totals_survive_conflating_target_mailbox() {
    let runtime = Runtime::dynamic().build().expect("runtime builds");
    let handle = runtime.spawn();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let sink = handle
        .add_actor(
            "sink",
            {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                    observed: observed_tx.clone(),
                    crash_after_release: false,
                }
            },
            DynamicActorOptions::default().mailbox(MailboxMode::Conflate),
        )
        .await
        .expect("sink added");
    let crasher = handle
        .add_actor("crasher", || Crasher, DynamicActorOptions::default())
        .await
        .expect("crasher added");
    handle.wait_started().await.expect("actors started");
    sink.send(BlockingSinkMsg::Block)
        .await
        .expect("block request delivered");
    entered.notified().await;

    let watch = handle.watch_restarts_to(&sink, BlockingSinkMsg::RestartTotal);
    crash_once(&handle, &crasher).await;
    wait_until(|| sink.stats().messages_accepted >= 2).await;
    crash_once(&handle, &crasher).await;
    wait_until(|| sink.stats().messages_conflated >= 1).await;

    release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(1), observed.recv())
            .await
            .expect("cumulative restart total delivered"),
        Some(2)
    );

    drop(watch);
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn latest_total_is_resent_after_target_restart() {
    let runtime = Runtime::dynamic().build().expect("runtime builds");
    let handle = runtime.spawn();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (observed_tx, mut observed) = mpsc::unbounded_channel();
    let sink = handle
        .add_actor(
            "sink",
            {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                    observed: observed_tx.clone(),
                    crash_after_release: true,
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("sink added");
    let crasher = handle
        .add_actor("crasher", || Crasher, DynamicActorOptions::default())
        .await
        .expect("crasher added");
    handle.wait_started().await.expect("actors started");
    sink.send(BlockingSinkMsg::Block)
        .await
        .expect("block request delivered");
    entered.notified().await;

    let watch = handle.watch_restarts_to(&sink, BlockingSinkMsg::RestartTotal);
    crash_once(&handle, &crasher).await;
    wait_until(|| sink.stats().messages_accepted >= 2).await;
    let (lifecycle, baseline) = restart_observer(&handle, "sink");
    release.notify_one();
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "sink", baseline),
    )
    .await
    .expect("sink restarted in time");

    let restored = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(total) = observed.recv().await
                && total >= 2
            {
                return total;
            }
        }
    })
    .await
    .expect("latest total restored to new incarnation");
    assert_eq!(restored, 2);

    drop(watch);
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn watched_supervisor_termination_cancels_backpressured_delivery() {
    let runtime = Runtime::dynamic().build().expect("runtime builds");
    let handle = runtime.spawn();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (observed_tx, _observed) = mpsc::unbounded_channel();
    let sink = handle
        .add_actor(
            "sink",
            {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || BlockingSink {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                    observed: observed_tx.clone(),
                    crash_after_release: false,
                }
            },
            DynamicActorOptions::default(),
        )
        .await
        .expect("sink added");

    let mut nested_graph = GraphBuilder::new();
    let nested_crasher = nested_graph.actor("crasher", || Crasher);
    let nested = Runtime::builder()
        .graph(nested_graph.build().expect("nested graph builds"))
        .restart(RestartPolicy::OnFailure);
    let watched = handle
        .add_subtree("watched", nested)
        .await
        .expect("nested runtime added");

    sink.send(BlockingSinkMsg::Block)
        .await
        .expect("block request delivered");
    entered.notified().await;
    let capacity = sink.stats().mailbox_capacity;
    for _ in 0..capacity {
        sink.send(BlockingSinkMsg::RestartTotal(0))
            .await
            .expect("mailbox filler accepted");
    }

    let watch = watched.watch_restarts_to(&sink, BlockingSinkMsg::RestartTotal);
    let (lifecycle, baseline) = restart_observer(&watched, "crasher");
    nested_crasher
        .send(())
        .await
        .expect("crash request delivered");
    timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "crasher", baseline),
    )
    .await
    .expect("nested child restarted in time");
    handle
        .remove_child("watched")
        .await
        .expect("watched supervisor removed");
    wait_until(|| watch.is_cancelled()).await;

    release.notify_one();
    handle.shutdown_and_wait().await.expect("clean shutdown");
}
