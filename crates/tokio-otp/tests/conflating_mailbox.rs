use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Notify, mpsc};
use tokio_otp::{
    Actor, ActorContext, ActorOptions, ActorResult, CallError, DrainPolicy, GraphBuilder,
    MailboxMode, MessageSize, RawActor, RebindPolicy, Reply, prelude::Continue,
};
use tokio_util::sync::CancellationToken;

struct GatedCollector<M> {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    received: mpsc::UnboundedSender<M>,
}

impl<M> Clone for GatedCollector<M> {
    fn clone(&self) -> Self {
        Self {
            started: self.started.clone(),
            release: Arc::clone(&self.release),
            received: self.received.clone(),
        }
    }
}

impl<M: Send + 'static> RawActor for GatedCollector<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: ActorContext<M>) -> ActorResult {
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;
        while let Some(message) = ctx.recv().await {
            self.received
                .send(message)
                .expect("test receives actor messages");
        }
        Ok(Continue)
    }
}

#[tokio::test]
async fn conflate_keeps_only_the_newest_unread_message() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor_with_options(
        "ticks",
        {
            let release = release.clone();
            move || GatedCollector {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        },
        ActorOptions::new().mailbox(MailboxMode::Conflate),
    );
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    for tick in 0..100 {
        if tick % 2 == 0 {
            actor_ref
                .send(tick)
                .await
                .expect("conflating send succeeds");
        } else {
            actor_ref
                .try_send(tick)
                .expect("conflating try_send succeeds");
        }
    }

    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 100);
    assert_eq!(stats.messages_conflated, 99);
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.mailbox_capacity, 1);

    release.notify_one();
    assert_eq!(received_rx.recv().await, Some(99));
    assert!(received_rx.try_recv().is_err());

    stop.cancel();
    task.await.expect("actor task joins").expect("actor stops");
}

struct SizedSnapshot(Vec<u8>);

impl MessageSize for SizedSnapshot {
    fn size_hint(&self) -> usize {
        self.0.len()
    }
}

#[tokio::test]
async fn actor_options_combine_conflation_and_message_size_observation() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor_with_options(
        "snapshots",
        {
            let release = release.clone();
            move || GatedCollector {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        },
        ActorOptions::new()
            .mailbox(MailboxMode::Conflate)
            .message_size(),
    );
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    for size in 1..=3 {
        actor_ref
            .send(SizedSnapshot(vec![0; size]))
            .await
            .expect("snapshot accepted");
    }

    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 3);
    assert_eq!(stats.messages_conflated, 2);
    assert_eq!(stats.message_bytes_accepted, Some(6));

    release.notify_one();
    assert_eq!(
        received_rx.recv().await.map(|message| message.0.len()),
        Some(3)
    );
    stop.cancel();
    task.await.expect("actor task joins").expect("actor stops");
}

#[derive(Debug, Eq, PartialEq)]
struct Tick {
    symbol: &'static str,
    price: u64,
}

#[tokio::test]
async fn conflate_by_key_replaces_values_and_evicts_the_oldest_key_at_capacity() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    builder.mailbox_capacity(2);
    let (slot, actor_ref) = builder.slot_with_options(
        "market-data",
        ActorOptions::new().mailbox(MailboxMode::conflate_by_key(|tick: &Tick| tick.symbol)),
    );
    builder.define(slot, {
        let release = release.clone();
        move || GatedCollector {
            started: started_tx.clone(),
            release: release.clone(),
            received: received_tx.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    for tick in [
        Tick {
            symbol: "ETH",
            price: 10,
        },
        Tick {
            symbol: "BTC",
            price: 20,
        },
        Tick {
            symbol: "ETH",
            price: 11,
        },
        Tick {
            symbol: "BTC",
            price: 21,
        },
        Tick {
            symbol: "SOL",
            price: 30,
        },
    ] {
        actor_ref.send(tick).await.expect("keyed send succeeds");
    }

    let stats = actor_ref.stats();
    assert_eq!(stats.messages_accepted, 5);
    assert_eq!(stats.messages_conflated, 3);
    assert_eq!(stats.mailbox_depth, 2);
    assert_eq!(stats.mailbox_capacity, 2);

    release.notify_one();
    assert_eq!(
        received_rx.recv().await,
        Some(Tick {
            symbol: "BTC",
            price: 21,
        })
    );
    assert_eq!(
        received_rx.recv().await,
        Some(Tick {
            symbol: "SOL",
            price: 30,
        })
    );
    assert!(received_rx.try_recv().is_err());

    stop.cancel();
    task.await.expect("actor task joins").expect("actor stops");
}

enum RequestMsg {
    Get(Reply<()>),
    Snapshot(u64),
}

#[tokio::test]
async fn replaced_call_reports_reply_dropped() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor_with_options(
        "requests",
        {
            let release = release.clone();
            move || GatedCollector {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        },
        ActorOptions::new().mailbox(MailboxMode::Conflate),
    );
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    let call = tokio::spawn({
        let actor_ref = actor_ref.clone();
        async move {
            actor_ref
                .call(Duration::from_secs(1), RequestMsg::Get)
                .await
        }
    });
    while actor_ref.stats().messages_accepted == 0 {
        tokio::task::yield_now().await;
    }
    actor_ref
        .send(RequestMsg::Snapshot(42))
        .await
        .expect("new snapshot replaces request");

    assert!(matches!(
        call.await.expect("call task joins"),
        Err(CallError::ReplyDropped { .. })
    ));

    release.notify_one();
    match received_rx.recv().await {
        Some(RequestMsg::Snapshot(value)) => assert_eq!(value, 42),
        Some(RequestMsg::Get(reply)) => {
            drop(reply);
            panic!("replaced request reached actor");
        }
        None => panic!("actor stopped before receiving snapshot"),
    }

    stop.cancel();
    task.await.expect("actor task joins").expect("actor stops");
}

#[derive(Clone)]
struct GatedDrainActor {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    received: mpsc::UnboundedSender<u64>,
}

impl Actor for GatedDrainActor {
    type Msg = u64;

    async fn handle(&mut self, message: u64, _ctx: &mut ActorContext<u64>) -> ActorResult {
        self.received
            .send(message)
            .expect("test receives drained message");
        Ok(Continue)
    }

    async fn on_start(&mut self, _ctx: &mut ActorContext<u64>) -> ActorResult {
        self.started.send(()).expect("test receives start signal");
        self.release.notified().await;
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_policy_handles_latest_message_after_shutdown() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor_with_options(
        "drain",
        {
            let release = release.clone();
            move || GatedDrainActor {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        },
        ActorOptions::new().mailbox(MailboxMode::Conflate),
    );
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    actor_ref.send(1).await.expect("first send succeeds");
    actor_ref.send(2).await.expect("replacement succeeds");
    actor_ref.send(3).await.expect("replacement succeeds");
    stop.cancel();
    release.notify_one();

    task.await.expect("actor task joins").expect("actor drains");
    assert_eq!(received_rx.recv().await, Some(3));
    assert!(received_rx.try_recv().is_err());
}

#[tokio::test]
async fn poisoned_key_match_lock_recovers_without_panicking_in_drop() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let panic_once = Arc::new(AtomicBool::new(true));
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor_with_options(
        "poison-recovery",
        {
            let release = release.clone();
            move || GatedCollector {
                started: started_tx.clone(),
                release: release.clone(),
                received: received_tx.clone(),
            }
        },
        ActorOptions::new().mailbox(MailboxMode::conflate_by_key({
            let panic_once = Arc::clone(&panic_once);
            move |value: &u64| {
                assert!(
                    !panic_once.swap(false, Ordering::SeqCst),
                    "key extraction panic"
                );
                value % 2
            }
        })),
    );
    let graph = builder.build().expect("valid graph");
    let actor = graph.actors()[0].clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move { actor.run_until(stop.cancelled(), RebindPolicy::Never).await }
    });
    started_rx.recv().await.expect("actor started");

    actor_ref.try_send(1).expect("first key is inserted");
    let panic = catch_unwind(AssertUnwindSafe(|| actor_ref.try_send(2)));
    assert!(panic.is_err());
    actor_ref
        .try_send(3)
        .expect("send recovers the poisoned lock");

    release.notify_one();
    assert_eq!(received_rx.recv().await, Some(3));
    stop.cancel();
    task.await.expect("actor task joins").expect("actor stops");
}
