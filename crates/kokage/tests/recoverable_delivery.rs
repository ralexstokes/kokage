use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use kokage::{
    ActorSpec, Backoff, CallError, ExitResult, Mailbox, Reply, RestartPolicy, SendError,
    SendErrorKind, Tree,
    raw::{RawActor, RawContext},
};
use tokio::sync::{Notify, mpsc};

#[derive(Clone)]
struct ParkBeforeReceive {
    started: mpsc::UnboundedSender<()>,
    release: std::sync::Arc<Notify>,
}

impl RawActor for ParkBeforeReceive {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        self.started.send(()).expect("start receiver remains open");
        self.release.notified().await;
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[derive(Clone)]
struct Drain;

impl RawActor for Drain {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

#[derive(Clone)]
struct FailWithoutReceiving {
    started: mpsc::UnboundedSender<()>,
    fail: Arc<Notify>,
}

impl RawActor for FailWithoutReceiving {
    type Msg = String;

    async fn run(&mut self, ctx: RawContext<Self::Msg>) -> ExitResult {
        self.started.send(()).expect("start receiver remains open");
        tokio::select! {
            () = self.fail.notified() => Err(io::Error::other("test failure").into()),
            () = ctx.shutdown_token().cancelled() => Ok(()),
        }
    }
}

async fn close_full_mailbox_during_bounded_send(
    restart: RestartPolicy,
    bound: Duration,
    message: &str,
) -> Result<(), SendError<String>> {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let fail = Arc::new(Notify::new());
    let spec = ActorSpec::new("worker", {
        let fail = Arc::clone(&fail);
        move || FailWithoutReceiving {
            started: started_tx.clone(),
            fail: Arc::clone(&fail),
        }
    })
    .mailbox(Mailbox::queue(1))
    .restart(restart);
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    started_rx.recv().await.expect("first incarnation starts");
    actor
        .send("fills first mailbox".to_owned())
        .await
        .expect("first mailbox fills");

    let bounded_actor = actor.clone();
    let message = message.to_owned();
    let bounded = tokio::spawn(async move { bounded_actor.send_timeout(message, bound).await });
    tokio::task::yield_now().await;
    assert!(!bounded.is_finished(), "bounded send waits for capacity");
    fail.notify_one();

    let result = bounded.await.expect("bounded send task joins");
    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");
    result
}

#[derive(Clone)]
struct CollectAfterRelease {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    received: mpsc::UnboundedSender<String>,
}

impl RawActor for CollectAfterRelease {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        self.started.send(()).expect("start receiver remains open");
        self.release.notified().await;
        while let Some(message) = ctx.recv().await {
            self.received
                .send(message)
                .expect("received-message observer remains open");
        }
        Ok(())
    }
}

#[tokio::test]
async fn unbound_try_send_and_send_timeout_return_the_message() {
    let spec = ActorSpec::new("worker", || Drain);
    let actor = spec.actor_ref();

    let try_error = actor
        .try_send("try".to_owned())
        .expect_err("an unbound actor rejects a fail-fast send");
    assert!(matches!(
        &try_error,
        SendError {
            actor_id,
            kind: SendErrorKind::NotRunning,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(try_error.into_message(), "try");

    let timeout_error = actor
        .send_timeout("bounded".to_owned(), Duration::from_millis(10))
        .await
        .expect_err("an actor that never binds reaches the send bound");
    assert!(matches!(
        &timeout_error,
        SendError {
            actor_id,
            kind: SendErrorKind::TimedOut,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(timeout_error.into_message(), "bounded");

    let waiting_actor = actor.clone();
    let waiting = tokio::spawn(async move {
        waiting_actor
            .send_timeout("after bind".to_owned(), Duration::from_secs(1))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "bounded send waits for a binding");
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    waiting
        .await
        .expect("bounded send task joins")
        .expect("bounded send reaches the new mailbox");

    let zero_bound = actor
        .send_timeout("zero bound".to_owned(), Duration::ZERO)
        .await
        .expect_err("the deadline is checked before the first acceptance attempt");
    assert!(matches!(
        &zero_bound,
        SendError {
            actor_id,
            kind: SendErrorKind::TimedOut,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(zero_bound.into_message(), "zero bound");
    actor
        .try_send("one immediate attempt".to_owned())
        .expect("try_send is the immediate-attempt API");
    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");
}

#[tokio::test]
async fn full_mailbox_rejections_return_the_message() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = std::sync::Arc::new(Notify::new());
    let spec = ActorSpec::new("worker", {
        let release = release.clone();
        move || ParkBeforeReceive {
            started: started_tx.clone(),
            release: release.clone(),
        }
    })
    .mailbox(Mailbox::queue(1));
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    started_rx.recv().await.expect("actor starts");

    actor
        .send("occupies capacity".to_owned())
        .await
        .expect("first message is accepted");

    let try_error = actor
        .try_send("try again".to_owned())
        .expect_err("full mailbox rejects try_send");
    assert!(matches!(
        &try_error,
        SendError {
            actor_id,
            kind: SendErrorKind::Full,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(try_error.into_message(), "try again");

    let timeout_error = actor
        .send_timeout("retry later".to_owned(), Duration::from_millis(10))
        .await
        .expect_err("full mailbox remains full through the bound");
    assert!(matches!(
        &timeout_error,
        SendError {
            actor_id,
            kind: SendErrorKind::TimedOut,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(timeout_error.into_message(), "retry later");

    let waiting_actor = actor.clone();
    let waiting = tokio::spawn(async move {
        waiting_actor
            .send_timeout("after capacity".to_owned(), Duration::from_secs(1))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "bounded send waits for capacity");
    release.notify_one();
    waiting
        .await
        .expect("bounded send task joins")
        .expect("bounded send uses released capacity");

    let stats = actor.stats();
    assert_eq!(stats.messages_accepted, 2);
    assert_eq!(stats.sends_rejected, 2);

    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");
}

#[tokio::test]
async fn bounded_send_accepts_immediately_into_a_conflating_mailbox() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = std::sync::Arc::new(Notify::new());
    let spec = ActorSpec::new("worker", {
        let release = release.clone();
        move || ParkBeforeReceive {
            started: started_tx.clone(),
            release: release.clone(),
        }
    })
    .mailbox(Mailbox::latest());
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    started_rx.recv().await.expect("actor starts");

    actor
        .send("stale".to_owned())
        .await
        .expect("first state is accepted");
    actor
        .send_timeout("fresh".to_owned(), Duration::from_secs(1))
        .await
        .expect("bounded send replaces stale unread state");
    let stats = actor.stats();
    assert_eq!(stats.messages_accepted, 2);
    assert_eq!(stats.messages_conflated, 1);

    release.notify_one();
    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");
}

#[tokio::test]
async fn bounded_keyed_conflation_rechecks_deadline_before_queue_mutation() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let key_extractions = Arc::new(AtomicUsize::new(0));
    let spec = ActorSpec::new("worker", {
        let release = Arc::clone(&release);
        move || CollectAfterRelease {
            started: started_tx.clone(),
            release: Arc::clone(&release),
            received: received_tx.clone(),
        }
    })
    .mailbox(Mailbox::latest_by_key(1, {
        let key_extractions = Arc::clone(&key_extractions);
        move |message: &String| {
            key_extractions.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(30));
            message.len()
        }
    }));
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    started_rx.recv().await.expect("actor starts");

    actor
        .send("stale".to_owned())
        .await
        .expect("first keyed state is accepted");
    let timeout_error = actor
        .send_timeout("fresh".to_owned(), Duration::from_millis(10))
        .await
        .expect_err("slow key matching crosses the acceptance deadline");
    assert!(matches!(
        &timeout_error,
        SendError {
            actor_id,
            kind: SendErrorKind::TimedOut,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(timeout_error.into_message(), "fresh");
    assert_eq!(
        key_extractions.load(Ordering::SeqCst),
        2,
        "the deadline expires while comparing the queued and incoming keys"
    );

    let stats = actor.stats();
    assert_eq!(stats.messages_accepted, 1);
    assert_eq!(stats.messages_conflated, 0);
    assert_eq!(stats.sends_rejected, 1);
    assert_eq!(stats.mailbox_depth, 1);

    release.notify_one();
    assert_eq!(received_rx.recv().await.as_deref(), Some("stale"));
    assert!(
        received_rx.try_recv().is_err(),
        "the timed-out replacement was not accepted"
    );
    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");
}

#[tokio::test]
async fn bounded_send_rides_a_closed_mailbox_into_the_next_incarnation() {
    close_full_mailbox_during_bounded_send(
        RestartPolicy::on_failure(),
        Duration::from_secs(1),
        "after restart",
    )
    .await
    .expect("bounded send reaches the replacement mailbox");
}

#[tokio::test]
async fn bounded_send_returns_termination_after_its_mailbox_closes() {
    let error = close_full_mailbox_during_bounded_send(
        RestartPolicy::never(),
        Duration::from_secs(1),
        "not accepted",
    )
    .await
    .expect_err("terminated membership rejects the message");
    assert!(matches!(
        &error,
        SendError {
            actor_id,
            kind: SendErrorKind::Terminated,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(error.into_message(), "not accepted");
}

#[tokio::test]
async fn bounded_send_times_out_waiting_for_rebind_after_its_mailbox_closes() {
    let error = close_full_mailbox_during_bounded_send(
        RestartPolicy::on_failure().backoff(Backoff::fixed(Duration::from_secs(1))),
        Duration::from_millis(20),
        "deadline wins",
    )
    .await
    .expect_err("restart backoff exceeds the delivery bound");
    assert!(matches!(
        &error,
        SendError {
            actor_id,
            kind: SendErrorKind::TimedOut,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(error.into_message(), "deadline wins");
}

#[tokio::test]
async fn terminated_delivery_errors_return_the_message_and_call_stays_non_generic() {
    let spec = ActorSpec::new("worker", || Drain);
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let running_tree = tree.spawn().expect("tree builds");
    running_tree
        .shutdown()
        .await
        .expect("runtime shuts down cleanly");

    let send_error = actor
        .send("awaited".to_owned())
        .await
        .expect_err("terminated actor rejects send");
    assert_eq!(send_error.actor_id, "worker");
    assert_eq!(send_error.kind, SendErrorKind::Terminated);
    assert_eq!(send_error.into_message(), "awaited");

    let try_error = actor
        .try_send("immediate".to_owned())
        .expect_err("terminated actor rejects try_send");
    assert!(matches!(
        &try_error,
        SendError {
            actor_id,
            kind: SendErrorKind::Terminated,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(try_error.into_message(), "immediate");

    let timeout_error = actor
        .send_timeout("bounded".to_owned(), Duration::MAX)
        .await
        .expect_err("terminated actor rejects send_timeout");
    assert!(matches!(
        &timeout_error,
        SendError {
            actor_id,
            kind: SendErrorKind::Terminated,
            ..
        } if actor_id == "worker"
    ));
    assert_eq!(timeout_error.into_message(), "bounded");

    let call_error: CallError = actor
        .call(
            |reply: Reply<()>| {
                drop(reply);
                "request".to_owned()
            },
            Duration::from_secs(1),
        )
        .await
        .expect_err("terminated actor rejects call delivery");
    assert!(matches!(
        call_error,
        CallError::Terminated { actor_id, .. } if actor_id == "worker"
    ));
}
