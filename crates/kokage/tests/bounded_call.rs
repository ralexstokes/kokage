use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    ActorResult, ActorSpec, CallError, Reply, Restart,
    host::{ActorContext, DEFAULT_SHUTDOWN_BOUND, RawActor, RunnableActor},
};
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const CALL_TIMEOUT: Duration = Duration::from_millis(50);

fn start(actor: RunnableActor) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let task = tokio::spawn({
        let stop = stop.clone();
        async move {
            actor
                .run_until(stop.cancelled(), Restart::never(), DEFAULT_SHUTDOWN_BOUND)
                .await
                .expect("actor run succeeds");
        }
    });
    (stop, task)
}

async fn stop(stop: CancellationToken, task: tokio::task::JoinHandle<()>) {
    stop.cancel();
    timeout(Duration::from_secs(1), task)
        .await
        .expect("actor stops in time")
        .expect("actor task joins");
}

enum Request {
    Get(Reply<&'static str>),
}

#[derive(Clone)]
struct ReplyImmediately;

impl RawActor for ReplyImmediately {
    type Msg = Request;

    async fn run(&mut self, mut ctx: ActorContext<Request>) -> ActorResult {
        while let Some(Request::Get(reply)) = ctx.recv().await {
            reply.send("ok");
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_before_mailbox_binding_drops_the_request() {
    let spec = ActorSpec::new("rpc", || ReplyImmediately);
    let (spec, rpc) = spec.actor_ref();
    let actor = spec.into_runnable();

    assert!(matches!(
        rpc.call(CALL_TIMEOUT, Request::Get).await,
        Err(CallError::Timeout { actor_id, .. }) if actor_id == "rpc"
    ));
    assert_eq!(rpc.stats().messages_accepted, 0);

    let (stop_token, task) = start(actor);
    assert_eq!(
        rpc.call(CALL_TIMEOUT, Request::Get)
            .await
            .expect("later call succeeds"),
        "ok"
    );
    assert_eq!(rpc.stats().messages_accepted, 1);
    stop(stop_token, task).await;
}

enum BackpressuredRequest {
    Occupy,
    Get(Reply<&'static str>),
}

#[derive(Clone)]
struct GatedMailbox {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl RawActor for GatedMailbox {
    type Msg = BackpressuredRequest;

    async fn run(&mut self, mut ctx: ActorContext<BackpressuredRequest>) -> ActorResult {
        self.started.send(()).expect("test receiver alive");
        self.release.notified().await;
        while let Some(message) = ctx.recv().await {
            match message {
                BackpressuredRequest::Occupy => {
                    self.observed.send("occupy").expect("test receiver alive");
                }
                BackpressuredRequest::Get(reply) => {
                    self.observed.send("get").expect("test receiver alive");
                    reply.send("ok");
                }
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_under_fifo_backpressure_drops_the_unaccepted_request() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let spec = ActorSpec::new("rpc", {
        let release = release.clone();
        move || GatedMailbox {
            started: started_tx.clone(),
            release: release.clone(),
            observed: observed_tx.clone(),
        }
    })
    .mailbox_capacity(1);
    let (spec, rpc) = spec.actor_ref();
    let (stop_token, task) = start(spec.into_runnable());
    started_rx.recv().await.expect("actor started");

    rpc.send(BackpressuredRequest::Occupy)
        .await
        .expect("first message fills the mailbox");
    assert!(matches!(
        rpc.call(CALL_TIMEOUT, BackpressuredRequest::Get).await,
        Err(CallError::Timeout { actor_id, .. }) if actor_id == "rpc"
    ));
    assert_eq!(rpc.stats().messages_accepted, 1);

    release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("occupied message is processed"),
        Some("occupy")
    );
    assert!(
        timeout(CALL_TIMEOUT, observed_rx.recv()).await.is_err(),
        "the timed-out request must not appear later"
    );
    stop(stop_token, task).await;
}

#[derive(Clone)]
struct DelayedReply {
    accepted: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    effects: Arc<AtomicUsize>,
    replied: mpsc::UnboundedSender<()>,
}

impl RawActor for DelayedReply {
    type Msg = Request;

    async fn run(&mut self, mut ctx: ActorContext<Request>) -> ActorResult {
        while let Some(Request::Get(reply)) = ctx.recv().await {
            self.accepted.send(()).expect("test receiver alive");
            self.release.notified().await;
            self.effects.fetch_add(1, Ordering::SeqCst);
            reply.send("late");
            self.replied.send(()).expect("test receiver alive");
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_after_acceptance_does_not_cancel_actor_work_or_late_reply() {
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    let (replied_tx, mut replied_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let spec = ActorSpec::new("rpc", {
        let release = release.clone();
        let effects = effects.clone();
        move || DelayedReply {
            accepted: accepted_tx.clone(),
            release: release.clone(),
            effects: effects.clone(),
            replied: replied_tx.clone(),
        }
    });
    let (spec, rpc) = spec.actor_ref();
    let (stop_token, task) = start(spec.into_runnable());

    let call = tokio::spawn({
        let rpc = rpc.clone();
        async move { rpc.call(CALL_TIMEOUT, Request::Get).await }
    });
    accepted_rx
        .recv()
        .await
        .expect("actor accepted the request");
    assert!(matches!(
        call.await.expect("call task joins"),
        Err(CallError::Timeout { actor_id, .. }) if actor_id == "rpc"
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 0);

    release.notify_one();
    timeout(Duration::from_secs(1), replied_rx.recv())
        .await
        .expect("actor attempts its late reply")
        .expect("reply attempt observed");
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    stop(stop_token, task).await;
}

#[derive(Clone)]
struct ExitWithoutReceiving {
    started: mpsc::UnboundedSender<()>,
    exit: Arc<Notify>,
}

impl RawActor for ExitWithoutReceiving {
    type Msg = Request;

    async fn run(&mut self, _ctx: ActorContext<Request>) -> ActorResult {
        self.started.send(()).expect("test receiver alive");
        self.exit.notified().await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn accepted_unread_request_lost_with_incarnation_reports_reply_dropped() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let exit = Arc::new(Notify::new());
    let spec = ActorSpec::new("rpc", {
        let exit = exit.clone();
        move || ExitWithoutReceiving {
            started: started_tx.clone(),
            exit: exit.clone(),
        }
    });
    let (spec, rpc) = spec.actor_ref();
    let (_stop_token, task) = start(spec.into_runnable());
    started_rx.recv().await.expect("actor started");

    let call = tokio::spawn({
        let rpc = rpc.clone();
        async move { rpc.call(CALL_TIMEOUT, Request::Get).await }
    });
    while rpc.stats().messages_accepted == 0 {
        tokio::task::yield_now().await;
    }

    exit.notify_one();
    timeout(Duration::from_secs(1), task)
        .await
        .expect("incarnation exits in time")
        .expect("actor task joins");
    assert!(matches!(
        call.await.expect("call task joins"),
        Err(CallError::ReplyDropped { actor_id, .. }) if actor_id == "rpc"
    ));
}
