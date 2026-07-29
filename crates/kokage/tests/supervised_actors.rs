use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    ActorRef, ActorResult, ActorSpec, GraphBuilder, OrderedTree, Reply, SendError,
    host::{ActorContext, BoxError, RawActor},
};
use kokage_supervisor::{BackoffPolicy, RestartConfig, RestartPolicy, Strategy};
use tokio::{
    sync::{mpsc, oneshot},
    time::{advance, timeout},
};

fn oneshot_slot<T>(tx: oneshot::Sender<T>) -> Arc<Mutex<Option<oneshot::Sender<T>>>> {
    Arc::new(Mutex::new(Some(tx)))
}

fn send_once<T>(slot: &Arc<Mutex<Option<oneshot::Sender<T>>>>, value: T) {
    if let Some(tx) = slot.lock().expect("mutex not poisoned").take() {
        let _ = tx.send(value);
    }
}

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
    starts: Arc<AtomicUsize>,
}

impl RawActor for Frontend {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        self.starts.fetch_add(1, Ordering::SeqCst);
        while let Some(message) = ctx.recv().await {
            let worker = self.worker.clone();
            worker.send(message).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    observed: mpsc::UnboundedSender<String>,
    starts: Arc<AtomicUsize>,
    failed: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl RawActor for Worker {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        let run = self.starts.fetch_add(1, Ordering::SeqCst);
        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
            if run == 0 {
                send_once(&self.failed, ());
                return Err::<_, BoxError>(Box::new(io::Error::other("boom")));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn supervised_actors_restart_only_the_failed_actor() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let frontend_starts = Arc::new(AtomicUsize::new(0));
    let worker_starts = Arc::new(AtomicUsize::new(0));
    let (failed_tx, failed_rx) = oneshot::channel();

    let mut builder = GraphBuilder::new();
    let (worker_slot, worker_ref) = builder.slot::<String>("worker");
    let (frontend_ref_slot, frontend_ref) = builder.slot("frontend");
    builder.define(frontend_ref_slot, {
        let worker_ref = worker_ref.clone();
        let frontend_starts = frontend_starts.clone();
        move || Frontend {
            worker: worker_ref.clone(),
            starts: frontend_starts.clone(),
        }
    });
    let failed = oneshot_slot(failed_tx);
    builder.define(worker_slot, {
        let worker_starts = worker_starts.clone();
        move || Worker {
            observed: observed_tx.clone(),
            starts: worker_starts.clone(),
            failed: failed.clone(),
        }
    });
    let graph = builder.build().expect("valid graph");

    let handle = OrderedTree::graph(graph)
        .strategy(Strategy::OneForOne)
        .default_restart(RestartPolicy::OnFailure)
        .spawn()
        .expect("runtime builds");

    frontend_ref
        .send("first".to_owned())
        .await
        .expect("frontend accepts the first message");
    let first = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker saw the first message")
        .expect("worker forwarded the first message");
    assert_eq!(first, "first");

    timeout(Duration::from_secs(1), failed_rx)
        .await
        .expect("worker failed on the first run")
        .expect("worker failure signal received");

    frontend_ref
        .send("second".to_owned())
        .await
        .expect("frontend accepts the second message");
    let second = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("worker saw the second message after restart")
        .expect("worker forwarded the second message");
    assert_eq!(second, "second");

    assert_eq!(frontend_starts.load(Ordering::SeqCst), 1);
    assert!(worker_starts.load(Ordering::SeqCst) >= 2);

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[derive(Clone)]
struct CleanThenReceive {
    runs: Arc<AtomicUsize>,
    first_exited: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    observed: mpsc::UnboundedSender<String>,
}

impl RawActor for CleanThenReceive {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        let run = self.runs.fetch_add(1, Ordering::SeqCst);
        if run == 0 {
            send_once(&self.first_exited, ());
            return Ok(());
        }

        while let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn send_waits_during_permanent_restart_window() {
    let mut restart_config = RestartConfig::new(10, Duration::from_secs(1));
    restart_config.backoff = BackoffPolicy::Fixed(Duration::from_millis(100));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (first_exited_tx, first_exited_rx) = oneshot::channel();
    let runs = Arc::new(AtomicUsize::new(0));
    let first_exited = oneshot_slot(first_exited_tx);

    let mut builder = GraphBuilder::new();
    let (worker_ref_slot, worker_ref) = builder.slot("worker");
    builder.define(worker_ref_slot, move || CleanThenReceive {
        runs: runs.clone(),
        first_exited: first_exited.clone(),
        observed: observed_tx.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let handle = OrderedTree::new()
        .strategy(Strategy::OneForOne)
        .actor(
            ActorSpec::new(graph.actors()[0].clone())
                .restart(RestartPolicy::Always)
                .restart_config(restart_config),
        )
        .spawn()
        .expect("runtime builds");

    timeout(Duration::from_secs(1), first_exited_rx)
        .await
        .expect("first run exited")
        .expect("first run signal received");

    let send_task = tokio::spawn({
        let worker_ref = worker_ref.clone();
        async move { worker_ref.send("after-rebind".to_owned()).await }
    });
    tokio::task::yield_now().await;
    assert!(
        !send_task.is_finished(),
        "send should wait during the restart backoff"
    );
    advance(Duration::from_millis(100)).await;

    let observed = timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("message delivered after restart")
        .expect("message observed");
    assert_eq!(observed, "after-rebind");
    send_task
        .await
        .expect("send task joined")
        .expect("send completed");

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}

#[derive(Clone)]
struct NotifyCleanExit {
    exited: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl RawActor for NotifyCleanExit {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        send_once(&self.exited, ());
        Ok(())
    }
}

#[tokio::test]
async fn send_to_cleanly_exiting_transient_returns_actor_terminated_promptly() {
    let (exited_tx, exited_rx) = oneshot::channel();
    let exited = oneshot_slot(exited_tx);

    let mut builder = GraphBuilder::new();
    let (worker_ref_slot, worker_ref) = builder.slot("worker");
    builder.define(worker_ref_slot, move || NotifyCleanExit {
        exited: exited.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let handle = OrderedTree::graph(graph)
        .strategy(Strategy::OneForOne)
        .default_restart(RestartPolicy::OnFailure)
        .spawn()
        .expect("runtime builds");

    timeout(Duration::from_secs(1), exited_rx)
        .await
        .expect("actor exited")
        .expect("exit signal received");
    let result = timeout(Duration::from_millis(100), worker_ref.send(()))
        .await
        .expect("send returned promptly");
    assert!(matches!(
        result,
        Err(SendError { actor_id , .. }) if actor_id == "worker"
    ));

    let mut snapshots = handle.handle().subscribe_snapshots();
    let completed = timeout(
        Duration::from_secs(1),
        snapshots.wait_for(|snapshot| {
            snapshot
                .child("worker")
                .is_some_and(|child| child.state.is_terminal())
        }),
    )
    .await
    .expect("actor should complete")
    .expect("snapshot stream should remain open")
    .clone();
    assert!(matches!(
        completed
            .child("worker")
            .expect("worker remains visible")
            .state
            .last_exit(),
        Some(exit) if exit.is_completed()
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

enum RpcMsg {
    FailOnce,
    Get(Reply<String>),
}

#[derive(Clone)]
struct RestartingRpc {
    runs: Arc<AtomicUsize>,
    failed: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl RawActor for RestartingRpc {
    type Msg = RpcMsg;

    async fn run(&mut self, mut ctx: ActorContext<RpcMsg>) -> ActorResult {
        let run = self.runs.fetch_add(1, Ordering::SeqCst);
        while let Some(message) = ctx.recv().await {
            match message {
                RpcMsg::FailOnce if run == 0 => {
                    send_once(&self.failed, ());
                    return Err::<_, BoxError>(Box::new(io::Error::other("boom")));
                }
                RpcMsg::FailOnce => {}
                RpcMsg::Get(reply) => reply.send("ok".to_owned()),
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn call_succeeds_across_restart_window() {
    let mut restart_config = RestartConfig::new(10, Duration::from_secs(1));
    restart_config.backoff = BackoffPolicy::Fixed(Duration::from_millis(100));
    let (failed_tx, failed_rx) = oneshot::channel();
    let runs = Arc::new(AtomicUsize::new(0));
    let failed = oneshot_slot(failed_tx);

    let mut builder = GraphBuilder::new();
    let (rpc_ref_slot, rpc_ref) = builder.slot("rpc");
    builder.define(rpc_ref_slot, move || RestartingRpc {
        runs: runs.clone(),
        failed: failed.clone(),
    });
    let graph = builder.build().expect("valid graph");

    let handle = OrderedTree::new()
        .strategy(Strategy::OneForOne)
        .actor(
            ActorSpec::new(graph.actors()[0].clone())
                .restart(RestartPolicy::OnFailure)
                .restart_config(restart_config),
        )
        .spawn()
        .expect("runtime builds");

    rpc_ref
        .send(RpcMsg::FailOnce)
        .await
        .expect("first request delivered");
    timeout(Duration::from_secs(1), failed_rx)
        .await
        .expect("actor failed")
        .expect("failure signal received");

    let call_task = tokio::spawn({
        let rpc_ref = rpc_ref.clone();
        async move { rpc_ref.call(Duration::from_secs(1), RpcMsg::Get).await }
    });
    tokio::task::yield_now().await;
    assert!(
        !call_task.is_finished(),
        "call should wait during the restart backoff"
    );
    advance(Duration::from_millis(100)).await;

    assert_eq!(
        call_task
            .await
            .expect("call task joined")
            .expect("call completed after restart"),
        "ok"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("supervisor shut down cleanly");
}
