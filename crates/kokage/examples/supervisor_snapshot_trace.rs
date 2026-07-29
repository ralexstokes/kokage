use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorResult, ActorSpec, GraphBuilder, MessageContext, OrderedTree,
    StartContext, host::BoxError,
};
use kokage_supervisor::RestartConfig;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(
        &mut self,
        message: String,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let worker = self.worker.clone();
        worker.send(message).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    runs: Arc<AtomicUsize>,
    run: usize,
    processed: mpsc::UnboundedSender<String>,
}

impl Actor for Worker {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn handle(
        &mut self,
        message: String,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        println!("worker generation {} received `{message}`", self.run);
        if message == "fail-worker" {
            return Err::<_, BoxError>(Box::new(io::Error::other("simulated failure")));
        }
        let _ = self.processed.send(message);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let worker_runs = Arc::new(AtomicUsize::new(0));
    let worker = builder.actor("worker", move || Worker {
        runs: worker_runs.clone(),
        run: 0,
        processed: processed_tx.clone(),
    });
    let frontend = builder.actor("frontend", {
        let worker = worker.clone();
        move || Frontend {
            worker: worker.clone(),
        }
    });
    let graph = builder.build()?;
    let frontend_actor = graph.actor_for(&frontend)?;
    let worker_actor = graph.actor_for(&worker)?;

    let runtime_owner = OrderedTree::new()
        .actor(frontend_actor)
        .actor(
            ActorSpec::new(worker_actor)
                .restart_config(RestartConfig::new(2, Duration::from_secs(1))),
        )
        .spawn()?;
    let runtime = runtime_owner.handle();
    let mut events = runtime.watch_lifecycle_recursive();
    let mut snapshots = runtime.subscribe_snapshots();

    let event_task = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            println!("event: {event:?}");
        }
    });

    frontend.send("hello".to_owned()).await?;
    let mut lifecycle = runtime.watch_lifecycle();
    let baseline = runtime
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;
    frontend.send("fail-worker".to_owned()).await?;
    lifecycle
        .started_after("worker", baseline)
        .await
        .ok_or_else(|| io::Error::other("worker restart could not be observed"))?;
    frontend.send("after-restart".to_owned()).await?;

    // Wait for the worker to finish the last order before shutting down.
    // Shutdown cancels every child at once; a frontend caught mid-forward
    // would fail its handoff when the worker's binding terminates.
    while let Some(message) = processed_rx.recv().await {
        if message == "after-restart" {
            break;
        }
    }

    let snapshot = snapshots.changed().await?;
    println!("snapshot: {:?}", snapshot.state);

    runtime.shutdown_and_wait().await?;
    event_task.abort();
    Ok(())
}
