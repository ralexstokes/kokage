use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_otp::{
    Actor, ActorContext, ActorRef, ActorResult, BoxError, GraphBuilder, Runtime, prelude::Continue,
};
use tokio_supervisor::{RestartIntensity, Strategy};

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &ActorContext<String>) -> ActorResult {
        let worker = self.worker.clone();
        worker.send(message).await?;
        Ok(Continue)
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

    async fn on_start(&mut self, _ctx: &ActorContext<String>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Continue)
    }

    async fn handle(&mut self, message: String, _ctx: &ActorContext<String>) -> ActorResult {
        println!("worker generation {} received `{message}`", self.run);
        if message == "fail-worker" {
            return Err::<_, BoxError>(Box::new(io::Error::other("simulated failure")));
        }
        let _ = self.processed.send(message);
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let (worker_slot, worker_ref) = builder.slot::<String>("worker");
    let frontend = builder.actor("frontend", {
        let worker_ref = worker_ref.clone();
        move || Frontend {
            worker: worker_ref.clone(),
        }
    });
    let worker_runs = Arc::new(AtomicUsize::new(0));
    builder.define(worker_slot, move || Worker {
        runs: worker_runs.clone(),
        run: 0,
        processed: processed_tx.clone(),
    });
    let graph = builder.build()?;

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .actor_restart_intensity(
            &worker_ref,
            RestartIntensity::new(2, Duration::from_secs(1)),
        )
        .build()?;
    let handle = runtime.spawn();
    let mut events = handle.subscribe();
    let mut snapshots = handle.subscribe_snapshots();

    let event_task = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            println!("event: {event:?}");
        }
    });

    frontend.send("hello".to_owned()).await?;
    let restart = handle.monitor_restart("worker")?;
    frontend.send("fail-worker".to_owned()).await?;
    restart.await?;
    frontend.send("after-restart".to_owned()).await?;

    // Wait for the worker to finish the last order before shutting down.
    // Shutdown cancels every child at once; a frontend caught mid-forward
    // would fail its handoff when the worker's binding terminates.
    while let Some(message) = processed_rx.recv().await {
        if message == "after-restart" {
            break;
        }
    }

    snapshots.changed().await?;
    println!("snapshot: {:?}", snapshots.borrow().state);

    handle.shutdown_and_wait().await?;
    event_task.abort();
    Ok(())
}
