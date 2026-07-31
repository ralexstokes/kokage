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
    Actor, ActorRef, ActorSpec, BoxError, Context, ExitResult, OrderedTree, RestartPolicy,
};
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
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
    let worker_runs = Arc::new(AtomicUsize::new(0));
    let worker_spec = ActorSpec::new("worker", move || Worker {
        runs: worker_runs.clone(),
        run: 0,
        processed: processed_tx.clone(),
    })
    .restart_policy(RestartPolicy::on_failure().limit(2, Duration::from_secs(1)));
    let worker = worker_spec.actor_ref();
    let frontend_spec = ActorSpec::new("frontend", {
        let worker = worker.clone();
        move || Frontend {
            worker: worker.clone(),
        }
    });
    let frontend = frontend_spec.actor_ref();

    let mut tree = OrderedTree::new();
    tree.add_actor(frontend_spec);
    tree.add_actor(worker_spec);
    let runtime = tree.spawn()?;
    let handle = runtime.scope();
    let mut events = handle.watch_lifecycle();
    let mut snapshots = handle.subscribe_snapshots();

    let event_task = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            println!("event: {event:?}");
        }
    });

    frontend.send("hello".to_owned()).await?;
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;
    frontend.send("fail-worker".to_owned()).await?;
    snapshots
        .wait_for_child("worker", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .map_err(|_| io::Error::other("worker restart could not be observed"))?;
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
