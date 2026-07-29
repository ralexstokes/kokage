use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kokage::{Actor, ActorRef, ActorResult, ActorSpec, Context, OrderedTree, host::BoxError};
use kokage_supervisor::Restart;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ActorResult {
        let worker = self.worker.clone();
        worker.send(order).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<String>,
    run: usize,
}

impl Actor for Worker {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ActorResult {
        if self.run == 0 && order == "fail-worker" {
            return Err::<_, BoxError>(Box::new(io::Error::other("worker failed")));
        }
        self.observed.send(order).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let worker_runs = Arc::new(AtomicUsize::new(0));
    let worker_spec = ActorSpec::new("worker", move || Worker {
        runs: worker_runs.clone(),
        observed: observed_tx.clone(),
        run: 0,
    })
    .restart(Restart::on_failure().limit(5, std::time::Duration::from_secs(5)));
    let (worker_spec, worker) = worker_spec.actor_ref();
    let frontend_spec = ActorSpec::new("frontend", {
        let worker = worker.clone();
        move || Frontend {
            worker: worker.clone(),
        }
    });
    let (frontend_spec, frontend) = frontend_spec.actor_ref();

    let runtime = OrderedTree::new()
        .actor(frontend_spec)
        .actor(worker_spec)
        .spawn()?;
    let handle = runtime.handle();

    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is declared")
        .generation;
    let mut restarted = handle.subscribe_snapshots();
    frontend.send("fail-worker".to_owned()).await?;
    restarted
        .wait_for_child("worker", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .map_err(|_| io::Error::other("worker restart could not be observed"))?;
    frontend.send("after-restart".to_owned()).await?;
    println!("observed {}", observed_rx.recv().await.expect("message"));

    runtime.shutdown_and_wait().await?;
    Ok(())
}
