use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kokage::{ActorSpec, BoxError, prelude::*};
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let worker = self.worker.clone();
        worker.send(order).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    runs: Arc<AtomicUsize>,
    delivered: mpsc::UnboundedSender<String>,
    run: usize,
}

impl Actor for Worker {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.run == 0 && order.contains("jam") {
            return Err::<_, BoxError>(Box::new(io::Error::other("press jam")));
        }
        self.delivered.send(order).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), run()).await??;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();
    let worker_runs = Arc::new(AtomicUsize::new(0));
    let worker_spec = ActorSpec::new("worker", move || Worker {
        runs: worker_runs.clone(),
        delivered: delivered_tx.clone(),
        run: 0,
    });
    let mut tree = Tree::new();
    let worker = tree.add_actor_spec(worker_spec);
    let orders_spec = ActorSpec::new("front-desk", move || Frontend {
        worker: worker.clone(),
    });
    let orders = tree.add_actor_spec(orders_spec);
    let runtime = tree.spawn()?;
    let handle = runtime.scope();

    orders.send("business cards x100".to_owned()).await?;
    println!("delivered {}", delivered_rx.recv().await.expect("delivery"));

    // Crash the worker. Each run gets a fresh mailbox, so an order queued
    // behind the jam would be lost with it — wait for the supervisor to
    // restart the worker before sending more.
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is declared")
        .generation;
    let mut restarted = handle.snapshots();
    orders.send("jam".to_owned()).await?;
    restarted
        .wait_for_child("worker", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .map_err(|_| io::Error::other("worker restart could not be observed"))?;

    orders.send("flyers x500".to_owned()).await?;
    println!("delivered {}", delivered_rx.recv().await.expect("delivery"));

    runtime.shutdown_and_wait().await?;
    Ok(())
}
