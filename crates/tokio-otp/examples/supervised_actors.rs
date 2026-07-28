use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::mpsc;
use tokio_otp::prelude::*;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        let worker = self.worker.clone();
        worker.send(order).await?;
        Ok(Continue)
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

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        if self.run == 0 && order.contains("jam") {
            return Err::<_, BoxError>(Box::new(io::Error::other("press jam")));
        }
        self.delivered.send(order).expect("receiver alive");
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), run()).await??;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let (worker_slot, worker_ref) =
        builder.slot::<String>("worker", tokio_otp::ActorOptions::new());
    let frontend_worker = worker_ref.clone();
    let (orders_slot, orders) = builder.slot("front-desk", tokio_otp::ActorOptions::new());
    builder.define(orders_slot, move || Frontend {
        worker: frontend_worker.clone(),
    });
    let worker_runs = Arc::new(AtomicUsize::new(0));
    builder.define(worker_slot, move || Worker {
        runs: worker_runs.clone(),
        delivered: delivered_tx.clone(),
        run: 0,
    });
    let graph = builder.build()?;

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .restart(RestartPolicy::OnFailure)
        .build()?;
    let handle = runtime.spawn();

    orders.send("business cards x100".to_owned()).await?;
    println!("delivered {}", delivered_rx.recv().await.expect("delivery"));

    // Crash the worker. Each run gets a fresh mailbox, so an order queued
    // behind the jam would be lost with it — wait for the supervisor to
    // restart the worker before sending more.
    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;
    orders.send("jam".to_owned()).await?;
    lifecycle
        .started_after("worker", baseline)
        .await
        .ok_or_else(|| io::Error::other("worker restart could not be observed"))?;

    orders.send("flyers x500".to_owned()).await?;
    println!("delivered {}", delivered_rx.recv().await.expect("delivery"));

    handle.shutdown_and_wait().await?;
    Ok(())
}
