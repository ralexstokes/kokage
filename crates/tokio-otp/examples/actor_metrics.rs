//! Actor observability uses a user-owned sampler: periodically pull
//! `ActorRef::stats()` and supervisor snapshots, then export them in the
//! application's preferred format. This example prints Prometheus-shaped text
//! without installing a metrics backend or adding an exporter dependency.

use std::{error::Error, time::Duration};

use tokio::sync::mpsc;
use tokio_otp::{CancellationToken, prelude::*};

#[derive(Clone)]
struct Worker {
    completed: mpsc::UnboundedSender<()>,
}

impl Actor for Worker {
    type Msg = &'static str;

    async fn handle(
        &mut self,
        message: &'static str,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        println!("processing `{message}`");
        self.completed.send(()).expect("receiver alive");
        Ok(())
    }
}

async fn sample(worker: ActorRef<&'static str>, runtime: RuntimeHandle, stop: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick.tick() => {
                let stats = worker.stats();
                let restarts = runtime.snapshot().child(worker.id()).map_or(0, |c| c.restart_count);
                println!("actor_messages_received_total{{actor=\"{}\"}} {}\nactor_messages_accepted_total{{actor=\"{}\"}} {}\nactor_sends_rejected_total{{actor=\"{}\"}} {}\nactor_mailbox_depth{{actor=\"{}\"}} {}\nactor_restarts_total{{actor=\"{}\"}} {}", worker.id(), stats.messages_received, worker.id(), stats.messages_accepted, worker.id(), stats.sends_rejected, worker.id(), stats.mailbox_depth, worker.id(), restarts);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (worker_slot, worker) = graph.slot("Worker");
    graph.define(worker_slot, move || Worker {
        completed: completed_tx.clone(),
    });
    let runtime = Runtime::builder().graph(graph.build()?).build()?;
    let handle = runtime.spawn();

    let sampler_stop = CancellationToken::new();
    let sampler = tokio::spawn(sample(worker.clone(), handle.clone(), sampler_stop.clone()));
    worker.send("hello stats").await?;
    completed_rx.recv().await.expect("message processed");
    tokio::time::sleep(Duration::from_millis(1100)).await;

    sampler_stop.cancel();
    sampler.await?;
    handle.shutdown_and_wait().await?;
    Ok(())
}
