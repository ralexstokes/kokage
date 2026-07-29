use std::error::Error;

use kokage::{Actor, ActorResult, ActorSpec, Context};
use tokio::sync::mpsc;

mod support;

#[derive(Clone)]
struct Worker {
    completed: mpsc::UnboundedSender<()>,
}

impl Actor for Worker {
    type Msg = &'static str;

    async fn handle(&mut self, message: &'static str, ctx: &mut Context<'_, Self>) -> ActorResult {
        tracing::info!(message, "worker received message");
        ctx.run_blocking(|_token| ()).await?;
        self.completed.send(()).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let worker_spec = ActorSpec::new("Worker", move || Worker {
        completed: completed_tx.clone(),
    });
    let (worker_spec, worker) = worker_spec.actor_ref();

    let handle = support::ActorTasks::start([worker_spec.into_runnable()]);

    worker.send("hello tracing").await?;
    completed_rx.recv().await.expect("message processed");

    handle.shutdown_and_wait().await?;
    Ok(())
}
