use std::error::Error;

use tokio::sync::mpsc;
use tokio_otp::{Actor, ActorResult, GraphBuilder, LiveContext, MessageContext, prelude::Continue};

mod support;

#[derive(Clone)]
struct Worker {
    completed: mpsc::UnboundedSender<()>,
}

impl Actor for Worker {
    type Msg = &'static str;

    async fn handle(
        &mut self,
        message: &'static str,
        ctx: &mut MessageContext<'_, &'static str>,
    ) -> ActorResult {
        tracing::info!(message, "worker received message");
        ctx.run_blocking(|_token| ()).await?;
        self.completed.send(()).expect("receiver alive");
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let worker = builder.add(move || Worker {
        completed: completed_tx.clone(),
    });
    let graph = builder.build()?;

    let handle = support::ActorTasks::start(&graph);

    worker.send("hello tracing").await?;
    completed_rx.recv().await.expect("message processed");

    handle.shutdown_and_wait().await?;
    Ok(())
}
