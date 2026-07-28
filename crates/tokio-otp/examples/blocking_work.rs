use std::error::Error;

use tokio::sync::mpsc;
use tokio_otp::{
    Actor, ActorResult, AmbientContext, GraphBuilder, MessageContext, prelude::Continue,
};

mod support;

enum WorkMsg {
    Process(String),
    Finished(String),
}

#[derive(Clone)]
struct Worker {
    observed: mpsc::UnboundedSender<String>,
}

impl Actor for Worker {
    type Msg = WorkMsg;

    async fn handle(
        &mut self,
        message: WorkMsg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            WorkMsg::Process(input) => {
                let output = ctx
                    .run_blocking(move |token| {
                        if token.is_cancelled() {
                            return None;
                        }
                        Some(input.to_uppercase())
                    })
                    .await?;
                if let Some(output) = output {
                    ctx.myself().try_send(WorkMsg::Finished(output))?;
                }
            }
            WorkMsg::Finished(output) => {
                self.observed.send(output).expect("receiver alive");
            }
        }
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let (worker_slot, worker) = builder.slot("Worker");
    builder.define(worker_slot, move || Worker {
        observed: observed_tx.clone(),
    });
    let graph = builder.build()?;

    let handle = support::ActorTasks::start(&graph);

    worker
        .send(WorkMsg::Process("hello blocking actor".to_owned()))
        .await?;
    println!("result: {}", observed_rx.recv().await.expect("result"));

    handle.shutdown_and_wait().await?;
    Ok(())
}
