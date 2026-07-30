use std::error::Error;

use kokage::{Actor, ActorResult, ActorSpec, Context};
use tokio::sync::mpsc;

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

    async fn handle(&mut self, message: WorkMsg, ctx: &mut Context<'_, Self>) -> ActorResult {
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
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let worker_spec = ActorSpec::new("Worker", move || Worker {
        observed: observed_tx.clone(),
    });
    let worker = worker_spec.actor_ref();

    let handle = support::ActorTasks::start([worker_spec.into_runnable()]);

    worker
        .send(WorkMsg::Process("hello blocking actor".to_owned()))
        .await?;
    println!("result: {}", observed_rx.recv().await.expect("result"));

    handle.shutdown_and_wait().await?;
    Ok(())
}
