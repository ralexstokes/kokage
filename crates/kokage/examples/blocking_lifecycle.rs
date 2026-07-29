use std::{error::Error, thread, time::Duration};

use kokage::{Actor, ActorResult, ActorSpec, GraphBuilder, MessageContext};
use tokio::sync::mpsc;

mod support;

enum Command {
    Start(i64),
    Finished(Result<u64, String>),
}

#[derive(Clone)]
struct Worker {
    completed: mpsc::UnboundedSender<Result<u64, String>>,
}

impl Actor for Worker {
    type Msg = Command;

    async fn handle(
        &mut self,
        command: Command,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match command {
            Command::Start(input) => {
                let myself = ctx.myself();
                tokio::task::spawn_blocking(move || {
                    thread::sleep(Duration::from_millis(20));
                    let outcome = u64::try_from(input)
                        .map(|value| value * value)
                        .map_err(|_| format!("{input} is negative"));
                    let _ = myself.try_send(Command::Finished(outcome));
                });
            }
            Command::Finished(outcome) => {
                self.completed.send(outcome).expect("receiver alive");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let worker = builder.actor(ActorSpec::new("Worker", move || Worker {
        completed: completed_tx.clone(),
    }));
    let graph = builder.build()?;

    let handle = support::ActorTasks::start(graph);

    worker.send(Command::Start(12)).await?;
    worker.send(Command::Start(-1)).await?;
    println!(
        "first outcome: {:?}",
        completed_rx.recv().await.expect("outcome")
    );
    println!(
        "second outcome: {:?}",
        completed_rx.recv().await.expect("outcome")
    );

    handle.shutdown_and_wait().await?;
    Ok(())
}
