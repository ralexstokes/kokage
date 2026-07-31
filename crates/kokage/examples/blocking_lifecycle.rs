use std::{error::Error, thread, time::Duration};

use kokage::{Actor, ActorSpec, Context, ExitResult};
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

    async fn handle(&mut self, command: Command, ctx: &mut Context<'_, Self>) -> ExitResult {
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
    let worker_spec = ActorSpec::new("Worker", move || Worker {
        completed: completed_tx.clone(),
    });
    let worker = worker_spec.actor_ref();

    let handle = support::ActorTasks::start([worker_spec.into_host()]);

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
