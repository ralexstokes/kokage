use std::{error::Error, future::pending, marker::PhantomData};

use kokage::{
    Actor, ActorSpec, CancellationToken, Context, ExitResult, Shutdown,
    raw::{DEFAULT_SHUTDOWN_BOUND, IncarnationExit},
};
use tokio::sync::{mpsc, oneshot};

enum Command<M> {
    Observe(M),
    Fail,
}

struct Observe<M> {
    observed: mpsc::UnboundedSender<M>,
    _message: PhantomData<fn(M)>,
}

impl<M> Observe<M> {
    fn new(observed: mpsc::UnboundedSender<M>) -> Self {
        Self {
            observed,
            _message: PhantomData,
        }
    }
}

impl<M> Clone for Observe<M> {
    fn clone(&self) -> Self {
        Self {
            observed: self.observed.clone(),
            _message: PhantomData,
        }
    }
}

impl<M: Send + 'static> Actor for Observe<M> {
    type Msg = Command<M>;

    async fn handle(&mut self, message: Command<M>, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Command::Observe(message) => {
                self.observed.send(message).expect("receiver alive");
                Ok(())
            }
            Command::Fail => Err(std::io::Error::other("restart requested").into()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), run()).await??;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

    let spec = ActorSpec::new("Observe", move || {
        Observe::<String>::new(observed_tx.clone())
    });
    let frontend = spec.actor_ref();
    let mut actor = spec.into_host();
    let stop = CancellationToken::new();
    let (first_exit_tx, first_exit_rx) = oneshot::channel();
    let (restart_tx, restart_rx) = oneshot::channel();
    let actor_task = tokio::spawn({
        let stop = stop.clone();
        async move {
            let first_exit = actor
                .run_incarnation(
                    pending::<()>(),
                    Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
                )
                .await;
            let _ = first_exit_tx.send(first_exit);
            let _ = restart_rx.await;
            actor
                .run_once(
                    stop.cancelled(),
                    Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
                )
                .await
        }
    });
    frontend.send(Command::Observe("first".to_owned())).await?;
    println!("observed {:?}", observed_rx.recv().await);

    frontend.send(Command::Fail).await?;
    assert!(matches!(first_exit_rx.await?, IncarnationExit::Failed(_)));
    println!("actor is waiting for its next binding");

    let _ = restart_tx.send(());
    frontend.send(Command::Observe("second".to_owned())).await?;
    println!("observed {:?}", observed_rx.recv().await);
    stop.cancel();
    actor_task.await??;

    Ok(())
}
