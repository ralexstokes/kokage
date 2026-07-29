use std::{error::Error, future::pending, marker::PhantomData};

use kokage::{
    Actor, ActorResult, ActorSpec, CancellationToken, Context, Restart, Shutdown,
    host::DEFAULT_SHUTDOWN_BOUND,
};
use tokio::sync::mpsc;

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

    async fn handle(&mut self, message: Command<M>, _ctx: &mut Context<'_, Self>) -> ActorResult {
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
    let (spec, frontend) = spec.actor_ref();
    let actor = spec.into_runnable();
    let first_run = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .run_until(
                    pending::<()>(),
                    Restart::on_failure(),
                    Shutdown::drain_for(DEFAULT_SHUTDOWN_BOUND),
                )
                .await
        }
    });
    frontend.send(Command::Observe("first".to_owned())).await?;
    println!("observed {:?}", observed_rx.recv().await);

    frontend.send(Command::Fail).await?;
    let _failure = first_run.await?.expect_err("first run fails");
    println!("actor is waiting for its next binding");

    let stop = CancellationToken::new();
    let second_run = tokio::spawn({
        let stop = stop.clone();
        async move {
            actor
                .run_until(
                    stop.cancelled(),
                    Restart::on_failure(),
                    Shutdown::drain_for(DEFAULT_SHUTDOWN_BOUND),
                )
                .await
        }
    });
    frontend.send(Command::Observe("second".to_owned())).await?;
    println!("observed {:?}", observed_rx.recv().await);
    stop.cancel();
    second_run.await??;

    Ok(())
}
