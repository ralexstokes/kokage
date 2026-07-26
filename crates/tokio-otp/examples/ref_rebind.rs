use std::{error::Error, future::pending, marker::PhantomData};

use tokio::sync::mpsc;
use tokio_otp::{
    Actor, ActorContext, ActorResult, CancellationToken, DEFAULT_SHUTDOWN_BOUND, GraphBuilder,
    RestartPolicy, prelude::Continue,
};

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

    async fn handle(
        &mut self,
        message: Command<M>,
        _ctx: &mut ActorContext<Command<M>>,
    ) -> ActorResult {
        match message {
            Command::Observe(message) => {
                self.observed.send(message).expect("receiver alive");
                Ok(Continue)
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

    let mut builder = GraphBuilder::new();
    let frontend = builder.add(move || Observe::<String>::new(observed_tx.clone()));
    let graph = builder.build()?;
    let actor = graph.actors()[0].clone();
    let first_run = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .run_until(
                    pending::<()>(),
                    RestartPolicy::OnFailure,
                    DEFAULT_SHUTDOWN_BOUND,
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
                    RestartPolicy::OnFailure,
                    DEFAULT_SHUTDOWN_BOUND,
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
