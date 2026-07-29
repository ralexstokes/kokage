use std::error::Error;

use kokage::{
    CancellationToken, Restart,
    host::{ActorRunError, DEFAULT_SHUTDOWN_BOUND, RunnableActor},
};
use tokio::task::JoinHandle;

pub struct ActorTasks {
    stop: CancellationToken,
    tasks: Vec<JoinHandle<Result<(), ActorRunError>>>,
}

impl ActorTasks {
    pub fn start(actors: impl IntoIterator<Item = RunnableActor>) -> Self {
        let stop = CancellationToken::new();
        let tasks = actors
            .into_iter()
            .map(|actor| {
                let stop = stop.clone();
                tokio::spawn(async move {
                    actor
                        .run_until(stop.cancelled(), Restart::never(), DEFAULT_SHUTDOWN_BOUND)
                        .await
                })
            })
            .collect();
        Self { stop, tasks }
    }

    pub fn shutdown(&self) {
        self.stop.cancel();
    }

    pub async fn shutdown_and_wait(self) -> Result<(), Box<dyn Error>> {
        self.shutdown();
        self.wait().await
    }

    pub async fn wait(self) -> Result<(), Box<dyn Error>> {
        for task in self.tasks {
            task.await??;
        }
        Ok(())
    }
}
