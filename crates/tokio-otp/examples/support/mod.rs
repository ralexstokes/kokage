use std::error::Error;

use tokio::task::JoinHandle;
use tokio_otp::{
    CancellationToken, Graph, RestartPolicy,
    host::{ActorRunError, DEFAULT_SHUTDOWN_BOUND},
};

pub struct ActorTasks {
    stop: CancellationToken,
    tasks: Vec<JoinHandle<Result<(), ActorRunError>>>,
}

impl ActorTasks {
    pub fn start(graph: &Graph) -> Self {
        let stop = CancellationToken::new();
        let tasks = graph
            .actors()
            .iter()
            .cloned()
            .map(|actor| {
                let stop = stop.clone();
                tokio::spawn(async move {
                    actor
                        .run_until(
                            stop.cancelled(),
                            RestartPolicy::Never,
                            DEFAULT_SHUTDOWN_BOUND,
                        )
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
