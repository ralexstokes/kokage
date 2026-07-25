use std::{error::Error, time::Duration};

use tokio::task::JoinHandle;
use tokio_otp::{ActorRunError, CancellationToken, Graph, RestartPolicy};

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
                            Duration::from_secs(5),
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
