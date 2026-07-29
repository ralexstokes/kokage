use std::error::Error;

use kokage::{
    CancellationToken, Graph, RestartPolicy,
    host::{ActorRunError, DEFAULT_SHUTDOWN_BOUND},
};
use tokio::task::JoinHandle;

pub struct ActorTasks {
    stop: CancellationToken,
    tasks: Vec<JoinHandle<Result<(), ActorRunError>>>,
}

impl ActorTasks {
    pub fn start(graph: Graph) -> Self {
        let stop = CancellationToken::new();
        let tasks = graph
            .into_nodes()
            .into_iter()
            .map(|node| {
                let stop = stop.clone();
                let actor = node.into_runnable();
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
