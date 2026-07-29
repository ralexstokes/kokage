use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kokage::{
    Actor, ActorRef, ActorResult, ActorSpec, GraphBuilder, MessageContext, OrderedTree,
    StartContext, host::BoxError,
};
use kokage_supervisor::RestartConfig;
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        let worker = self.worker.clone();
        worker.send(order).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Worker {
    runs: Arc<AtomicUsize>,
    observed: mpsc::UnboundedSender<String>,
    run: usize,
}

impl Actor for Worker {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        if self.run == 0 && order == "fail-worker" {
            return Err::<_, BoxError>(Box::new(io::Error::other("worker failed")));
        }
        self.observed.send(order).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let worker_runs = Arc::new(AtomicUsize::new(0));
    let worker = builder.actor("worker", move || Worker {
        runs: worker_runs.clone(),
        observed: observed_tx.clone(),
        run: 0,
    });
    let frontend = builder.actor("frontend", {
        let worker = worker.clone();
        move || Frontend {
            worker: worker.clone(),
        }
    });
    let graph = builder.build()?;
    let frontend_actor = graph.actor_for(&frontend)?;
    let worker_actor = graph.actor_for(&worker)?;

    let runtime = OrderedTree::new()
        .actor(frontend_actor)
        .actor(
            ActorSpec::new(worker_actor)
                .restart_config(RestartConfig::new(5, std::time::Duration::from_secs(5))),
        )
        .spawn()?;

    let restarted = runtime.restart_of("worker");
    frontend.send("fail-worker".to_owned()).await?;
    restarted
        .await
        .ok_or_else(|| io::Error::other("worker restart could not be observed"))?;
    frontend.send("after-restart".to_owned()).await?;
    println!("observed {}", observed_rx.recv().await.expect("message"));

    runtime.shutdown_and_wait().await?;
    Ok(())
}
