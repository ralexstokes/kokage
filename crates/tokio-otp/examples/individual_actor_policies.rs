use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::mpsc;
use tokio_otp::{
    Actor, ActorRef, ActorResult, ActorSpec, BoxError, GraphBuilder, MessageContext, StartContext,
    SupervisionTree,
};
use tokio_supervisor::{RestartConfig, RestartPolicy, Strategy};

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
    let (worker_slot, worker_ref) = builder.slot::<String>("worker");
    let (frontend_slot, frontend) = builder.slot("frontend");
    builder.define(frontend_slot, {
        let worker_ref = worker_ref.clone();
        move || Frontend {
            worker: worker_ref.clone(),
        }
    });
    let worker_runs = Arc::new(AtomicUsize::new(0));
    builder.define(worker_slot, move || Worker {
        runs: worker_runs.clone(),
        observed: observed_tx.clone(),
        run: 0,
    });
    let graph = builder.build()?;
    let frontend_actor = graph.actor_for(&frontend)?;
    let worker_actor = graph.actor_for(&worker_ref)?;

    let runtime = SupervisionTree::new()
        .strategy(Strategy::OneForOne)
        .actor(frontend_actor)
        .actor(
            ActorSpec::new(worker_actor)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartConfig::new(5, std::time::Duration::from_secs(5))),
        )
        .build()?;
    let handle = runtime.spawn();

    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;
    frontend.send("fail-worker".to_owned()).await?;
    lifecycle
        .started_after("worker", baseline)
        .await
        .ok_or_else(|| io::Error::other("worker restart could not be observed"))?;
    frontend.send("after-restart".to_owned()).await?;
    println!("observed {}", observed_rx.recv().await.expect("message"));

    handle.shutdown_and_wait().await?;
    Ok(())
}
