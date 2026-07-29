use std::{error::Error, sync::Arc};

use kokage::{ActorSpec, DrainPolicy, prelude::*};
use tokio::sync::{Notify, mpsc};

const JOBS: usize = 5;

enum Msg {
    Hold,
    Job(usize),
}

#[derive(Clone)]
struct Worker {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
    handled: mpsc::UnboundedSender<usize>,
}

impl Actor for Worker {
    type Msg = Msg;

    async fn handle(&mut self, message: Msg, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        match message {
            Msg::Hold => {
                self.started.send(()).expect("receiver alive");
                self.release.notified().await;
            }
            Msg::Job(job) => {
                self.handled.send(job).expect("receiver alive");
            }
        }
        Ok(())
    }

    // Drain is the default; spelled out here because it is what this example
    // demonstrates. Actors that should drop queued work at shutdown instead
    // return `DrainPolicy::Discard`.
    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), run()).await??;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let release = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (handled_tx, mut handled_rx) = mpsc::unbounded_channel();

    let actor_release = release.clone();
    let worker_spec = ActorSpec::new("Worker", move || Worker {
        started: started_tx.clone(),
        release: actor_release.clone(),
        handled: handled_tx.clone(),
    });
    let worker = worker_spec.actor_ref();

    let runtime = OrderedTree::new().actor(worker_spec).spawn()?;
    worker.send(Msg::Hold).await?;
    started_rx.recv().await.expect("worker entered hold");

    for job in 1..=JOBS {
        worker.send(Msg::Job(job)).await?;
    }

    runtime.shutdown();
    release.notify_one();

    for _ in 0..JOBS {
        println!("handled {}", handled_rx.recv().await.expect("drained job"));
    }

    runtime.wait().await?;
    Ok(())
}
