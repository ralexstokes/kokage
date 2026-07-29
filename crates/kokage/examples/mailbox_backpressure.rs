use std::{error::Error, sync::Arc};

use kokage::{
    ActorResult, GraphBuilder, TrySendError,
    host::{ActorContext, RawActor},
};
use tokio::sync::Notify;

mod support;

#[derive(Clone)]
struct ParkBeforeRecv {
    release: Arc<Notify>,
}

// Direct `RawActor` remains the escape hatch when an actor needs custom loop
// control. This example parks before receiving so the mailbox visibly fills.
impl RawActor for ParkBeforeRecv {
    type Msg = &'static str;

    async fn run(&mut self, mut ctx: ActorContext<&'static str>) -> ActorResult {
        self.release.notified().await;
        while let Some(message) = ctx.recv().await {
            println!("worker received `{message}`");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let release = Arc::new(Notify::new());

    let mut builder = GraphBuilder::new();
    builder.name("backpressure");
    builder.mailbox_capacity(1);
    let actor_release = release.clone();
    let (worker_slot, worker) = builder.slot("ParkBeforeRecv");
    builder.define(worker_slot, move || ParkBeforeRecv {
        release: actor_release.clone(),
    });
    let graph = builder.build()?;

    let handle = support::ActorTasks::start(&graph);

    // `send` waits for the worker's mailbox to bind, so the first message
    // deterministically occupies the single mailbox slot; `try_send` before
    // the binding exists would fail with `TrySendError::NotRunning`.
    worker.send("first").await?;
    match worker.try_send("second") {
        Err(TrySendError::Full { actor_id, .. }) => {
            println!("`{actor_id}` mailbox is full");
        }
        Ok(()) => panic!("second send unexpectedly succeeded"),
        Err(other) => panic!("unexpected send error: {other}"),
    }

    release.notify_one();
    handle.shutdown_and_wait().await?;

    Ok(())
}
