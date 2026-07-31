use std::{error::Error, sync::Arc};

use kokage::{
    ActorSpec, ExitResult, Mailbox, SendError, SendErrorKind,
    raw::{RawActor, RawContext},
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

    async fn run(&mut self, mut ctx: RawContext<&'static str>) -> ExitResult {
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

    let actor_release = release.clone();
    let worker_spec = ActorSpec::new("ParkBeforeRecv", move || ParkBeforeRecv {
        release: actor_release.clone(),
    })
    .mailbox(Mailbox::queue(1));
    let worker = worker_spec.actor_ref();

    let handle = support::ActorTasks::start([worker_spec.into_host()]);

    // `send` waits for the worker's mailbox to bind, so the first message
    // deterministically occupies the single mailbox slot; `try_send` before
    // the binding exists would fail with `SendErrorKind::NotRunning`.
    worker.send("first").await?;
    match worker.try_send("second") {
        Err(
            error @ SendError {
                kind: SendErrorKind::Full,
                ..
            },
        ) => {
            println!(
                "mailbox is full; recovered `{}` for another route",
                error.into_message()
            );
        }
        Ok(()) => panic!("second send unexpectedly succeeded"),
        Err(other) => panic!("unexpected send error: {other}"),
    }

    release.notify_one();
    handle.shutdown_and_wait().await?;

    Ok(())
}
