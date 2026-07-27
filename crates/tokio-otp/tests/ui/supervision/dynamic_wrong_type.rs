use tokio_otp::{ActorContext, ActorResult, RawActor, Supervision};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Supervision)]
struct App {
    manager: Worker,
    #[supervision(dynamic)]
    sessions: Worker,
}

fn main() {}
