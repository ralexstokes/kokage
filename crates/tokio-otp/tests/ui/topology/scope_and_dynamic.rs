use tokio_otp::{ActorContext, ActorResult, DynamicScope, RawActor, Topology};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct App {
    manager: Worker,
    #[topology(scope, dynamic)]
    sessions: DynamicScope,
}

fn main() {}
