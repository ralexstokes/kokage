use tokio_otp::{ActorContext, ActorResult, RawActor, Topology};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct Pool {
    #[topology(leader)]
    manager: Worker,
}

fn main() {}
