use tokio_otp::{ActorContext, ActorResult, RawActor, Supervision};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct App {
    #[supervision(label = "workers.manager")]
    manager: Worker,
}

fn main() {}
