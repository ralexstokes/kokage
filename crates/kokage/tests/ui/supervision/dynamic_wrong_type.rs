use kokage::{ActorContext, ActorResult, Supervision, host::RawActor};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct App {
    manager: Worker,
    #[supervision(dynamic)]
    sessions: Worker,
}

fn main() {}
