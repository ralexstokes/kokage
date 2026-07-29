use kokage::{ActorResult, DynamicScope, Supervision, host::{ActorContext, RawActor}};

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
    #[supervision(scope)]
    sessions: DynamicScope,
}

fn main() {}
