use tokio_otp::{ActorContext, ActorResult, DynamicScope, RawActor, Supervision};

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
    #[supervision(dynamic, restart = tokio_otp::RestartPolicy::Never)]
    sessions: DynamicScope,
}

fn main() {}
