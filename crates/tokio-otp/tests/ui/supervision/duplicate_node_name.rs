use tokio_otp::{ActorContext, ActorResult, RawActor, Supervision};

struct Worker;

impl RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct Pool {
    parse: Worker,
    #[supervision(label = "parse")]
    render: Worker,
}

fn main() {}
