use tokio_otp::{ActorContext, ActorResult, RawActor, Supervision};

#[derive(Clone)]
struct Park;

impl RawActor for Park {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Supervision)]
struct ParkGraph {
    #[supervision(options = tokio_otp::ActorOptions::new())]
    #[supervision(options = tokio_otp::ActorOptions::new())]
    park: Park,
}

fn main() {}
