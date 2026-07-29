use tokio_otp::{ActorContext, ActorResult, Supervision, host::RawActor};

#[derive(Clone)]
struct Park;

impl RawActor for Park {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct ParkGraph {
    #[supervision(options = tokio_otp::ActorOptions::new())]
    #[supervision(options = tokio_otp::ActorOptions::new())]
    park: Park,
}

fn main() {}
