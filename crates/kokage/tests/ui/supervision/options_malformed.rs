use kokage::{host::ActorContext, ActorResult, Supervision, host::RawActor};

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
    #[supervision(options)]
    park: Park,
}

fn main() {}
