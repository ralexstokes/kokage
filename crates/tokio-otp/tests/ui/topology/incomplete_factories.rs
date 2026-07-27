use tokio_otp::{Actor, MessageContext, ActorResult, Topology};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct Application {
    first: Worker,
    second: Worker,
}

fn main() {
    Application::graph(|_| ApplicationFactories { first: || Worker }).unwrap();
}
