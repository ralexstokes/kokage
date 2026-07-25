use tokio_otp::{Actor, ActorContext, ActorResult, Topology};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

struct Other;

impl Actor for Other {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct Application {
    worker: Worker,
}

fn main() {
    Application::graph(|_| ApplicationFactories { worker: || Other }).unwrap();
}
