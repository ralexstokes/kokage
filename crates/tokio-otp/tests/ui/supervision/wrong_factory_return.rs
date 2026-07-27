use tokio_otp::{Actor, MessageContext, ActorResult, Supervision};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

struct Other;

impl Actor for Other {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Supervision)]
struct Application {
    worker: Worker,
}

fn main() {
    Application::graph(|_| ApplicationFactories { worker: || Other }).unwrap();
}
