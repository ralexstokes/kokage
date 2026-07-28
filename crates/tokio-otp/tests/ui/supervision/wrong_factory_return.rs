use tokio_otp::{Actor, ActorResult, MessageContext, Supervision};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

struct Other;

impl Actor for Other {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct Application {
    worker: Worker,
}

fn main() {
    Application::tree(|_| ApplicationFactories { worker: || Other }).unwrap();
}
