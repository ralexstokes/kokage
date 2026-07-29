use kokage::{Actor, ActorResult, GraphBuilder, MessageContext, Supervision};

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
    let mut builder = GraphBuilder::new();
    Application::wire(&mut builder, |_| ApplicationFactories { worker: || Other });
}
