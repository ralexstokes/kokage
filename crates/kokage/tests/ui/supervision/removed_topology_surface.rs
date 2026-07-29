use kokage::{Actor, ActorResult, GraphBuilder, MessageContext, Supervision};

struct Worker;

impl Actor for Worker {
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
    let _: Option<ApplicationSlots> = None;
    let _: Option<ApplicationScopes> = None;
    Application::tree(|_| ApplicationFactories { worker: || Worker });
    Application::tree_with(GraphBuilder::new(), |_| ApplicationFactories {
        worker: || Worker,
    });
}
