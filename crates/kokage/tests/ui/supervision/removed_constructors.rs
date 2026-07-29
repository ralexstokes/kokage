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
    Application::graph(|_| ApplicationFactories { worker: || Worker });
    Application::graph_with(GraphBuilder::new(), |_| ApplicationFactories {
        worker: || Worker,
    });
    Application::runtime(|_| ApplicationFactories { worker: || Worker });
    Application::runtime_with(GraphBuilder::new(), |_| ApplicationFactories {
        worker: || Worker,
    });
}
