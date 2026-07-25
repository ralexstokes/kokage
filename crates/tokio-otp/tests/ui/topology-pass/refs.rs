use tokio_otp::{Actor, ActorContext, ActorResult, Topology};

mod application {
    use super::*;

    pub struct Message;

    pub struct Worker;

    impl Actor for Worker {
        type Msg = Message;

        async fn handle(
            &mut self,
            _: Message,
            _: &mut ActorContext<Message>,
        ) -> ActorResult {
            Ok(tokio_otp::prelude::Continue)
        }
    }

    #[derive(Topology)]
    pub struct Application {
        pub worker: Worker,
    }
}

fn assert_clone<T: Clone>(_: &T) {}

fn main() {
    let (graph, refs) = application::Application::graph_with_refs(|_| {
        application::ApplicationFactories {
            worker: || application::Worker,
        }
    })
    .expect("topology with refs builds");

    assert_clone(&refs);
    assert_eq!(refs.worker.id(), "worker");
    drop(graph);
}
