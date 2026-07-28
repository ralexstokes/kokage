use tokio_otp::{Actor, MessageContext, ActorResult, Supervision};

mod application {
    use super::*;

    pub struct Message;

    pub struct Worker;

    impl Actor for Worker {
        type Msg = Message;

        async fn handle(
            &mut self,
            _: Message,
            _: &mut MessageContext<'_, Self>,
        ) -> ActorResult {
            Ok(())
        }
    }

    #[derive(Supervision)]
    pub struct Application {
        pub worker: Worker,
    }
}

fn assert_clone<T: Clone>(_: &T) {}

fn main() {
    let (graph, refs) = application::Application::graph(|_| {
        application::ApplicationFactories {
            worker: || application::Worker,
        }
    })
    .expect("derived graph with refs builds");

    assert_clone(&refs);
    assert_eq!(refs.worker.id(), "worker");
    drop(graph);
}
