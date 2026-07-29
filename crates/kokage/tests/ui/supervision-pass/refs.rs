use kokage::{Actor, ActorResult, GraphBuilder, MessageContext, Supervision};

mod application {
    use super::*;

    pub struct Message;

    pub struct Worker;

    impl Actor for Worker {
        type Msg = Message;

        async fn handle(&mut self, _: Message, _: &mut MessageContext<'_, Self>) -> ActorResult {
            Ok(())
        }
    }

    #[derive(Supervision)]
    pub struct Application {
        pub worker: fn() -> Worker,
    }
}

fn assert_clone<T: Clone>(_: &T) {}

fn main() {
    let mut builder = GraphBuilder::new();
    builder.name("application").mailbox_capacity(8);
    let refs = application::Application::wire(&mut builder, |_| application::Application {
        worker: || application::Worker,
    });
    let graph = builder.build().expect("derived graph builds");

    assert_clone(&refs);
    assert_eq!(refs.worker.id(), "worker");
    drop(graph);
}
