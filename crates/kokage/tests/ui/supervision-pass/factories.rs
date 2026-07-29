use std::sync::Arc;

use kokage::{Actor, ActorResult, GraphBuilder, MessageContext, Supervision};

struct ConstructorActor;

impl ConstructorActor {
    fn new() -> Self {
        Self
    }
}

impl Actor for ConstructorActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

struct CapturingActor {
    _configuration: Arc<str>,
}

impl Actor for CapturingActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct Application {
    constructor: ConstructorActor,
    capturing: CapturingActor,
}

fn main() {
    let configuration: Arc<str> = Arc::from("durable");
    let mut builder = GraphBuilder::new();
    Application::wire(&mut builder, move |_| ApplicationFactories {
        constructor: ConstructorActor::new,
        capturing: move || CapturingActor {
            _configuration: configuration.clone(),
        },
    });
}
