use std::sync::Arc;

use tokio_otp::{Actor, ActorContext, ActorResult, Topology};

struct ConstructorActor;

impl ConstructorActor {
    fn new() -> Self {
        Self
    }
}

impl Actor for ConstructorActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

struct CapturingActor {
    _configuration: Arc<str>,
}

impl Actor for CapturingActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct Application {
    constructor: ConstructorActor,
    capturing: CapturingActor,
}

fn main() {
    let configuration: Arc<str> = Arc::from("durable");
    Application::graph(move |_| ApplicationFactories {
        constructor: ConstructorActor::new,
        capturing: move || CapturingActor {
            _configuration: configuration.clone(),
        },
    })
    .expect("factory graph builds");
}
