use std::sync::Arc;

use tokio_otp::{Actor, ActorContext, ActorFactory, ActorResult, Topology};

struct SpecActor {
    _configuration: Arc<str>,
}

impl Actor for SpecActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

struct SpecActorFactory {
    configuration: Arc<str>,
}

impl ActorFactory for SpecActorFactory {
    type Actor = SpecActor;

    fn build(&self) -> Self::Actor {
        SpecActor {
            _configuration: self.configuration.clone(),
        }
    }
}

struct ClosureActor;

impl Actor for ClosureActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct Application {
    spec: SpecActor,
    closure: ClosureActor,
}

fn main() {
    Application::graph(|_| ApplicationFactories {
        spec: SpecActorFactory {
            configuration: Arc::from("durable"),
        },
        closure: || ClosureActor,
    })
    .expect("factory graph builds");
}
