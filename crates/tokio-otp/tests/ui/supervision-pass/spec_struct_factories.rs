use std::sync::Arc;

use tokio_otp::{Actor, ActorFactory, ActorResult, MessageContext, Supervision};

struct SpecActor {
    _configuration: Arc<str>,
}

impl Actor for SpecActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
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

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct Application {
    spec: SpecActor,
    closure: ClosureActor,
}

fn main() {
    Application::tree(|_| ApplicationFactories {
        spec: SpecActorFactory {
            configuration: Arc::from("durable"),
        },
        closure: || ClosureActor,
    })
    .expect("factory tree builds");
}
