use std::sync::{Arc, Mutex};

use tokio_otp::prelude::*;
use tokio_otp::ActorFactory;

mod actor {
    use super::*;

    #[derive(ActorFactory)]
    pub struct Worker {
        durable: Arc<usize>,
        #[factory(default)]
        local: Mutex<Vec<String>>,
    }

    impl Actor for Worker {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
            let _ = (&self.durable, &self.local);
            Ok(())
        }
    }
}

fn assert_factory<F: ActorFactory<Actor = actor::Worker> + Clone>() {}

fn main() {
    assert_factory::<actor::WorkerFactory>();
    let factory = actor::WorkerFactory {
        durable: Arc::new(1),
    };
    let _ = factory.build();
}
