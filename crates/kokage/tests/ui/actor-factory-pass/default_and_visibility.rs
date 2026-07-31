use std::sync::{Arc, Mutex};

use kokage::prelude::*;
use kokage::ActorFactory;

mod actor {
    use super::*;

    #[derive(ActorFactory)]
    pub struct Worker {
        durable: Arc<usize>,
        #[factory(default = Mutex::new(Vec::with_capacity(Self::INITIAL_CAPACITY)))]
        local: Mutex<Vec<String>>,
    }

    impl Worker {
        const INITIAL_CAPACITY: usize = 8;
    }

    impl Actor for Worker {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
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
