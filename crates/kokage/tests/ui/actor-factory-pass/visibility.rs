use std::sync::Arc;

use kokage::{Actor, ActorFactory, Context, ExitResult};

mod actor {
    use super::*;

    #[derive(ActorFactory)]
    pub struct Worker {
        durable: Arc<usize>,
        #[factory(default)]
        local: usize,
    }

    impl Actor for Worker {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
            let _ = (&self.durable, self.local);
            Ok(())
        }
    }
}

fn main() {
    let factory = actor::WorkerFactory {
        durable: Arc::new(1),
    };
    let _ = factory.build();
}
