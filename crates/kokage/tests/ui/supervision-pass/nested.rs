use kokage::prelude::*;
use kokage::{DynamicScopeRef, ScopeRef};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[derive(kokage::Supervision)]
struct Workers {
    worker: Worker,
}

#[derive(kokage::Supervision)]
struct App {
    #[supervision(scope)]
    workers: Workers,
    #[supervision(dynamic)]
    sessions: kokage::DynamicScope,
}

fn main() {
    let (tree, handles) = App::tree(|_| AppFactories {
        workers: WorkersFactories { worker: || Worker },
    });
    let _: ScopeRef = handles.scope();
    let _: ActorRef<()> = handles.workers.worker;
    let _: DynamicScopeRef = handles.sessions;
    let _: Tree = tree;
}
