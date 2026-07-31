mod component {
    use kokage::prelude::*;

    pub struct Worker;

    impl Actor for Worker {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
            Ok(())
        }
    }

    #[derive(kokage::Supervision)]
    pub struct Workers {
        pub worker: Worker,
    }
}

#[derive(kokage::Supervision)]
struct App {
    #[supervision(scope)]
    workers: component::Workers,
}

fn main() {
    let (tree, handles) = App::tree(|_| AppFactories {
        workers: component::WorkersFactories { worker: || component::Worker },
    });
    let _: kokage::Tree = tree;
    let _: kokage::ActorRef<()> = handles.workers.worker;
}
