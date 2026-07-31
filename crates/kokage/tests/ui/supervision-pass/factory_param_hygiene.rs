use kokage::prelude::*;

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct F1;

impl Actor for F1 {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[derive(kokage::Supervision)]
struct F0 {
    worker: Worker,
    named: F1,
}

fn main() {
    let (tree, handles) = F0::tree(|_| F0Factories {
        worker: || Worker,
        named: || F1,
    });
    let _: Tree = tree;
    let _: ActorRef<()> = handles.worker;
    let _: ActorRef<()> = handles.named;
}
