use kokage::prelude::*;

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[derive(kokage::Supervision)]
struct App {
    #[supervision(mailbox = Mailbox::queue(Self::MAILBOX_CAPACITY))]
    worker: Worker,
}

impl App {
    const MAILBOX_CAPACITY: usize = 8;
}

fn main() {
    let (tree, _) = App::tree(|_| AppFactories { worker: || Worker });
    let _: Tree = tree;
}
