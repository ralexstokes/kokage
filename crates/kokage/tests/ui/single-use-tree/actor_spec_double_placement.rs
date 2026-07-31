use kokage::{Actor, ExitResult, ActorSpec, Context, OrderedTree};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let mut tree = OrderedTree::new();
    tree.add_actor(spec);
    tree.add_actor(spec);
}
