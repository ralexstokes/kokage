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
    let _tree = OrderedTree::new().actor(spec).actor(spec);
}
