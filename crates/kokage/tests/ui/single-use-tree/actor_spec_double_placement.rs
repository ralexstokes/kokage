use kokage::{Actor, ActorResult, ActorSpec, MessageContext, OrderedTree};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let _tree = OrderedTree::new().actor(spec).actor(spec);
}
