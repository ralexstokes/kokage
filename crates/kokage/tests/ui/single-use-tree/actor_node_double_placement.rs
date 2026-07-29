use kokage::{Actor, ActorNode, ActorResult, ActorSpec, GraphBuilder, MessageContext, OrderedTree};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn node() -> ActorNode {
    let mut builder = GraphBuilder::new();
    builder.actor(ActorSpec::new("idle", || Idle));
    builder.build().unwrap().into_nodes().pop().unwrap()
}

fn main() {
    let node = node();
    let _tree = OrderedTree::new().actor(node).actor(node);
}
