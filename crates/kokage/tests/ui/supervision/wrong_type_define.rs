use kokage::{MessageContext, ActorResult, GraphBuilder, Actor};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let mut builder = GraphBuilder::new();
    let (slot, _worker) = builder.slot::<String>("worker");
    builder.define(slot, || Worker);
}
