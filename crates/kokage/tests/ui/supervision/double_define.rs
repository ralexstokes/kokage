use kokage::{Actor, ActorResult, ActorSlot, GraphBuilder, MessageContext};

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
    let slot = ActorSlot::<()>::new("worker");
    builder.define(slot, || Worker);
    builder.define(slot, || Worker);
}
