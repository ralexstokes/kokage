use tokio_otp::{HandleContext, ActorResult, GraphBuilder, Actor};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

fn main() {
    let mut builder = GraphBuilder::new();
    let (slot, _worker) = builder.slot::<String>("worker");
    builder.define(slot, || Worker);
}
