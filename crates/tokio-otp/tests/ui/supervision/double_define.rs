use tokio_otp::{MessageContext, ActorResult, GraphBuilder, Actor};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

fn main() {
    let mut builder = GraphBuilder::new();
    let (slot, _worker) = builder.slot::<()>("worker", tokio_otp::ActorOptions::new());
    builder.define(slot, || Worker);
    builder.define(slot, || Worker);
}
