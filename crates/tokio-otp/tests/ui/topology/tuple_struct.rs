use tokio_otp::{HandleContext, ActorResult, Actor, Topology};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct TupleTopology(Worker);

fn main() {}
