use std::marker::PhantomData;

use tokio_otp::{ActorContext, ActorResult, Actor, Topology};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
struct GenericTopology<T> {
    worker: Worker,
    _marker: PhantomData<T>,
}

fn main() {}
