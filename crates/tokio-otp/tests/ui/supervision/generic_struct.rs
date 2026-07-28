use std::marker::PhantomData;

use tokio_otp::{MessageContext, ActorResult, Actor, Supervision};

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct GenericScope<T> {
    worker: Worker,
    _marker: PhantomData<T>,
}

fn main() {}
