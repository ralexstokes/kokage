//! A handler must not read its own mailbox: the provided receive loop owns it,
//! and a direct read bypasses drain accounting and the continuation queue.
use tokio_otp::{Actor, ActorResult, MessageContext, prelude::Continue};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        let _ = ctx.recv().await;
        Ok(Continue)
    }
}

fn main() {}
