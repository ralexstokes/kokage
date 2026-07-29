//! Message handling receives the same restricted scope as every other stage:
//! a lifecycle wait may depend on this actor returning from the handler.
use kokage::{Actor, ActorResult, MessageContext};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        ctx.supervisor().wait().await?;
        Ok(())
    }
}

fn main() {}
