//! `on_start` cannot await the enclosing scope's readiness: the actor is not
//! ready until this hook returns, so the wait depends on itself.
use kokage::{Actor, ActorResult, MessageContext, StartContext};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.supervisor().wait_started().await?;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
