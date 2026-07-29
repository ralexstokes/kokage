//! `on_start` cannot await the enclosing scope's readiness: the actor is not
//! ready until this hook returns, so the wait depends on itself.
use kokage::{Actor, ActorResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ActorResult {
        ctx.supervisor().wait_started().await?;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
