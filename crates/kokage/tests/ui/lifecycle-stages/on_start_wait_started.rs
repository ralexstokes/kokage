//! `on_start` cannot await the enclosing scope's readiness: the actor is not
//! ready until this hook returns, so the wait depends on itself.
use kokage::{Actor, ExitResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.supervisor().wait_started().await?;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {}
