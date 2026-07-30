//! Completion watches from restricted scopes cannot be awaited.

use kokage::{Actor, ActorResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ActorResult {
        let _ = ctx.supervisor().completions(["worker"]).wait().await;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
