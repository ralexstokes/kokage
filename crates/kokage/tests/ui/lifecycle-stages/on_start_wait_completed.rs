//! Completion watches from restricted scopes cannot be awaited.

use kokage::{Actor, ExitResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let _ = ctx.supervisor().completions(["worker"]).wait().await;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {}
