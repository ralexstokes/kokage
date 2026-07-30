//! A leader's declared child scope starts only after `on_start` returns, so
//! awaiting its readiness inline can never succeed.
use kokage::{Actor, ExitResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        if let Some(children) = ctx.supervisor().subtree("children") {
            children.wait_started().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {}
