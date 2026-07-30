//! Navigating to a nested scope must not hand back the waits `RestrictedScopeRef`
//! withholds: an ordered sibling's start is sequenced after this actor reports
//! ready, so awaiting it from `on_start` deadlocks.
use kokage::{Actor, ExitResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        if let Some(sibling) = ctx.supervisor().subtree("later_sibling") {
            sibling.wait_started().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn main() {}
