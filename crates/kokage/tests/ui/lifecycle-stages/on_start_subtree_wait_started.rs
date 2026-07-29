//! Navigating to a nested scope must not hand back the waits `RestrictedScope`
//! withholds: an ordered sibling's start is sequenced after this actor reports
//! ready, so awaiting it from `on_start` deadlocks.
use kokage::{Actor, ActorResult, MessageContext, StartContext};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        if let Some(sibling) = ctx.supervisor().subtree("later_sibling") {
            sibling.wait_started().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
