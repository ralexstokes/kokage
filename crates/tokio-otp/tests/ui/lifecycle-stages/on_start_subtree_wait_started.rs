//! Navigating to a nested scope must not hand back the waits `StartingScope`
//! withholds: an ordered sibling's start is sequenced after this actor reports
//! ready, so awaiting it from `on_start` deadlocks.
use tokio_otp::{Actor, ActorResult, MessageContext, StartContext, prelude::Continue};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        if let Some(sibling) = ctx.supervisor().subtree("later_sibling") {
            sibling.wait_started().await?;
        }
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }
}

fn main() {}
