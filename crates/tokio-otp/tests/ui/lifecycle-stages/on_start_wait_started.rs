//! `on_start` cannot await the enclosing scope's readiness: the actor is not
//! ready until this hook returns, so the wait depends on itself.
use tokio_otp::{Actor, ActorResult, MessageContext, StartContext, prelude::Continue};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        ctx.supervisor().wait_started().await?;
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }
}

fn main() {}
