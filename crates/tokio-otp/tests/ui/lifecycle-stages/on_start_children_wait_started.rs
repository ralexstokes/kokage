//! A leader's declared child scope starts only after `on_start` returns, so
//! awaiting its readiness inline can never succeed.
use tokio_otp::{Actor, ActorResult, MessageContext, StartContext, prelude::Continue};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        if let Some(children) = ctx.children() {
            children.wait_started().await?;
        }
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }
}

fn main() {}
