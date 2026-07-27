//! The raw supervisor handle carries the same lifecycle waits, so `on_start`
//! cannot reach it directly; `after_start()` is the way through.
use tokio_otp::{Actor, ActorResult, MessageContext, StartContext, prelude::Continue};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        ctx.supervisor().supervisor_handle().wait_started().await?;
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }
}

fn main() {}
