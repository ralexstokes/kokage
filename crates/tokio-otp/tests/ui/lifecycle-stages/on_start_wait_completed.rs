//! `wait_completed` is withheld during startup because the current actor
//! cannot complete startup until this hook returns.

use tokio_otp::{Actor, ActorResult, MessageContext, StartContext, prelude::Continue};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let _ = ctx.supervisor().wait_completed(["worker"]).await;
        Ok(Continue)
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(Continue)
    }
}

fn main() {}
