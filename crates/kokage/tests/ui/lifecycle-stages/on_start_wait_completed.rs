//! `wait_completed` is withheld during startup because the current actor
//! cannot complete startup until this hook returns.

use kokage::{Actor, ActorResult, Context};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ActorResult {
        let _ = ctx.supervisor().wait_completed(["worker"]).await;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
