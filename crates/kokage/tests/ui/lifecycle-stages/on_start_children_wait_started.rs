//! A leader's declared child scope starts only after `on_start` returns, so
//! awaiting its readiness inline can never succeed.
use kokage::{Actor, ActorResult, MessageContext, StartContext};

struct Leader;

impl Actor for Leader {
    type Msg = ();

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        if let Some(children) = ctx.supervisor().subtree("children") {
            children.wait_started().await?;
        }
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {}
