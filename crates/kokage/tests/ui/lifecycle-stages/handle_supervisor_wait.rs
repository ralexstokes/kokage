//! Message handling receives the same restricted scope as every other stage:
//! a lifecycle wait may depend on this actor returning from the handler.
use kokage::{Actor, ExitResult, Context};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.supervisor().wait().await?;
        Ok(())
    }
}

fn main() {}
