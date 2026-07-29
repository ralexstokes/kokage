//! Shutdown-stage code cannot start actor-owned lifecycle waits.

use kokage::{Actor, ActorResult, Context, StopContext, host::BoxError};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        let scope = ctx.supervisor();
        ctx.spawn_scope_wait(
            &scope,
            |handle| async move { handle.wait_started().await },
            |_| (),
        );
        Ok(())
    }
}

fn main() {}
