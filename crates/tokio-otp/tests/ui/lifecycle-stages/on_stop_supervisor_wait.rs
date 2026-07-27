//! The supervisor waits for `on_stop` before detaching the child, so awaiting
//! the scope's own completion from this hook waits on a detach that is waiting
//! on this hook.
use tokio_otp::{Actor, ActorResult, BoxError, HandleContext, StopContext, prelude::Continue};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        Ok(Continue)
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        ctx.supervisor().wait().await?;
        Ok(())
    }
}

fn main() {}
