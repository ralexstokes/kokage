//! Continuations are abandoned once the actor begins stopping, so queuing one
//! from `on_stop` was a silent no-op before the stage split.
use tokio_otp::{Actor, ActorResult, BoxError, MessageContext, StopContext};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        ctx.continue_with(());
        Ok(())
    }
}

fn main() {}
