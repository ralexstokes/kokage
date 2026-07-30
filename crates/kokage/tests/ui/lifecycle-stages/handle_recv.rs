//! A handler must not read its own mailbox: the provided receive loop owns it,
//! and a direct read bypasses drain accounting and the continuation queue.
use kokage::{Actor, ExitResult, Context};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), ctx: &mut Context<'_, Self>) -> ExitResult {
        let _ = ctx.recv().await;
        Ok(())
    }
}

fn main() {}
