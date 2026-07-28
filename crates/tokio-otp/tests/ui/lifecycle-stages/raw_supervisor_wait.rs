//! A raw actor owns its receive loop, but lifecycle progress can still depend
//! on that loop returning, so its `ActorContext` exposes a restricted scope.
use tokio_otp::{ActorContext, ActorResult, RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.supervisor().wait().await?;
        Ok(())
    }
}

fn main() {}
