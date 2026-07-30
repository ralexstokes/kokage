//! A raw actor owns its receive loop, but lifecycle progress can still depend
//! on that loop returning, so its `RawContext` exposes a restricted scope.
use kokage::{host::RawContext, ExitResult, host::RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, ctx: RawContext<Self::Msg>) -> ExitResult {
        ctx.supervisor().wait().await?;
        Ok(())
    }
}

fn main() {}
