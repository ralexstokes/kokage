//! A `RawActor` owns its loop and never drains the continuation queue, so
//! `continue_with` there dropped the message silently.
use kokage::{raw::RawContext, ExitResult, raw::RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        ctx.continue_with(());
        Ok(())
    }
}

fn main() {}
