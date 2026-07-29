//! A `RawActor` owns its loop and never drains the continuation queue, so
//! `continue_with` there dropped the message silently.
use kokage::{host::RawContext, ActorResult, host::RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ActorResult {
        ctx.continue_with(());
        Ok(())
    }
}

fn main() {}
