//! A `RawActor` owns its loop and never drains the continuation queue, so
//! `continue_with` there dropped the message silently.
use tokio_otp::{ActorContext, ActorResult, RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.continue_with(());
        Ok(())
    }
}

fn main() {}
