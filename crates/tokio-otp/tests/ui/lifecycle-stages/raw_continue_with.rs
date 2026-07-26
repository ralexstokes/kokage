//! A `RawActor` owns its loop and never drains the continuation queue, so
//! `continue_with` there dropped the message silently.
use tokio_otp::{ActorContext, ActorResult, Flow, RawActor};

struct Custom;

impl RawActor for Custom {
    type Msg = ();

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.continue_with(());
        Ok(Flow::Stop)
    }
}

fn main() {}
