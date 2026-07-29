use kokage::{Actor, ActorResult, ActorSlot, MessageContext};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let slot = ActorSlot::<()>::new("idle");
    let _first = slot.define(|| Idle);
    let _second = slot.define(|| Idle);
}
