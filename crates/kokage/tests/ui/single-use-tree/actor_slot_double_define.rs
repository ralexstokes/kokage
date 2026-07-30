use kokage::{Actor, ActorResult, ActorSlot, Context};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let slot = ActorSlot::<()>::new("idle");
    let _first = slot.define(|| Idle);
    let _second = slot.define(|| Idle);
}
