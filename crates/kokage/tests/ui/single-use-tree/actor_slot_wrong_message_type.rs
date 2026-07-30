use kokage::{Actor, ActorResult, ActorSlot, Context};

struct TextActor;

impl Actor for TextActor {
    type Msg = String;

    async fn handle(
        &mut self,
        _: String,
        _: &mut Context<'_, Self>,
    ) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let slot = ActorSlot::<u32>::new("text");
    let _ = slot.define(|| TextActor);
}
