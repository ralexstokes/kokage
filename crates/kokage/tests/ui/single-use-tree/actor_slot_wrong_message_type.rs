use kokage::{Actor, ExitResult, ActorSlot, Context};

struct TextActor;

impl Actor for TextActor {
    type Msg = String;

    async fn handle(
        &mut self,
        _: String,
        _: &mut Context<'_, Self>,
    ) -> ExitResult {
        Ok(())
    }
}

fn main() {
    let slot = ActorSlot::<u32>::new("text");
    let _ = slot.define(|| TextActor);
}
