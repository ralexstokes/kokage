use kokage::{Actor, ExitResult, ActorSlot, Context};

struct Idle;

impl Actor for Idle {
    type Msg = String;

    async fn handle(&mut self, _: String, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn message_size(message: &String) -> usize {
    message.len()
}

fn main() {
    let slot = ActorSlot::<String>::new("idle");
    let first_ref = slot.actor_ref();
    let second_ref = slot.actor_ref();
    let spec = slot.define(|| Idle).message_size(message_size);
    let mut tree = kokage::Tree::new();
    tree.add_actor_spec(spec);
    assert_eq!(first_ref.id(), second_ref.id());
}
