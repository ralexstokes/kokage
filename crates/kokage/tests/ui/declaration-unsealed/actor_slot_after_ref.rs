use kokage::{Actor, ActorResult, ActorSlot, Context};

struct Idle;

impl Actor for Idle {
    type Msg = String;

    async fn handle(&mut self, _: String, _: &mut Context<'_, Self>) -> ActorResult {
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
    let spec = slot.message_size(message_size).define(|| Idle);
    let _tree = kokage::OrderedTree::new().actor(spec);
    assert_eq!(first_ref.id(), second_ref.id());
}
