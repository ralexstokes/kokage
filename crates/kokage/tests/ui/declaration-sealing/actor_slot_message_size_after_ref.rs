use kokage::ActorSlot;

fn message_size(message: &String) -> usize {
    message.len()
}

fn main() {
    let slot = ActorSlot::<String>::new("idle");
    let (slot, _actor_ref) = slot.actor_ref();
    let _slot = slot.message_size(message_size);
}
