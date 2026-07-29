use kokage::{Actor, ActorResult, ActorSpec, MessageContext};

struct Idle;

impl Actor for Idle {
    type Msg = String;

    async fn handle(&mut self, _: String, _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn message_size(message: &String) -> usize {
    message.len()
}

fn main() {
    let (sealed, _actor_ref) = ActorSpec::new("idle", || Idle).actor_ref();
    let _sealed = sealed.message_size(message_size);
}
