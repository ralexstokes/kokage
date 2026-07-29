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
    let spec = ActorSpec::new("idle", || Idle);
    let _actor_ref = spec.__actor_ref();
    let _spec = spec.message_size(message_size);
}
