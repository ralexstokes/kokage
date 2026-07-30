use kokage::{Actor, ActorResult, ActorSpec, Context, MailboxMode};

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
    let spec = ActorSpec::new("idle", || Idle);
    let first_ref = spec.actor_ref();
    let second_ref = spec.actor_ref();
    let spec = spec
        .mailbox(MailboxMode::queue())
        .message_size(message_size);
    let _tree = kokage::OrderedTree::new().actor(spec);
    assert_eq!(first_ref.id(), second_ref.id());
}
