use kokage::{Actor, ExitResult, ActorSpec, Context, MailboxMode};

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
    let spec = ActorSpec::new("idle", || Idle);
    let first_ref = spec.actor_ref();
    let second_ref = spec.actor_ref();
    let spec = spec
        .mailbox(MailboxMode::queue())
        .message_size(message_size);
    let mut tree = kokage::Tree::new();
    tree.add_actor_spec(spec);
    assert_eq!(first_ref.id(), second_ref.id());
}
