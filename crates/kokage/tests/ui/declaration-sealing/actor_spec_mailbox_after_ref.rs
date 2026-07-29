use kokage::{Actor, ActorResult, ActorSpec, Context, MailboxMode};

struct Idle;

impl Actor for Idle {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() {
    let spec = ActorSpec::new("idle", || Idle);
    let (spec, _actor_ref) = spec.actor_ref();
    let _spec = spec.mailbox(MailboxMode::queue());
}
