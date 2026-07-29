use kokage::{
    host::ActorContext, ActorOptions, ActorResult, MailboxMode, Supervision, host::RawActor,
};

struct MailboxMessage;

#[derive(Clone)]
struct MailboxWorker;

impl RawActor for MailboxWorker {
    type Msg = MailboxMessage;

    async fn run(&mut self, _: ActorContext<MailboxMessage>) -> ActorResult {
        Ok(())
    }
}

struct SizedMessage(Vec<u8>);

fn sized_message_size(message: &SizedMessage) -> usize {
    message.0.len()
}

#[derive(Clone)]
struct SizedWorker;

impl RawActor for SizedWorker {
    type Msg = SizedMessage;

    async fn run(&mut self, _: ActorContext<SizedMessage>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct OptionsGraph {
    #[supervision(options = ActorOptions::new().mailbox(MailboxMode::conflate()))]
    mailbox_only: MailboxWorker,
    #[supervision(options = ActorOptions::new().message_size(sized_message_size))]
    message_size_only: SizedWorker,
    #[supervision(options = ActorOptions::new()
        .mailbox(MailboxMode::conflate())
        .message_size(sized_message_size))]
    combined: SizedWorker,
}

fn main() {
    OptionsGraph::tree(|_| OptionsGraphFactories {
        mailbox_only: || MailboxWorker,
        message_size_only: || SizedWorker,
        combined: || SizedWorker,
    })
    .expect("options tree builds");
}
