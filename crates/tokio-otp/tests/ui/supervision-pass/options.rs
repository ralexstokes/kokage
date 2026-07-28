use tokio_otp::{
    ActorContext, ActorOptions, ActorResult, MailboxMode, MessageSize, RawActor, Supervision,
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

impl MessageSize for SizedMessage {
    fn size_hint(&self) -> usize {
        self.0.len()
    }
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
    #[supervision(options = ActorOptions::new().message_size())]
    message_size_only: SizedWorker,
    #[supervision(options = ActorOptions::new()
        .mailbox(MailboxMode::conflate())
        .message_size())]
    combined: SizedWorker,
}

fn main() {
    OptionsGraph::graph(|_| OptionsGraphFactories {
        mailbox_only: || MailboxWorker,
        message_size_only: || SizedWorker,
        combined: || SizedWorker,
    })
    .expect("options graph builds");
}
