use tokio_otp::{ActorContext, ActorOptions, ActorResult, RawActor, Supervision};

struct Message;

#[derive(Clone)]
struct Worker;

impl RawActor for Worker {
    type Msg = Message;

    async fn run(&mut self, _: ActorContext<Message>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct OptionsGraph {
    #[supervision(options = ActorOptions::new().message_size())]
    worker: Worker,
}

fn main() {}
