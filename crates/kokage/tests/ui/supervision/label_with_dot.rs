use kokage::{Actor, ActorResult, MessageContext, Supervision};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct App {
    #[supervision(label = "workers.manager")]
    manager: Worker,
}

fn main() {}
