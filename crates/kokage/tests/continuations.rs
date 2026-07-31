use std::sync::Arc;

use kokage::{Actor, Context, ExitResult, Tree};
use tokio::sync::{Notify, mpsc};

enum Msg {
    Start,
    Continue(u8),
    External,
}

struct FairContinuation {
    observed: mpsc::UnboundedSender<&'static str>,
    first_continuation: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for FairContinuation {
    type Msg = Msg;

    async fn handle(&mut self, message: Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            Msg::Start => ctx.continue_with(Msg::Continue(2)),
            Msg::Continue(remaining) => {
                self.observed
                    .send("continuation")
                    .expect("receiver remains live");
                if remaining == 2 {
                    self.first_continuation.notify_one();
                    self.release.notified().await;
                }
                if remaining > 0 {
                    ctx.continue_with(Msg::Continue(remaining - 1));
                }
            }
            Msg::External => {
                self.observed
                    .send("external")
                    .expect("receiver remains live");
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_continuation_chain_gives_ready_mailbox_input_a_turn() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let first_continuation = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut tree = Tree::new();
    let actor = tree.add_actor("worker", {
        let first_continuation = Arc::clone(&first_continuation);
        let release = Arc::clone(&release);
        move || FairContinuation {
            observed: observed_tx.clone(),
            first_continuation: Arc::clone(&first_continuation),
            release: Arc::clone(&release),
        }
    });
    let runtime = tree.spawn().expect("tree builds");

    actor.send(Msg::Start).await.expect("chain starts");
    first_continuation.notified().await;
    actor
        .send(Msg::External)
        .await
        .expect("external input is queued");
    release.notify_one();

    assert_eq!(observed_rx.recv().await, Some("continuation"));
    assert_eq!(observed_rx.recv().await, Some("external"));
    assert_eq!(observed_rx.recv().await, Some("continuation"));

    runtime.shutdown().await.expect("tree stops");
}
