use std::{error::Error, future::pending, sync::Arc, time::Duration};

use tokio::{
    sync::{
        Mutex,
        mpsc::{self, UnboundedSender},
    },
    time::{sleep, timeout},
};
use tokio_otp::{
    ActorContext, ActorResult, GraphBuilder, RawActor, RestartPolicy, SendError, prelude::Continue,
};

#[derive(Clone)]
struct OneMessageSink {
    observed: UnboundedSender<String>,
}

impl RawActor for OneMessageSink {
    type Msg = String;

    async fn run(&mut self, mut ctx: ActorContext<String>) -> ActorResult {
        if let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let sink_ref = builder.add(move || OneMessageSink {
        observed: observed_tx.clone(),
    });
    let graph = builder.build()?;
    let sink = graph.actors()[0].clone();

    let first_run = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.run_until(
                pending::<()>(),
                RestartPolicy::Always,
                Duration::from_secs(5),
            )
            .await
        }
    });
    sink_ref.send("first run".to_owned()).await?;
    println!(
        "sink observed `{}`",
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await?
            .expect("first message observed")
    );
    first_run.await??;

    match sink_ref.try_send("try during restart".to_owned()) {
        Err(SendError::ActorNotRunning { actor_id, .. }) => {
            println!("try_send failed fast while `{actor_id}` was between runs");
        }
        other => println!("unexpected try_send result: {other:?}"),
    }

    let send_result = Arc::new(Mutex::new(None));
    let send_task = tokio::spawn({
        let sink_ref = sink_ref.clone();
        let send_result = Arc::clone(&send_result);
        async move {
            let result = sink_ref.send("second run".to_owned()).await;
            *send_result.lock().await = Some(result);
        }
    });
    sleep(Duration::from_millis(50)).await;
    assert!(send_result.lock().await.is_none());
    println!("send is waiting for the next binding");

    let second_run = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.run_until(
                pending::<()>(),
                RestartPolicy::Always,
                Duration::from_secs(5),
            )
            .await
        }
    });
    println!(
        "sink observed `{}`",
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await?
            .expect("second message observed")
    );
    send_task.await?;
    send_result
        .lock()
        .await
        .take()
        .expect("send task recorded result")?;
    second_run.await??;

    Ok(())
}
