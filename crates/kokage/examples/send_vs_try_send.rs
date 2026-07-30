use std::{error::Error, future::pending, sync::Arc, time::Duration};

use kokage::{
    ActorSpec, ExitResult, Restart, SendTimeoutError, Shutdown, TrySendError,
    host::{DEFAULT_SHUTDOWN_BOUND, RawActor, RawContext},
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{self, UnboundedSender},
    },
    time::{sleep, timeout},
};

#[derive(Clone)]
struct OneMessageSink {
    observed: UnboundedSender<String>,
}

impl RawActor for OneMessageSink {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
        if let Some(message) = ctx.recv().await {
            self.observed.send(message).expect("receiver alive");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let spec = ActorSpec::new("OneMessageSink", move || OneMessageSink {
        observed: observed_tx.clone(),
    });
    let sink_ref = spec.actor_ref();
    let sink = spec.into_runnable();

    let first_run = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.run_until(
                pending::<()>(),
                Restart::always(),
                Shutdown::drain_for(DEFAULT_SHUTDOWN_BOUND),
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
        Err(TrySendError::NotRunning { actor_id, .. }) => {
            println!("try_send failed fast while `{actor_id}` was between runs");
        }
        other => println!("unexpected try_send result: {other:?}"),
    }

    let recovered = match sink_ref
        .send_timeout("second run".to_owned(), Duration::from_millis(50))
        .await
    {
        Err(SendTimeoutError::Timeout {
            actor_id, message, ..
        }) => {
            println!("bounded send to `{actor_id}` timed out; retrying `{message}`");
            message
        }
        other => panic!("unexpected bounded send result: {other:?}"),
    };

    let send_result = Arc::new(Mutex::new(None));
    let send_task = tokio::spawn({
        let sink_ref = sink_ref.clone();
        let send_result = Arc::clone(&send_result);
        async move {
            let result = sink_ref.send(recovered).await;
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
                Restart::always(),
                Shutdown::drain_for(DEFAULT_SHUTDOWN_BOUND),
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
