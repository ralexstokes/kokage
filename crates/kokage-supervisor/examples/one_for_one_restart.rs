use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::{ChildLifecycleEventKind, prelude::*};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicUsize::new(0));

    let flaky = ChildSpec::task("flaky-worker", move |ctx| {
        let attempts = Arc::clone(&attempts);
        async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            println!("flaky-worker started in generation {}", ctx.generation());

            if attempt == 0 {
                sleep(Duration::from_millis(100)).await;
                println!("flaky-worker failed in generation {}", ctx.generation());
                return Err(example_error("simulated startup failure"));
            }

            ctx.shutdown_token().cancelled().await;
            println!("flaky-worker observed shutdown");
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    });

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForOne)
        .child(flaky)
        .child(metrics)
        .build()?;

    let handle = supervisor.spawn();
    let mut lifecycle = handle.watch_lifecycle();

    loop {
        let event = timeout(Duration::from_secs(2), lifecycle.next())
            .await?
            .ok_or_else(|| std::io::Error::other("lifecycle stream closed"))?;
        println!("event: {event:?}");

        if event.child_id == "flaky-worker"
            && let ChildLifecycleEventKind::Started { generation: 1 } = event.kind
        {
            println!("child flaky-worker restarted into generation 1");
            break;
        }
    }

    handle.shutdown();
    handle.wait().await?;
    println!("supervisor stopped");

    Ok(())
}
use kokage_tokio::TokioSupervisorExt as _;
