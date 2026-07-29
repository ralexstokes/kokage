use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::{LifecycleEventKind, prelude::*};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nested_attempts = Arc::new(AtomicUsize::new(0));

    let nested_worker = {
        let nested_attempts = Arc::clone(&nested_attempts);
        ChildSpec::task("nested-worker", move |ctx| {
            let nested_attempts = Arc::clone(&nested_attempts);
            async move {
                println!("nested-worker started in generation {}", ctx.generation());

                if nested_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    sleep(Duration::from_millis(100)).await;
                    println!("nested-worker failed");
                    return Err(example_error("simulated nested failure"));
                }

                ctx.shutdown_token().cancelled().await;
                println!("nested-worker observed shutdown");
                Ok(())
            }
        })
    };

    let nested_supervisor = Supervisor::ordered().child(nested_worker).build()?;

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    })
    .restart(RestartPolicy::Always);

    let running_owner = Supervisor::dynamic().spawn()?;
    let running = running_owner.handle();
    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(metrics)
        .await?;
    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::supervisor("nested-pipeline", nested_supervisor))
        .await?;
    let nested_handle = running
        .supervisor("nested-pipeline")
        .expect("newly added nested supervisor has a stable handle");
    let mut nested_lifecycle = nested_handle.watch_lifecycle();

    loop {
        let event = timeout(Duration::from_secs(2), nested_lifecycle.next())
            .await?
            .ok_or_else(|| std::io::Error::other("nested lifecycle stream closed"))?;
        println!("nested event: {event:?}");
        if matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, generation: 1, .. }
                if child_id == "nested-worker"
        ) {
            break;
        }
    }

    let metrics = running
        .snapshot()
        .child("metrics")
        .expect("metrics remains present")
        .clone();
    assert_eq!(metrics.generation, 0);
    println!("nested subtree recovered internally without restarting outer siblings");

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
