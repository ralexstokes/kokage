use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::time::{Duration, sleep, timeout};
use tokio_supervisor::prelude::*;

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nested_attempts = Arc::new(AtomicUsize::new(0));

    let nested_worker = {
        let nested_attempts = Arc::clone(&nested_attempts);
        ChildSpec::new("nested-worker", move |ctx| {
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
        .restart(RestartPolicy::OnFailure)
    };

    let nested_supervisor = SupervisorBuilder::new().child(nested_worker).build()?;

    let metrics = ChildSpec::new("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    })
    .restart(RestartPolicy::Always);

    let supervisor = DynamicSupervisorBuilder::new().build()?;

    let handle = supervisor.spawn();
    handle.add_child(metrics).await?;
    handle
        .add_supervisor(SupervisorSpec::new("nested-pipeline", nested_supervisor))
        .await?;
    let nested_handle = handle
        .supervisor("nested-pipeline")
        .expect("newly added nested supervisor has a stable handle");
    let mut nested_lifecycle = nested_handle.watch_lifecycle();

    loop {
        let event = timeout(Duration::from_secs(2), nested_lifecycle.next())
            .await?
            .ok_or_else(|| std::io::Error::other("nested lifecycle stream closed"))?;
        println!("nested event: {event:?}");
        if event.child_id == "nested-worker"
            && matches!(event.kind, LifecycleEventKind::Started { generation: 1 })
        {
            break;
        }
    }

    let metrics = handle
        .snapshot()
        .child("metrics")
        .expect("metrics remains present")
        .clone();
    assert_eq!(metrics.generation, 0);
    println!("nested subtree recovered internally without restarting outer siblings");

    handle.shutdown();
    handle.wait().await?;
    println!("supervisor stopped");

    Ok(())
}
