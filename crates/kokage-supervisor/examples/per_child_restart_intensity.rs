use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::{
    BackoffPolicy, ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleEventKind, prelude::*,
};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let warm_cache_restart = RestartConfig::new(1, Duration::from_secs(1))
        .backoff(BackoffPolicy::Fixed(Duration::from_millis(100)));
    let warm_cache_attempts = Arc::new(AtomicUsize::new(0));

    // Intensity uses a sliding timestamp window. Backoff attempts are tracked
    // separately as consecutive restarts and reset only after an incarnation
    // runs longer than `within`.
    let warm_cache = ChildSpec::task("warm-cache", move |ctx| {
        let warm_cache_attempts = Arc::clone(&warm_cache_attempts);
        async move {
            let attempt = warm_cache_attempts.fetch_add(1, Ordering::SeqCst);
            println!(
                "warm-cache started in generation {} (attempt {})",
                ctx.generation(),
                attempt + 1
            );

            if attempt == 0 {
                sleep(Duration::from_millis(50)).await;
                println!("warm-cache failed during initial generation");
                return Err(example_error("cache priming failed"));
            }

            ctx.shutdown_token().cancelled().await;
            println!("warm-cache observed shutdown");
            Ok(())
        }
    })
    .restart_config(warm_cache_restart);

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    });

    // Supervisor default: children do not get any restart budget unless they override it.
    let running = Supervisor::ordered()
        .restart_config(RestartConfig::new(0, Duration::from_secs(1)))
        .child(warm_cache)
        .child(metrics)
        .spawn()?;
    let mut events = running.watch_lifecycle_recursive();

    loop {
        let event = timeout(Duration::from_secs(2), events.next())
            .await?
            .ok_or_else(|| std::io::Error::other("lifecycle stream closed"))?;
        println!("event: {event:?}");

        match event.kind {
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id,
                kind:
                    ChildLifecycleEventKind::RestartScheduled {
                        generation, delay, ..
                    },
                ..
            }) if child_id == "warm-cache" => {
                println!(
                    "warm-cache generation {} is allowed one delayed restart: {delay:?}",
                    generation
                );
            }
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id,
                kind: ChildLifecycleEventKind::Started { generation: 1 },
                ..
            }) if child_id == "warm-cache" => {
                break;
            }
            LifecycleEventKind::RestartIntensityExceeded { .. } => {
                return Err(std::io::Error::other(
                    "unexpected restart intensity failure in example",
                )
                .into());
            }
            _ => {}
        }
    }

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
