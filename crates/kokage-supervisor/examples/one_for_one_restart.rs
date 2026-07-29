use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::prelude::*;
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
    });

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    });

    let running_owner = Supervisor::ordered().child(flaky).child(metrics).spawn()?;
    let running = running_owner.handle();
    let mut snapshots = running.subscribe_snapshots();
    let restarted = timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("flaky-worker", |child| {
            child.generation > 0 && child.state.is_running()
        }),
    )
    .await??;
    println!(
        "child flaky-worker restarted into generation {}",
        restarted.generation
    );

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
