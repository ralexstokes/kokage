use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{BoxError, TaskSpec, Tree};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicUsize::new(0));

    let flaky = TaskSpec::new("flaky-worker", move |ctx| {
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

    let metrics = TaskSpec::new("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    });

    let mut tree = Tree::new();
    tree.add_task_spec(flaky);
    tree.add_task_spec(metrics);
    let running_owner = tree.spawn()?;
    let running = running_owner.scope();
    let mut snapshots = running.snapshots();
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

    running.shutdown().await?;
    println!("supervisor stopped");

    Ok(())
}
