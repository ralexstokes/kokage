use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{
    Backoff, OrderedTree, Restart,
    host::{BoxError, ChildSpec},
};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let warm_cache_restart = Restart::on_failure()
        .limit(1, Duration::from_secs(1))
        .backoff(Backoff::fixed(Duration::from_millis(100)));
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
    .restart(warm_cache_restart);

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    });

    // Supervisor default: children do not get any restart budget unless they override it.
    let running_owner = OrderedTree::new()
        .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1)))
        .task(warm_cache)
        .task(metrics)
        .spawn()?;
    let running = running_owner.scope();
    let mut snapshots = running.subscribe_snapshots();
    let scheduled = timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("warm-cache", |child| child.next_restart_in.is_some()),
    )
    .await??;
    println!(
        "warm-cache generation {} is allowed one delayed restart: {:?}",
        scheduled.generation, scheduled.next_restart_in
    );
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("warm-cache", |child| {
            child.generation > scheduled.generation && child.state.is_running()
        }),
    )
    .await??;

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
