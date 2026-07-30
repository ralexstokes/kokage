use kokage::{DynamicTree, host::ChildSpec};
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running_owner = DynamicTree::new().spawn()?;
    let running = running_owner.scope();
    let mut snapshots = running.subscribe_snapshots();

    running
        .add_child(ChildSpec::task("api", |ctx| async move {
            println!("api started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("api shutting down");
            Ok(())
        }))
        .await?;

    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("api", |child| child.state.is_running()),
    )
    .await??;

    running
        .add_child(ChildSpec::task("cache-warmer", |ctx| async move {
            println!("cache-warmer started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => {
                        println!("cache-warmer received removal/shutdown");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_millis(50)) => {
                        println!("cache-warmer tick");
                    }
                }
            }
        }))
        .await?;

    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("cache-warmer", |child| child.state.is_running()),
    )
    .await??;
    println!("cache-warmer added at runtime");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    running.remove_child("cache-warmer").await?;
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for(|snapshot| snapshot.child("cache-warmer").is_none()),
    )
    .await??;
    println!("cache-warmer removed at runtime");

    let nested = running.add_subtree("nested", DynamicTree::new()).await?;
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("nested", |child| child.state.is_running()),
    )
    .await??;
    let mut nested_snapshots = nested.subscribe_snapshots();
    nested
        .add_child(ChildSpec::task("seed", |ctx| async move {
            println!("nested seed started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("nested seed shutting down");
            Ok(())
        }))
        .await?;
    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for_child("seed", |child| child.state.is_running()),
    )
    .await??;
    println!("nested supervisor added at runtime");

    nested
        .add_child(ChildSpec::task("nested-cache", |ctx| async move {
            println!("nested-cache started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => {
                        println!("nested-cache received removal/shutdown");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_millis(50)) => {
                        println!("nested-cache tick");
                    }
                }
            }
        }))
        .await?;

    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for_child("nested-cache", |child| child.state.is_running()),
    )
    .await??;
    println!("nested-cache added inside nested supervisor");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    nested.remove_child("nested-cache").await?;
    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for(|snapshot| snapshot.child("nested-cache").is_none()),
    )
    .await??;
    println!("nested-cache removed from nested supervisor");

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
