use kokage::DynamicTree;
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running_tree = DynamicTree::new().spawn()?;
    let scope = running_tree.scope();
    let mut snapshots = scope.snapshots();

    let cache_warmer = scope
        .add_task("api", |ctx| async move {
            println!("api started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("api shutting down");
            Ok(())
        })
        .await?;

    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("api", |child| child.state.is_running()),
    )
    .await??;

    scope
        .add_task("cache-warmer", |ctx| async move {
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
        })
        .await?;

    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("cache-warmer", |child| child.state.is_running()),
    )
    .await??;
    println!("cache-warmer added at runtime");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    scope.remove_task(&cache_warmer).await?;
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for(|snapshot| snapshot.child("cache-warmer").is_none()),
    )
    .await??;
    println!("cache-warmer removed at runtime");

    let nested = scope
        .add_dynamic_subtree("nested", DynamicTree::new())
        .await?;
    timeout(
        Duration::from_secs(2),
        snapshots.wait_for_child("nested", |child| child.state.is_running()),
    )
    .await??;
    let mut nested_snapshots = nested.snapshots();
    let nested_cache = nested
        .add_task("seed", |ctx| async move {
            println!("nested seed started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("nested seed shutting down");
            Ok(())
        })
        .await?;
    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for_child("seed", |child| child.state.is_running()),
    )
    .await??;
    println!("nested supervisor added at runtime");

    nested
        .add_task("nested-cache", |ctx| async move {
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
        })
        .await?;

    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for_child("nested-cache", |child| child.state.is_running()),
    )
    .await??;
    println!("nested-cache added inside nested supervisor");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    nested.remove_task(&nested_cache).await?;
    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for(|snapshot| snapshot.child("nested-cache").is_none()),
    )
    .await??;
    println!("nested-cache removed from nested supervisor");

    scope.shutdown().await?;
    println!("supervisor stopped");

    Ok(())
}
