use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{
    DynamicTree, OrderedTree, Restart,
    host::{BoxError, ChildSpec},
};
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

    let nested_tree = OrderedTree::new().task(nested_worker);

    let metrics = ChildSpec::task("metrics", |ctx| async move {
        println!("metrics started in generation {}", ctx.generation());
        ctx.shutdown_token().cancelled().await;
        println!("metrics observed shutdown");
        Ok(())
    })
    .restart(Restart::always());

    let running_owner = DynamicTree::new().spawn()?;
    let running = running_owner.handle();
    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(metrics)
        .await?;
    let nested_handle = running
        .dynamic()
        .expect("dynamic supervisor")
        .add_subtree("nested-pipeline", nested_tree)
        .await?;
    let mut nested_snapshots = nested_handle.subscribe_snapshots();
    timeout(
        Duration::from_secs(2),
        nested_snapshots.wait_for_child("nested-worker", |child| {
            child.generation > 0 && child.state.is_running()
        }),
    )
    .await??;

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
