use kokage::{CancellationToken, OrderedTree, TaskSpec};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("http-server", |ctx| async move {
        println!("http-server started");

        loop {
            tokio::select! {
                _ = ctx.shutdown_token().cancelled() => {
                    println!("http-server received cancellation");
                    return Ok(());
                }
                _ = sleep(Duration::from_millis(75)) => {
                    println!("http-server heartbeat");
                }
            }
        }
    }));
    let running = tree.spawn()?;

    let app_shutdown = CancellationToken::new();
    app_shutdown.cancel_when(async {
        sleep(Duration::from_millis(250)).await;
        println!("application shutdown requested");
    });

    app_shutdown.cancelled().await;
    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
