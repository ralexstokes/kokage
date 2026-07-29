use kokage_supervisor::prelude::*;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("http-server", |ctx| async move {
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
        }))
        .build()?;

    let handle = supervisor.spawn();

    let app_shutdown = CancellationToken::new();
    let trigger = tokio::spawn({
        let app_shutdown = app_shutdown.clone();
        async move {
            sleep(Duration::from_millis(250)).await;
            println!("application shutdown requested");
            app_shutdown.cancel();
        }
    });

    app_shutdown.cancelled().await;
    handle.shutdown();

    handle.wait().await?;
    println!("supervisor stopped");

    trigger.await?;
    Ok(())
}
use kokage_tokio::TokioSupervisorExt as _;
