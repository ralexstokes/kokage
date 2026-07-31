use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{Backoff, RestartPolicy, TaskSpec, Tree};
use tokio::time::{Duration, sleep};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nested_restart = RestartPolicy::on_failure()
        .limit(5, Duration::from_secs(5))
        .backoff(Backoff::fixed(Duration::from_millis(100)));
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .compact()
        .init();

    let attempts = Arc::new(AtomicUsize::new(0));
    let nested_attempts = Arc::clone(&attempts);
    let mut nested = Tree::new();
    nested.add_task_spec(
        TaskSpec::new("leaf", move |ctx| {
            let attempts = Arc::clone(&nested_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(std::io::Error::other("fail once").into());
                }

                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })
        .restart(nested_restart),
    );

    let mut tree = Tree::new();
    tree.add_task("anchor", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    tree.add_subtree("nested", nested);
    let running = tree.spawn()?;

    sleep(Duration::from_millis(300)).await;
    running.shutdown().await?;

    Ok(())
}
