use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::{BackoffPolicy, prelude::*};
use tokio::time::{Duration, sleep};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut nested_restart = RestartConfig::new(5, Duration::from_secs(5));
    nested_restart.backoff = BackoffPolicy::Fixed(Duration::from_millis(100));
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .compact()
        .init();

    let attempts = Arc::new(AtomicUsize::new(0));
    let nested_attempts = Arc::clone(&attempts);
    let nested = Supervisor::ordered()
        .child(
            ChildSpec::task("leaf", move |ctx| {
                let attempts = Arc::clone(&nested_attempts);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(std::io::Error::other("fail once").into());
                    }

                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_config(nested_restart),
        )
        .build()?;

    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("anchor", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .child(ChildSpec::supervisor("nested", nested))
        .build()?;

    let handle = supervisor.spawn();

    sleep(Duration::from_millis(300)).await;
    handle.shutdown();

    handle.wait().await?;

    Ok(())
}
