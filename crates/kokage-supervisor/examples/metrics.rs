#[cfg(feature = "metrics")]
use kokage_supervisor::prelude::*;
#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::PrometheusBuilder;
#[cfg(feature = "metrics")]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(feature = "metrics")]
use tokio::time::{Duration, sleep};

#[cfg(feature = "metrics")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let recorder = PrometheusBuilder::new().install_recorder()?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("flaky", move |ctx| {
                let attempts = Arc::clone(&attempts);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(std::io::Error::other("boom").into());
                    }

                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()?;

    let handle = supervisor.spawn();

    sleep(Duration::from_millis(100)).await;
    handle.shutdown();

    handle.wait().await?;

    println!("# Prometheus snapshot");
    println!("{}", recorder.render());

    Ok(())
}

#[cfg(not(feature = "metrics"))]
fn main() {
    eprintln!("run this example with: cargo run --example metrics --features metrics");
}
