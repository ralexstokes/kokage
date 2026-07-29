use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kokage_supervisor::prelude::*;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel::<(&'static str, u64)>();
    let decode_attempts = Arc::new(AtomicUsize::new(0));

    let fetch = {
        let starts_tx = starts_tx.clone();
        ChildSpec::task("fetch", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send(("fetch", ctx.generation()))
                    .expect("example receiver dropped");
                println!("fetch started in generation {}", ctx.generation());

                loop {
                    tokio::select! {
                        _ = ctx.shutdown_token().cancelled() => return Ok(()),
                        _ = sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        })
        .restart(RestartPolicy::Always)
    };

    let decode = {
        let starts_tx = starts_tx.clone();
        let decode_attempts = Arc::clone(&decode_attempts);
        ChildSpec::task("decode", move |ctx| {
            let starts_tx = starts_tx.clone();
            let decode_attempts = Arc::clone(&decode_attempts);
            async move {
                starts_tx
                    .send(("decode", ctx.generation()))
                    .expect("example receiver dropped");
                println!("decode started in generation {}", ctx.generation());

                if decode_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    sleep(Duration::from_millis(100)).await;
                    println!("decode failed in generation {}", ctx.generation());
                    return Err(example_error("corrupt frame"));
                }

                loop {
                    tokio::select! {
                        _ = ctx.shutdown_token().cancelled() => return Ok(()),
                        _ = sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        })
        .restart(RestartPolicy::OnFailure)
    };

    let sink = {
        let starts_tx = starts_tx.clone();
        ChildSpec::task("sink", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send(("sink", ctx.generation()))
                    .expect("example receiver dropped");
                println!("sink started in generation {}", ctx.generation());

                loop {
                    tokio::select! {
                        _ = ctx.shutdown_token().cancelled() => return Ok(()),
                        _ = sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        })
        .restart(RestartPolicy::Always)
    };

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(fetch)
        .child(decode)
        .child(sink)
        .build()?;

    let handle = supervisor.spawn();
    let mut restarted_stage_names = BTreeSet::new();

    while restarted_stage_names.len() < 3 {
        let maybe_start = timeout(Duration::from_secs(2), starts_rx.recv()).await?;
        let (stage, generation) = maybe_start
            .ok_or_else(|| std::io::Error::other("example start channel closed unexpectedly"))?;
        println!("observed {stage} start in generation {generation}");

        if generation == 1 {
            restarted_stage_names.insert(stage);
        }
    }

    println!("all pipeline stages restarted together: {restarted_stage_names:?}");
    handle.shutdown();
    handle.wait().await?;
    println!("supervisor stopped");

    Ok(())
}
use kokage_tokio::TokioSupervisorExt as _;
