use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{
    OrderedTree, Restart, Strategy,
    host::{BoxError, ChildSpec},
};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decode_attempts = Arc::new(AtomicUsize::new(0));

    let fetch = {
        ChildSpec::task("fetch", move |ctx| async move {
            println!("fetch started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => return Ok(()),
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            }
        })
        .restart(Restart::always())
    };

    let decode = {
        let decode_attempts = Arc::clone(&decode_attempts);
        ChildSpec::task("decode", move |ctx| {
            let decode_attempts = Arc::clone(&decode_attempts);
            async move {
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
    };

    let sink = {
        ChildSpec::task("sink", move |ctx| async move {
            println!("sink started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => return Ok(()),
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            }
        })
        .restart(Restart::always())
    };

    let running_owner = OrderedTree::new()
        .strategy(Strategy::OneForAll)
        .task(fetch)
        .task(decode)
        .task(sink)
        .spawn()?;
    let running = running_owner.scope();
    let mut snapshots = running.subscribe_snapshots();
    let restarted = timeout(
        Duration::from_secs(2),
        snapshots.wait_for(|snapshot| {
            ["fetch", "decode", "sink"].iter().all(|id| {
                snapshot
                    .child(id)
                    .is_some_and(|child| child.generation > 0 && child.state.is_running())
            })
        }),
    )
    .await??;
    let restarted_stage_names: Vec<_> = restarted
        .children
        .iter()
        .filter(|child| child.generation > 0)
        .map(|child| child.id.as_str())
        .collect();
    println!("all pipeline stages restarted together: {restarted_stage_names:?}");
    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}
