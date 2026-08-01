use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage::{BoxError, RestartPolicy, Strategy, TaskSpec, Tree};
use tokio::time::{Duration, sleep, timeout};

fn example_error(message: &'static str) -> BoxError {
    Box::new(std::io::Error::other(message))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decode_attempts = Arc::new(AtomicUsize::new(0));

    let fetch = {
        TaskSpec::new("fetch", move |ctx| async move {
            println!("fetch started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => return Ok(()),
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            }
        })
        .restart(RestartPolicy::always())
    };

    let decode = {
        let decode_attempts = Arc::clone(&decode_attempts);
        TaskSpec::new("decode", move |ctx| {
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
        TaskSpec::new("sink", move |ctx| async move {
            println!("sink started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => return Ok(()),
                    _ = sleep(Duration::from_millis(50)) => {}
                }
            }
        })
        .restart(RestartPolicy::always())
    };

    let mut tree = Tree::new().strategy(Strategy::OneForAll);
    tree.add_task_spec(fetch);
    tree.add_task_spec(decode);
    tree.add_task_spec(sink);
    let running_tree = tree.spawn()?;
    let scope = running_tree.scope();
    let mut snapshots = scope.subscribe_snapshots();
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
    scope.shutdown().await?;
    println!("supervisor stopped");

    Ok(())
}
