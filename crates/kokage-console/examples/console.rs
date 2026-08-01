//! Demo workload for the web console.
//!
//! Builds a supervision tree with nested supervisors and keeps it busy so the
//! console has something to show:
//!
//! ```text
//! root (OneForOne)
//! ├── frontend            actor, forwards jobs to the worker
//! ├── worker              actor, fails every 6th job (backoff 1.5s)
//! ├── burst               dynamic actor, added and removed on a cycle
//! ├── pipeline (OneForAll)
//! │   ├── source          runs until shutdown
//! │   └── transform       fails every 9s (backoff 2s), bouncing its sibling
//! └── telemetry (OneForOne)
//!     ├── heartbeat       completes cleanly every 7s, RestartPolicy::always()
//!     ├── schema-migration completes once, RestartPolicy::never()
//!     └── exporters (OneForOne)
//!         └── stdout-exporter  runs until shutdown
//! ```
//!
//! Run with `cargo run -p kokage-console --example console`,
//! open the printed URL, and stop with Ctrl-C.

use std::{error::Error, io, time::Duration};

use kokage::{
    Actor, ActorRef, ActorSpec, Backoff, BoxError, Context, DynamicTree, ExitResult, RestartPolicy,
    Strategy, TaskSpec, Tree,
};
use kokage_console::{ConsoleBuilder, ConsoleError};
use tokio::time::sleep;

fn example_error(message: &'static str) -> BoxError {
    Box::new(io::Error::other(message))
}

#[derive(Clone)]
struct Frontend {
    worker: ActorRef<String>,
}

impl Actor for Frontend {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.worker.send(message).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Worker;

impl Actor for Worker {
    type Msg = String;

    async fn handle(&mut self, message: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if message.contains("fail") {
            println!("worker failing on `{message}`");
            return Err::<_, BoxError>(example_error("simulated worker failure"));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Burst;

impl Actor for Burst {
    type Msg = u32;

    async fn handle(&mut self, _message: u32, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

/// A OneForAll group: when `transform` fails, `source` is stopped and
/// restarted along with it.
fn pipeline_runtime() -> Tree {
    let transform_restart = RestartPolicy::on_failure()
        .limit(60, Duration::from_secs(60))
        .backoff(Backoff::fixed(Duration::from_secs(2)));
    let source = TaskSpec::new("source", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });

    let transform = TaskSpec::new("transform", |ctx| async move {
        tokio::select! {
            _ = ctx.shutdown_token().cancelled() => Ok(()),
            _ = sleep(Duration::from_secs(9)) => {
                println!("transform failing (generation {})", ctx.generation());
                Err(example_error("pipeline transform wedged"))
            }
        }
    })
    .restart(transform_restart);

    let mut tree = Tree::new()
        .strategy(Strategy::OneForAll)
        .default_child_restart(RestartPolicy::on_failure().limit(60, Duration::from_secs(60)));
    tree.add_task_spec(source);
    tree.add_task_spec(transform);
    tree
}

/// A OneForOne group demonstrating the other restart policies, with a further
/// nested supervisor one level deeper.
fn telemetry_runtime() -> Tree {
    // `Always` restarts even after a clean exit, so this child completes and
    // comes back every 7 seconds.
    let heartbeat = TaskSpec::new("heartbeat", |ctx| async move {
        tokio::select! {
            _ = ctx.shutdown_token().cancelled() => {}
            _ = sleep(Duration::from_secs(7)) => {}
        }
        Ok(())
    });

    // `Never` runs at most once: this child completes after 3 seconds and
    // stays Stopped for the rest of the run.
    let migration = TaskSpec::new("schema-migration", |_ctx| async move {
        sleep(Duration::from_secs(3)).await;
        Ok(())
    });

    let exporter = TaskSpec::new("stdout-exporter", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let mut exporters = Tree::new();
    exporters.add_task_spec(exporter);

    let mut tree = Tree::new()
        .default_child_restart(RestartPolicy::on_failure().limit(60, Duration::from_secs(60)));
    tree.add_task_spec(
        heartbeat.restart(RestartPolicy::always().limit(60, Duration::from_secs(60))),
    );
    tree.add_task_spec(migration.restart(RestartPolicy::never()));
    tree.add_subtree("exporters", exporters);
    tree
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let worker_restart = RestartPolicy::on_failure()
        .limit(60, Duration::from_secs(60))
        .backoff(Backoff::fixed(Duration::from_millis(1500)));
    let worker_spec = ActorSpec::new("worker", || Worker).restart(worker_restart);
    let worker = worker_spec.actor_ref();
    let frontend_spec = ActorSpec::new("frontend", {
        let worker = worker.clone();
        move || Frontend {
            worker: worker.clone(),
        }
    });
    let frontend = frontend_spec.actor_ref();

    let mut tree = Tree::new()
        .default_child_restart(RestartPolicy::on_failure().limit(60, Duration::from_secs(60)));
    tree.add_actor_spec(frontend_spec);
    tree.add_actor_spec(worker_spec);
    tree.add_subtree("pipeline", pipeline_runtime());
    tree.add_subtree("telemetry", telemetry_runtime());
    tree.add_subtree("dynamic", DynamicTree::new());
    let running_tree = tree.spawn()?;

    let console = ConsoleBuilder::for_runtime(&running_tree.scope())
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await;
    let console = match console {
        Ok(console) => {
            println!("console available at http://{}", console.local_addr());
            Some(console)
        }
        Err(ConsoleError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied => {
            println!("console bind skipped: {error}");
            None
        }
        Err(error) => return Err(error.into()),
    };

    // Periodically add a dynamic actor, feed it, and remove it again, so the
    // console shows children joining and leaving the tree.
    let dynamic = running_tree
        .scope()
        .subtree("dynamic")
        .and_then(|scope| scope.dynamic())
        .expect("declared dynamic scope");
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(12)).await;
            let Ok(burst) = dynamic.add_actor("burst", || Burst).await else {
                break;
            };
            for index in 0..40u32 {
                if burst.send(index).await.is_err() {
                    break;
                }
                sleep(Duration::from_millis(150)).await;
            }
            sleep(Duration::from_secs(4)).await;
            if dynamic.remove_named("burst").await.is_err() {
                break;
            }
        }
    });

    println!("press Ctrl-C to stop");

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let mut message_index = 0usize;
    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = sleep(Duration::from_millis(800)) => {
                message_index += 1;
                let payload = if message_index.is_multiple_of(6) {
                    format!("fail-worker-{message_index}")
                } else {
                    format!("job-{message_index}")
                };
                frontend.send(payload).await?;
            }
        }
    }

    println!("shutting down");
    running_tree.shutdown().await?;
    if let Some(console) = console {
        console.shutdown();
    }
    Ok(())
}
