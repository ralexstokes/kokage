use tokio::time::{Duration, sleep};
use tokio_supervisor::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            println!("worker started");
            ctx.shutdown_token().cancelled().await;
            println!("worker shutting down");
            Ok(())
        }))
        .build()?;

    let handle = supervisor.spawn();
    // Events are lossy observability: a slow subscriber sees
    // `RecvError::Lagged` and skips missed events. Control logic (e.g. a
    // restart breaker) should use `watch_lifecycle()` or snapshots instead.
    let mut events = handle.subscribe();

    let observer = tokio::spawn(async move {
        loop {
            let event = events.recv().await?;
            if matches!(event, SupervisorEvent::SupervisorStopped) {
                print_event(&event, 0);
                break;
            }

            print_event(&event, 0);
        }

        Ok::<(), tokio::sync::broadcast::error::RecvError>(())
    });

    sleep(Duration::from_millis(200)).await;
    handle.shutdown();

    handle.wait().await?;
    observer.await??;

    Ok(())
}

fn print_event(event: &SupervisorEvent, depth: usize) {
    let indent = "  ".repeat(depth);

    match event {
        SupervisorEvent::SupervisorStarted => println!("{indent}supervisor started"),
        SupervisorEvent::SupervisorStopping => println!("{indent}supervisor stopping"),
        SupervisorEvent::SupervisorStopped => {
            println!("{indent}supervisor stopped");
        }
        SupervisorEvent::Nested {
            id,
            generation,
            event,
            ..
        } => {
            println!("{indent}nested supervisor child: {id} generation={generation}");
            print_event(event, depth + 1);
        }
        SupervisorEvent::ChildStarted { id, generation, .. } => {
            println!("{indent}child started: {id} generation={generation}");
        }
        SupervisorEvent::ChildRemoved { id, .. } => {
            println!("{indent}child removed: {id}");
        }
        SupervisorEvent::ChildExited {
            id,
            generation,
            status,
            ..
        } => match status {
            ExitStatusView::Completed => {
                println!("{indent}child exited cleanly: {id} generation={generation}");
            }
            ExitStatusView::Failed(err) => {
                println!("{indent}child failed: {id} generation={generation} error={err}");
            }
            ExitStatusView::Panicked => {
                println!("{indent}child panicked: {id} generation={generation}");
            }
            ExitStatusView::Aborted => {
                println!("{indent}child aborted: {id} generation={generation}");
            }
            _ => println!("{indent}child exited: {id} generation={generation}"),
        },
        SupervisorEvent::AutoShutdownTriggered { id, mode, .. } => {
            println!("{indent}automatic shutdown triggered by {id}: {mode:?}");
        }
        SupervisorEvent::ChildRestartScheduled {
            id,
            generation,
            delay,
            ..
        } => {
            println!(
                "{indent}child restart scheduled: {id} generation={generation} delay={delay:?}"
            );
        }
        SupervisorEvent::ChildRestarted {
            id,
            old_generation,
            new_generation,
            ..
        } => {
            println!("{indent}child restarted: {id} {old_generation}->{new_generation}");
        }
        SupervisorEvent::RestartIntensityExceeded => {
            println!("{indent}restart intensity exceeded");
        }
        _ => println!("{indent}unknown supervisor event"),
    }
}
