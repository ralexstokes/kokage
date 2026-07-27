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
    let mut events = handle.watch_lifecycle_recursive();

    let observer = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            let stopped = matches!(event.kind, RecursiveLifecycleEventKind::SupervisorStopped);
            print_event(&event);
            if stopped {
                break;
            }
        }
    });

    sleep(Duration::from_millis(200)).await;
    handle.shutdown();

    handle.wait().await?;
    observer.await?;

    Ok(())
}

fn print_event(event: &RecursiveLifecycleEvent) {
    let path = event
        .supervisor_path
        .iter()
        .map(|segment| {
            format!(
                "{}[lineage={}, generation={}]",
                segment.id, segment.lineage, segment.generation
            )
        })
        .collect::<Vec<_>>()
        .join("/");
    let scope = if path.is_empty() { "root" } else { &path };

    match &event.kind {
        RecursiveLifecycleEventKind::SupervisorStarted => {
            println!("{scope}: supervisor started");
        }
        RecursiveLifecycleEventKind::SupervisorStopping => {
            println!("{scope}: supervisor stopping");
        }
        RecursiveLifecycleEventKind::SupervisorStopped => {
            println!("{scope}: supervisor stopped");
        }
        RecursiveLifecycleEventKind::Child(child) => match &child.kind {
            LifecycleEventKind::Added => println!("{scope}: child added: {}", child.child_id),
            LifecycleEventKind::Started { generation } => println!(
                "{scope}: child started: {} generation={generation}",
                child.child_id
            ),
            LifecycleEventKind::Exited {
                generation, reason, ..
            } => println!(
                "{scope}: child exited: {} generation={generation} reason={reason:?}",
                child.child_id
            ),
            LifecycleEventKind::Removed => {
                println!("{scope}: child removed: {}", child.child_id);
            }
            LifecycleEventKind::Lagged { dropped } => {
                println!("{scope}: direct lifecycle dropped {dropped} transitions");
            }
            _ => println!("{scope}: unknown child lifecycle event"),
        },
        RecursiveLifecycleEventKind::RestartScheduled {
            child_id,
            generation,
            delay,
            ..
        } => {
            println!(
                "{scope}: child restart scheduled: {child_id} generation={generation} delay={delay:?}"
            );
        }
        RecursiveLifecycleEventKind::RestartIntensityExceeded { total_restarts, .. } => {
            println!("{scope}: restart intensity exceeded after {total_restarts} restarts");
        }
        RecursiveLifecycleEventKind::Lagged { dropped, .. } => {
            println!("{scope}: recursive lifecycle dropped {dropped} events; resync snapshots");
        }
        _ => println!("{scope}: unknown recursive lifecycle event"),
    }
}
