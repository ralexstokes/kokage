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
            let stopped = matches!(event, LifecycleEvent::SupervisorStopped { .. });
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

fn print_event(event: &LifecycleEvent) {
    let path = event
        .supervisor_path()
        .unwrap_or_default()
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

    match event {
        LifecycleEvent::SupervisorStarted { .. } => {
            println!("{scope}: supervisor started");
        }
        LifecycleEvent::SupervisorStopping { .. } => {
            println!("{scope}: supervisor stopping");
        }
        LifecycleEvent::SupervisorStopped { .. } => {
            println!("{scope}: supervisor stopped");
        }
        LifecycleEvent::Added { child_id, .. } => println!("{scope}: child added: {child_id}"),
        LifecycleEvent::Started {
            child_id,
            generation,
            ..
        } => println!("{scope}: child started: {child_id} generation={generation}"),
        LifecycleEvent::Exited {
            child_id,
            generation,
            reason,
            ..
        } => {
            println!("{scope}: child exited: {child_id} generation={generation} reason={reason:?}")
        }
        LifecycleEvent::Removed { child_id, .. } => {
            println!("{scope}: child removed: {child_id}");
        }
        LifecycleEvent::RestartScheduled {
            child_id,
            generation,
            delay,
            ..
        } => {
            println!(
                "{scope}: child restart scheduled: {child_id} generation={generation} delay={delay:?}"
            );
        }
        LifecycleEvent::RestartIntensityExceeded { total_restarts, .. } => {
            println!("{scope}: restart intensity exceeded after {total_restarts} restarts");
        }
        LifecycleEvent::Lagged { dropped } => {
            println!("{scope}: recursive lifecycle dropped {dropped} events; resync snapshots");
        }
        _ => println!("{scope}: unknown lifecycle event"),
    }
}
