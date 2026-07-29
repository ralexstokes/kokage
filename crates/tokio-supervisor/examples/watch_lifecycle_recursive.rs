use tokio::time::{Duration, sleep};
use tokio_supervisor::{
    ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleEvent, LifecycleEventKind,
    SupervisorLifecycleEvent, prelude::*,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("worker", |ctx| async move {
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
            let stopped = matches!(
                event.kind,
                LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Stopped)
            );
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
        LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Started) => {
            println!("{scope}: supervisor started");
        }
        LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Stopping) => {
            println!("{scope}: supervisor stopping");
        }
        LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Stopped) => {
            println!("{scope}: supervisor stopped");
        }
        LifecycleEventKind::Child(ChildLifecycleEvent {
            child_id,
            kind: ChildLifecycleEventKind::Added,
            ..
        }) => println!("{scope}: child added: {child_id}"),
        LifecycleEventKind::Child(ChildLifecycleEvent {
            child_id,
            kind: ChildLifecycleEventKind::Started { generation },
            ..
        }) => println!("{scope}: child started: {child_id} generation={generation}"),
        LifecycleEventKind::Child(ChildLifecycleEvent {
            child_id,
            kind:
                ChildLifecycleEventKind::Exited {
                    generation, reason, ..
                },
            ..
        }) => {
            println!("{scope}: child exited: {child_id} generation={generation} reason={reason:?}")
        }
        LifecycleEventKind::Child(ChildLifecycleEvent {
            child_id,
            kind: ChildLifecycleEventKind::Removed,
            ..
        }) => {
            println!("{scope}: child removed: {child_id}");
        }
        LifecycleEventKind::Child(ChildLifecycleEvent {
            child_id,
            kind:
                ChildLifecycleEventKind::RestartScheduled {
                    generation, delay, ..
                },
            ..
        }) => {
            println!(
                "{scope}: child restart scheduled: {child_id} generation={generation} delay={delay:?}"
            );
        }
        LifecycleEventKind::RestartIntensityExceeded { total_restarts, .. } => {
            println!("{scope}: restart intensity exceeded after {total_restarts} restarts");
        }
        LifecycleEventKind::Lagged { dropped } => {
            println!("recursive lifecycle dropped {dropped} tree events; resync snapshots");
        }
        _ => println!("{scope}: unknown lifecycle event"),
    }
}
