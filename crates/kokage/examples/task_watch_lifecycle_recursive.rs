use kokage::{
    Tree,
    observe::{ChildEventKind, LifecycleEvent, LifecycleEventKind},
};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    tree.add_task("worker", |ctx| async move {
        println!("worker started");
        ctx.shutdown_token().cancelled().await;
        println!("worker shutting down");
        Ok(())
    });
    let running = tree.spawn()?;
    let mut events = running.scope().lifecycle_events();

    let observer = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            let stopped = matches!(event.kind, LifecycleEventKind::SupervisorStopped);
            print_event(&event);
            if stopped {
                break;
            }
        }
    });

    sleep(Duration::from_millis(200)).await;
    running.shutdown().await?;
    observer.await?;

    Ok(())
}

fn print_event(event: &LifecycleEvent) {
    let path = event
        .scope_path
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
        LifecycleEventKind::SupervisorStarted => {
            println!("{scope}: supervisor started");
        }
        LifecycleEventKind::SupervisorStopping => {
            println!("{scope}: supervisor stopping");
        }
        LifecycleEventKind::SupervisorStopped => {
            println!("{scope}: supervisor stopped");
        }
        LifecycleEventKind::Child(child) => match &child.kind {
            ChildEventKind::Added => println!("{scope}: child added: {}", child.child_id),
            ChildEventKind::Started { generation } => println!(
                "{scope}: child started: {} generation={generation}",
                child.child_id
            ),
            ChildEventKind::Exited { generation, exit } => println!(
                "{scope}: child exited: {} generation={generation} exit={exit:?}",
                child.child_id
            ),
            ChildEventKind::Removed => println!("{scope}: child removed: {}", child.child_id),
            ChildEventKind::RestartScheduled { generation, delay } => println!(
                "{scope}: child restart scheduled: {} generation={generation} delay={delay:?}",
                child.child_id
            ),
            _ => println!("{scope}: unknown child lifecycle event"),
        },
        LifecycleEventKind::RestartIntensityExceeded { total_restarts, .. } => {
            println!("{scope}: restart intensity exceeded after {total_restarts} restarts");
        }
        LifecycleEventKind::Lagged { dropped } => {
            println!("recursive lifecycle dropped {dropped} tree events; resync snapshots");
        }
        _ => println!("{scope}: unknown lifecycle event"),
    }
}
