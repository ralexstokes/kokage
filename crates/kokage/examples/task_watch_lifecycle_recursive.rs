use kokage::{
    OrderedTree, TaskSpec,
    observe::{LifecycleEvent, LifecycleEventKind},
};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("worker", |ctx| async move {
        println!("worker started");
        ctx.shutdown_token().cancelled().await;
        println!("worker shutting down");
        Ok(())
    }));
    let running = tree.spawn()?;
    let handle = running.scope();
    let mut events = handle.watch_lifecycle();

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
    handle.shutdown_and_wait().await?;
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
    let child_id = event
        .child
        .as_ref()
        .map_or("<missing child identity>", |child| child.child_id.as_str());

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
        LifecycleEventKind::ChildAdded => {
            println!("{scope}: child added: {child_id}")
        }
        LifecycleEventKind::ChildStarted { generation } => {
            println!("{scope}: child started: {child_id} generation={generation}")
        }
        LifecycleEventKind::ChildExited { generation, exit } => {
            println!("{scope}: child exited: {child_id} generation={generation} exit={exit:?}")
        }
        LifecycleEventKind::ChildRemoved => {
            println!("{scope}: child removed: {child_id}");
        }
        LifecycleEventKind::ChildRestartScheduled { generation, delay } => {
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
