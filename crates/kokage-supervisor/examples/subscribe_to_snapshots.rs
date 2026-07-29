use kokage_supervisor::{
    ChildMembershipView, ChildSnapshot, ChildStateView, SupervisorSnapshot, SupervisorStateView,
    prelude::*,
};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nested = Supervisor::ordered()
        .child(ChildSpec::task("leaf", |ctx| async move {
            println!("leaf started");
            ctx.shutdown_token().cancelled().await;
            println!("leaf stopping");
            Ok(())
        }))
        .build()?;

    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("worker", |ctx| async move {
            println!("worker started");
            ctx.shutdown_token().cancelled().await;
            println!("worker stopping");
            Ok(())
        }))
        .child(ChildSpec::supervisor("nested", nested).restart(RestartPolicy::Never))
        .build()?;

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();

    println!("initial snapshot:");
    print_snapshot(&handle.snapshot(), 0);

    let observer = tokio::spawn(async move {
        loop {
            let snapshot = snapshots.changed().await?;
            println!("\nsnapshot update:");
            print_snapshot(&snapshot, 0);

            if snapshot.state == SupervisorStateView::Stopped {
                break;
            }
        }

        Ok::<(), kokage_supervisor::SnapshotRecvError>(())
    });

    sleep(Duration::from_millis(200)).await;
    handle.shutdown();

    handle.wait().await?;
    observer.await??;

    Ok(())
}

fn print_snapshot(snapshot: &SupervisorSnapshot, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{indent}supervisor state={:?} strategy={:?}",
        snapshot.state, snapshot.strategy
    );

    for child in &snapshot.children {
        print_child_snapshot(child, depth + 1);
    }
}

fn print_child_snapshot(child: &ChildSnapshot, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{indent}child id={} generation={} state={} membership={} restarts={} next_restart_in={:?} last_exit={:?}",
        child.id,
        child.generation,
        child_state(&child.state),
        child_membership(child.membership),
        child.restart_count,
        child.next_restart_in,
        child.last_exit()
    );

    if let Some(snapshot) = child.supervisor.as_ref() {
        print_snapshot(snapshot, depth + 1);
    }
}

fn child_state(state: &ChildStateView) -> &'static str {
    match state {
        ChildStateView::Starting { .. } => "starting",
        ChildStateView::Running { .. } => "running",
        ChildStateView::Stopping { .. } => "stopping",
        ChildStateView::Stopped { .. } => "stopped",
        ChildStateView::StartupAborted { .. } => "startup-aborted",
        _ => "unknown",
    }
}

fn child_membership(membership: ChildMembershipView) -> &'static str {
    match membership {
        ChildMembershipView::Active => "active",
        ChildMembershipView::Removing => "removing",
        _ => "unknown",
    }
}
