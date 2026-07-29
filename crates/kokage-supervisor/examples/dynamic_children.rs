use kokage_supervisor::{
    ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleEvent, LifecycleEventKind,
    LifecyclePathSegment, LifecycleWatch, SupervisorLifecycleEvent, prelude::*,
};
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Supervisor::dynamic().spawn()?;
    let mut events = running.watch_lifecycle_recursive();

    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::task("api", |ctx| async move {
            println!("api started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("api shutting down");
            Ok(())
        }))
        .await?;

    wait_for_child_started(&mut events, "api").await?;

    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::task("cache-warmer", |ctx| async move {
            println!("cache-warmer started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => {
                        println!("cache-warmer received removal/shutdown");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_millis(50)) => {
                        println!("cache-warmer tick");
                    }
                }
            }
        }))
        .await?;

    wait_for_child_started(&mut events, "cache-warmer").await?;
    println!("cache-warmer added at runtime");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    running
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("cache-warmer")
        .await?;
    wait_for_child_removed(&mut events, "cache-warmer").await?;
    println!("cache-warmer removed at runtime");

    let nested = Supervisor::dynamic().build()?;

    running
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::supervisor("nested", nested))
        .await?;
    wait_for_nested_supervisor_started(&mut events, "nested").await?;
    let nested = running
        .supervisor("nested")
        .expect("nested supervisor handle should be available");
    nested
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::task("seed", |ctx| async move {
            println!("nested seed started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("nested seed shutting down");
            Ok(())
        }))
        .await?;
    wait_for_nested_child_started(&mut events, "nested", "seed").await?;
    println!("nested supervisor added at runtime");

    nested
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(ChildSpec::task("nested-cache", |ctx| async move {
            println!("nested-cache started in generation {}", ctx.generation());

            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => {
                        println!("nested-cache received removal/shutdown");
                        return Ok(());
                    }
                    _ = sleep(Duration::from_millis(50)) => {
                        println!("nested-cache tick");
                    }
                }
            }
        }))
        .await?;

    wait_for_nested_child_started(&mut events, "nested", "nested-cache").await?;
    println!("nested-cache added inside nested supervisor");

    // Let the child do visible work before demonstrating runtime removal.
    sleep(Duration::from_millis(150)).await;

    nested
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("nested-cache")
        .await?;
    wait_for_nested_child_removed(&mut events, "nested", "nested-cache").await?;
    println!("nested-cache removed from nested supervisor");

    running.shutdown_and_wait().await?;
    println!("supervisor stopped");

    Ok(())
}

async fn wait_for_child_started(
    events: &mut LifecycleWatch,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = wait_for_event(events, |event| {
        matches!(
            &event.kind,
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id: id,
                kind: ChildLifecycleEventKind::Started { .. },
                ..
            }) if event.supervisor_path.is_empty() && id == child_id
        )
    })
    .await?;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_child_removed(
    events: &mut LifecycleWatch,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = wait_for_event(events, |event| {
        matches!(
            &event.kind,
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id: id,
                kind: ChildLifecycleEventKind::Removed,
                ..
            }) if event.supervisor_path.is_empty() && id == child_id
        )
    })
    .await?;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_supervisor_started(
    events: &mut LifecycleWatch,
    nested_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = wait_for_event(events, |event| {
        matches!(
            event.supervisor_path.as_slice(),
            [LifecyclePathSegment { id, generation: 0, .. }] if id == nested_id
        ) && matches!(
            event.kind,
            LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Started)
        )
    })
    .await?;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_child_started(
    events: &mut LifecycleWatch,
    nested_id: &str,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = wait_for_event(events, |event| {
        matches!(
            event.supervisor_path.as_slice(),
            [LifecyclePathSegment { id, generation: 0, .. }] if id == nested_id
        ) && matches!(
            &event.kind,
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id: id,
                kind: ChildLifecycleEventKind::Started { generation: 0 },
                ..
            }) if id == child_id
        )
    })
    .await?;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_child_removed(
    events: &mut LifecycleWatch,
    nested_id: &str,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = wait_for_event(events, |event| {
        matches!(
            event.supervisor_path.as_slice(),
            [LifecyclePathSegment { id, generation: 0, .. }] if id == nested_id
        ) && matches!(
            &event.kind,
            LifecycleEventKind::Child(ChildLifecycleEvent {
                child_id: id,
                kind: ChildLifecycleEventKind::Removed,
                ..
            }) if id == child_id
        )
    })
    .await?;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_event(
    events: &mut LifecycleWatch,
    mut predicate: impl FnMut(&LifecycleEvent) -> bool,
) -> Result<LifecycleEvent, Box<dyn std::error::Error>> {
    Ok(timeout(Duration::from_secs(2), async {
        loop {
            let event = events
                .next()
                .await
                .ok_or_else(|| std::io::Error::other("lifecycle stream closed"))?;
            if predicate(&event) {
                return Ok::<_, std::io::Error>(event);
            }
        }
    })
    .await??)
}
