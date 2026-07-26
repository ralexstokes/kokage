use tokio::time::{Duration, sleep, timeout};
use tokio_supervisor::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("api", |ctx| async move {
            println!("api started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("api shutting down");
            Ok(())
        }))
        .build()?;

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();

    wait_for_child_started(&mut events, "api").await?;

    handle
        .add_child(ChildSpec::new("cache-warmer", |ctx| async move {
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

    handle.remove_child("cache-warmer").await?;
    wait_for_child_removed(&mut events, "cache-warmer").await?;
    println!("cache-warmer removed at runtime");

    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("seed", |ctx| async move {
            println!("nested seed started in generation {}", ctx.generation());
            ctx.shutdown_token().cancelled().await;
            println!("nested seed shutting down");
            Ok(())
        }))
        .build()?;

    handle
        .add_supervisor(SupervisorSpec::new("nested", nested))
        .await?;
    wait_for_nested_supervisor_started(&mut events, "nested").await?;
    wait_for_nested_child_started(&mut events, "nested", "seed").await?;
    println!("nested supervisor added at runtime");

    let nested = handle
        .supervisor("nested")
        .expect("nested supervisor handle should be available");
    nested
        .add_child(ChildSpec::new("nested-cache", |ctx| async move {
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

    nested.remove_child("nested-cache").await?;
    wait_for_nested_child_removed(&mut events, "nested", "nested-cache").await?;
    println!("nested-cache removed from nested supervisor");

    handle.shutdown();
    handle.wait().await?;
    println!("supervisor stopped");

    Ok(())
}

async fn wait_for_child_started(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = timeout(
        Duration::from_secs(2),
        events.wait_for_event(
            |event| matches!(event, SupervisorEvent::ChildStarted { id, .. } if id == child_id),
        ),
    )
    .await??;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_child_removed(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = timeout(
        Duration::from_secs(2),
        events.wait_for_event(
            |event| matches!(event, SupervisorEvent::ChildRemoved { id , .. } if id == child_id),
        ),
    )
    .await??;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_supervisor_started(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    nested_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = timeout(
        Duration::from_secs(2),
        events.wait_for_event(|event| {
            matches!(
                event,
                SupervisorEvent::Nested {
                    id,
                    generation: 0,
                    event,
                .. } if id == nested_id && matches!(**event, SupervisorEvent::SupervisorStarted)
            )
        }),
    )
    .await??;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_child_started(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    nested_id: &str,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = timeout(
        Duration::from_secs(2),
        events.wait_for_event(|event| {
            matches!(
                event,
                SupervisorEvent::Nested {
                    id,
                    generation: 0,
                    event,
                .. } if id == nested_id
                    && matches!(
                        **event,
                        SupervisorEvent::ChildStarted {
                            ref id,
                            generation: 0,
                        .. } if id == child_id
                    )
            )
        }),
    )
    .await??;
    println!("event: {event:?}");
    Ok(())
}

async fn wait_for_nested_child_removed(
    events: &mut tokio::sync::broadcast::Receiver<SupervisorEvent>,
    nested_id: &str,
    child_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = timeout(
        Duration::from_secs(2),
        events.wait_for_event(|event| {
            matches!(
                event,
                SupervisorEvent::Nested {
                    id,
                    generation: 0,
                    event,
                .. } if id == nested_id
                    && matches!(**event, SupervisorEvent::ChildRemoved { ref id , .. } if id == child_id)
            )
        }),
    )
    .await??;
    println!("event: {event:?}");
    Ok(())
}
