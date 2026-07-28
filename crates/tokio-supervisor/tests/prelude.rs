use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_supervisor::{BackoffPolicy, ChildStateView, ShutdownMode, prelude::*};

mod common;
use common::ObservedEvent;

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use tokio_supervisor::prelude::{
            BoxError, ChildContext, ChildResult, ChildSpec, ControlError, DynamicSupervisorBuilder,
            RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy, Supervisor,
            SupervisorBuildError, SupervisorBuilder, SupervisorError, SupervisorHandle,
            SupervisorSpec,
        };
    }

    mod advanced_root {
        use tokio_supervisor::{
            BackoffPolicy, ChildMembershipView, ChildSnapshot, ChildStateView, CompletionGuard,
            CompletionOutcome, ControlOperation, ExitStatusView, LifecycleEvent,
            LifecyclePathSegment, LifecycleWatch, ScopeKind, ShutdownMode, SupervisorSnapshot,
            SupervisorStateView,
        };
    }
}

#[tokio::test]
async fn prelude_supports_handle_event_and_snapshot_helpers() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let handle = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", move |ctx| {
            let started_tx = started_tx.clone();
            async move {
                started_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut events = common::event_watch(&handle);
    assert_eq!(common::recv_event(&mut started_rx).await, 0);

    let started = timeout(
        common::EVENT_TIMEOUT,
        events.wait_for_event(|event| {
            matches!(
                event,
                ObservedEvent::ChildStarted { id, generation: 0 , .. } if id == "worker"
            )
        }),
    )
    .await
    .expect("timed out waiting for started event")
    .expect("event stream should remain open");
    assert!(matches!(
        started,
        ObservedEvent::ChildStarted {
            ref id,
            generation: 0,
            ..
        } if id == "worker"
    ));

    let snapshot = handle.snapshot();
    assert_eq!(
        snapshot
            .child("worker")
            .expect("worker child should exist")
            .state,
        ChildStateView::Running
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn prelude_snapshot_helpers_walk_nested_children() {
    let (leaf_started_tx, mut leaf_started_rx) = mpsc::unbounded_channel();

    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |ctx| {
            let leaf_started_tx = leaf_started_tx.clone();
            async move {
                leaf_started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let handle = SupervisorBuilder::new()
        .child(
            ChildSpec::new("anchor", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(ShutdownPolicy::new(
                Duration::from_millis(25),
                ShutdownMode::CooperativeStrict,
            )),
        )
        .supervisor(SupervisorSpec::new("nested", nested))
        .build()
        .expect("valid outer supervisor")
        .spawn();

    common::recv_event(&mut leaf_started_rx).await;

    let snapshot = handle.snapshot();
    let nested = snapshot.child("nested").expect("nested child should exist");
    assert!(nested.child("leaf").is_some());
    assert!(snapshot.descendant(["nested", "leaf"]).is_some());

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[test]
fn prelude_policy_types_cover_common_configuration() {
    assert_eq!(ShutdownPolicy::abort().mode, ShutdownMode::Abort);
    assert!(ShutdownPolicy::abort().grace.is_zero());

    assert_eq!(
        RestartIntensity::new(3, Duration::from_secs(10)),
        RestartIntensity::new(3, Duration::from_secs(10))
    );
    assert_eq!(
        RestartIntensity::new(2, Duration::from_secs(5))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(50))),
        RestartIntensity::new(2, Duration::from_secs(5))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(50)))
    );
}
