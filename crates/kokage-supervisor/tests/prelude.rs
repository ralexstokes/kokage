use std::time::Duration;

use kokage_supervisor::{Backoff, prelude::*};
use tokio::{sync::mpsc, time::timeout};

mod common;
use common::ObservedEvent;

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use kokage_supervisor::prelude::{
            BoxError, ChildContext, ChildResult, ChildSpec, ControlError, DynamicSupervisorBuilder,
            DynamicSupervisorHandle, OrderedSupervisorBuilder, Restart, RunningSupervisor,
            Shutdown, Strategy, Supervisor, SupervisorBuildError, SupervisorError,
            SupervisorHandle,
        };
    }

    mod advanced_root {
        use kokage_supervisor::{
            Backoff, ChildExitView, ChildMembershipView, ChildSnapshot, ChildStateView,
            CompletionError, CompletionOutcome, Guard, LifecycleEvent, LifecyclePathSegment,
            LifecycleWatch, ScopeKind, SupervisorSnapshot, SupervisorStateView,
        };
    }
}

#[test]
fn closed_policy_sets_can_be_matched_exhaustively_in_the_supervisor_crate() {
    fn strategy_name(strategy: Strategy) -> &'static str {
        match strategy {
            Strategy::OneForOne => "one-for-one",
            Strategy::OneForAll => "one-for-all",
            Strategy::RestForOne => "rest-for-one",
        }
    }

    fn restart_name(policy: Restart) -> &'static str {
        if policy == Restart::always() {
            "always"
        } else if policy == Restart::never() {
            "never"
        } else {
            "on-failure"
        }
    }

    fn scope_name(kind: kokage_supervisor::ScopeKind) -> &'static str {
        match kind {
            kokage_supervisor::ScopeKind::Ordered => "ordered",
            kokage_supervisor::ScopeKind::Dynamic => "dynamic",
        }
    }

    assert_eq!(strategy_name(Strategy::default()), "one-for-one");
    assert_eq!(restart_name(Restart::default()), "on-failure");
    assert_eq!(
        scope_name(kokage_supervisor::ScopeKind::default()),
        "ordered"
    );
}

#[tokio::test]
async fn prelude_supports_handle_event_and_snapshot_helpers() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let running = Supervisor::ordered()
        .child(ChildSpec::task("worker", move |ctx| {
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
    let handle = running.handle();

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
    assert!(
        snapshot
            .child("worker")
            .expect("worker child should exist")
            .state
            .is_running()
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn prelude_snapshot_helpers_walk_nested_children() {
    let (leaf_started_tx, mut leaf_started_rx) = mpsc::unbounded_channel();

    let nested = Supervisor::ordered()
        .child(ChildSpec::task("leaf", move |ctx| {
            let leaf_started_tx = leaf_started_tx.clone();
            async move {
                leaf_started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let running = Supervisor::ordered()
        .child(
            ChildSpec::task("anchor", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(Shutdown::drain_for(Duration::from_millis(25))),
        )
        .child(ChildSpec::supervisor("nested", nested))
        .build()
        .expect("valid outer supervisor")
        .spawn();
    let handle = running.handle();

    common::recv_event(&mut leaf_started_rx).await;

    let snapshot = handle.snapshot();
    let nested = snapshot.child("nested").expect("nested child should exist");
    assert!(
        nested
            .supervisor
            .as_deref()
            .and_then(|snapshot| snapshot.child("leaf"))
            .is_some()
    );
    assert!(snapshot.descendant(["nested", "leaf"]).is_some());

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[test]
fn prelude_policy_types_cover_common_configuration() {
    assert_eq!(Shutdown::abort(), Shutdown::abort());

    assert_eq!(
        Restart::on_failure().limit(3, Duration::from_secs(10)),
        Restart::on_failure().limit(3, Duration::from_secs(10))
    );
    let configured = Restart::on_failure()
        .limit(2, Duration::from_secs(5))
        .backoff(Backoff::fixed(Duration::from_millis(50)));
    let expected = Restart::on_failure()
        .limit(2, Duration::from_secs(5))
        .backoff(Backoff::fixed(Duration::from_millis(50)));
    assert_eq!(configured, expected);
}
