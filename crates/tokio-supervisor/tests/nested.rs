use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, broadcast, mpsc},
    time::timeout,
};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, ChildStateView, ControlError, ControlOperation,
    DynamicSupervisorBuilder, EventPathSegment, ExitStatusView, RestartIntensity, RestartPolicy,
    ScopeKind, ShutdownPolicy, SupervisorBuilder, SupervisorError, SupervisorEvent, SupervisorSpec,
    SupervisorStateView,
};

mod common;

#[tokio::test]
async fn nested_supervisor_completes_as_a_clean_child_exit() {
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never))
        .build()
        .expect("valid nested supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut snapshots = handle.subscribe_snapshots();
    let completed = snapshots
        .wait_for(|snapshot| {
            snapshot
                .descendant(["nested", "leaf"])
                .is_some_and(|child| child.state == ChildStateView::Stopped)
        })
        .await
        .expect("nested completion snapshot should remain available")
        .clone();
    assert_eq!(completed.state, SupervisorStateView::Running);
    assert!(matches!(
        completed
            .descendant(["nested", "leaf"])
            .expect("leaf remains visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Completed)
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn nested_terminal_failure_remains_in_the_nested_snapshot() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let nested_attempts = attempts.clone();
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("leaf", move |ctx| {
                let attempts = nested_attempts.clone();
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx.send(()).expect("test receiver dropped");
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(common::test_error("nested failure"))
                    } else {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid nested supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();

    common::recv_event(&mut starts_rx).await;
    let mut snapshots = handle.subscribe_snapshots();
    let failed = snapshots
        .wait_for(|snapshot| {
            snapshot
                .descendant(["nested", "leaf"])
                .is_some_and(|child| child.state == ChildStateView::Stopped)
        })
        .await
        .expect("nested failure snapshot should remain available")
        .clone();
    assert!(matches!(
        failed
            .descendant(["nested", "leaf"])
            .expect("leaf remains visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Failed(message)) if message.contains("nested failure")
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parent_shutdown_propagates_into_nested_supervisor() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let cancellations = Arc::new(AtomicUsize::new(0));

    let nested_cancellations = cancellations.clone();
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |ctx| {
            let started_tx = started_tx.clone();
            let cancellations = nested_cancellations.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                cancellations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor("nested", nested)
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    common::recv_event(&mut started_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dynamically_added_nested_supervisor_can_be_removed() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let cancellations = Arc::new(AtomicUsize::new(0));

    let nested_cancellations = cancellations.clone();
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |ctx| {
            let started_tx = started_tx.clone();
            let cancellations = nested_cancellations.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                cancellations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let outer = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut events = handle.subscribe();

    let membership_epoch = handle
        .add_supervisor("nested", nested)
        .await
        .expect("dynamic nested child should be accepted");
    assert_eq!(
        handle
            .snapshot()
            .child("nested")
            .expect("dynamic nested child is visible")
            .membership_epoch,
        membership_epoch
    );
    common::recv_event(&mut started_rx).await;

    handle
        .remove_child("nested")
        .await
        .expect("dynamic nested child should be removable");

    let mut saw_nested_supervisor_started = false;
    let mut saw_nested_leaf_started = false;
    let mut saw_removed = false;

    while !(saw_nested_supervisor_started && saw_nested_leaf_started && saw_removed) {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::SupervisorStarted) =>
            {
                saw_nested_supervisor_started = true;
            }
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::ChildStarted { ref id, generation: 0 , .. } if id == "leaf") =>
            {
                saw_nested_leaf_started = true;
            }
            SupervisorEvent::ChildRemoved { id, .. } if id == "nested" => {
                saw_removed = true;
            }
            _ => {}
        }
    }

    assert_eq!(cancellations.load(Ordering::SeqCst), 1);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn root_handle_can_add_and_remove_children_inside_nested_supervisor() {
    let (seed_tx, mut seed_rx) = mpsc::unbounded_channel();
    let (dynamic_tx, mut dynamic_rx) = mpsc::unbounded_channel();
    let dynamic_cancellations = Arc::new(AtomicUsize::new(0));

    let seed = ChildSpec::new("seed", move |ctx| {
        let seed_tx = seed_tx.clone();
        async move {
            seed_tx.send(()).expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });
    let nested = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid nested supervisor");

    let outer = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut events = handle.subscribe();

    handle
        .add_supervisor("nested", nested)
        .await
        .expect("dynamic nested child should be accepted");

    let mut saw_nested_supervisor_started = false;
    while !saw_nested_supervisor_started {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::SupervisorStarted) =>
            {
                saw_nested_supervisor_started = true;
            }
            _ => {}
        }
    }

    let dynamic_cancellations_for_child = dynamic_cancellations.clone();
    let nested_handle = handle
        .supervisor("nested")
        .expect("nested supervisor handle should be available");
    nested_handle
        .add_child(seed)
        .await
        .expect("seed child inside nested supervisor should be accepted");
    common::recv_event(&mut seed_rx).await;
    nested_handle
        .add_child(ChildSpec::new("dynamic", move |ctx| {
            let dynamic_tx = dynamic_tx.clone();
            let dynamic_cancellations = dynamic_cancellations_for_child.clone();
            async move {
                dynamic_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                dynamic_cancellations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }))
        .await
        .expect("dynamic child inside nested supervisor should be accepted");

    common::recv_event(&mut dynamic_rx).await;

    nested_handle
        .remove_child("dynamic")
        .await
        .expect("dynamic child inside nested supervisor should be removable");

    let mut saw_nested_dynamic_started = false;
    let mut saw_nested_removed = false;

    while !(saw_nested_dynamic_started && saw_nested_removed) {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::ChildStarted { ref id, generation: 0 , .. } if id == "dynamic") =>
            {
                saw_nested_dynamic_started = true;
            }
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::ChildRemoved { ref id , .. } if id == "dynamic") =>
            {
                saw_nested_removed = true;
            }
            _ => {}
        }
    }

    assert_eq!(dynamic_cancellations.load(Ordering::SeqCst), 1);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn parent_event_stream_includes_forwarded_nested_events() {
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never))
        .build()
        .expect("valid nested supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut events = handle.subscribe();

    let mut saw_nested_supervisor_started = false;
    let mut saw_nested_leaf_started = false;
    let mut saw_nested_leaf_exit = false;

    loop {
        let event = common::recv_supervisor_event(&mut events).await;
        match event {
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::SupervisorStarted) =>
            {
                saw_nested_supervisor_started = true;
            }
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::ChildStarted { ref id, generation: 0 , .. } if id == "leaf") =>
            {
                saw_nested_leaf_started = true;
            }
            SupervisorEvent::Nested {
                id,
                generation,
                event,
                ..
            } if id == "nested"
                && generation == 0
                && matches!(*event, SupervisorEvent::ChildExited { ref id, generation: 0, status: ExitStatusView::Completed , .. } if id == "leaf") =>
            {
                saw_nested_leaf_exit = true;
            }
            SupervisorEvent::SupervisorStopped => break,
            _ => {}
        }

        if saw_nested_leaf_exit {
            handle.shutdown();
        }
    }

    assert!(saw_nested_supervisor_started);
    assert!(saw_nested_leaf_started);
    assert!(saw_nested_leaf_exit);
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn nested_events_preserve_the_full_tree_path() {
    let deepest = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never))
        .build()
        .expect("valid deepest supervisor");

    let middle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(deepest).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid middle supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor(
            "outer",
            SupervisorSpec::new(middle).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut events = handle.subscribe();

    loop {
        let event = common::recv_supervisor_event(&mut events).await;
        if let SupervisorEvent::Nested { .. } = &event
            && matches!(event.leaf(), SupervisorEvent::ChildStarted { id, generation: 0 , .. } if id == "leaf")
        {
            assert_eq!(
                event.path(),
                vec![
                    EventPathSegment::new("outer", 0),
                    EventPathSegment::new("middle", 0),
                ]
            );
            break;
        }
    }

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn removing_nested_supervisor_unregisters_its_control_endpoint() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |ctx| {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid outer supervisor")
        .spawn();

    handle
        .add_supervisor("nested", nested)
        .await
        .expect("nested child should be accepted");
    common::recv_event(&mut started_rx).await;

    let nested_handle = handle
        .supervisor("nested")
        .expect("nested supervisor handle should be available");

    handle
        .remove_child("nested")
        .await
        .expect("nested child should be removable");

    let add_err = nested_handle
        .add_child(ChildSpec::new("late", |_ctx| async move { Ok(()) }))
        .await
        .expect_err("removed nested supervisor should no longer accept child commands");
    assert_eq!(add_err, ControlError::Unavailable);

    let remove_err = nested_handle
        .remove_child("late")
        .await
        .expect_err("removed nested supervisor should no longer accept remove commands");
    assert_eq!(remove_err, ControlError::Unavailable);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn nested_handle_subscription_survives_parent_restart() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let fail_first = Arc::new(Notify::new());
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let nested = SupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .child(ChildSpec::new("leaf", {
            let attempts = attempts.clone();
            let fail_first = fail_first.clone();
            move |ctx| {
                let attempts = attempts.clone();
                let fail_first = fail_first.clone();
                let starts_tx = starts_tx.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    starts_tx.send(attempt).expect("test receiver dropped");
                    if attempt == 0 {
                        fail_first.notified().await;
                        Err(common::test_error("escalate nested supervisor"))
                    } else {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let handle = SupervisorBuilder::new()
        .supervisor("nested", nested)
        .build()
        .expect("valid outer supervisor")
        .spawn();
    let nested_handle = handle
        .supervisor("nested")
        .expect("stable nested handle should exist before the first incarnation starts");
    let initial_kind = nested_handle.snapshot().kind;
    assert_eq!(initial_kind, ScopeKind::Ordered);
    let mut nested_events = nested_handle.subscribe();

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    loop {
        match nested_events.try_recv() {
            Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => {
                panic!("stable nested event channel closed between incarnations")
            }
        }
    }

    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle
        .snapshot()
        .child("nested")
        .expect("nested child exists")
        .generation;
    fail_first.notify_one();
    let restarted_generation = lifecycle
        .wait_started("nested", baseline)
        .await
        .expect("outer supervisor remains live");
    assert_eq!(restarted_generation, baseline + 1);
    assert_eq!(common::recv_event(&mut starts_rx).await, 1);

    loop {
        if matches!(
            common::recv_supervisor_event(&mut nested_events).await,
            SupervisorEvent::SupervisorStarted
        ) {
            break;
        }
    }

    let replacement = nested_handle.snapshot();
    assert_eq!(replacement.state, SupervisorStateView::Running);
    assert_eq!(replacement.kind, initial_kind);
    assert_eq!(
        nested_handle
            .add_child(ChildSpec::new("rebound", |_| async { Ok(()) }))
            .await,
        Err(ControlError::UnsupportedByScopeKind {
            operation: ControlOperation::AddChild,
            kind: ScopeKind::Ordered,
        }),
        "the stable control channel should bind to the replacement incarnation"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn abort_mode_hard_cascades_through_a_nested_supervisor() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let live = common::LiveFlag::new();
    let leaf_live = live.clone();
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |_ctx| {
            let started_tx = started_tx.clone();
            let live = leaf_live.clone();
            async move {
                let _guard = live.guard();
                started_tx.send(()).expect("test receiver dropped");
                std::future::pending().await
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid outer supervisor")
        .spawn();
    handle
        .add_supervisor(
            "nested",
            SupervisorSpec::new(nested).shutdown(ShutdownPolicy::abort()),
        )
        .await
        .expect("nested child should be accepted");
    common::recv_event(&mut started_rx).await;

    handle
        .remove_child("nested")
        .await
        .expect("aborting the wrapper should remove the nested subtree");
    timeout(common::EVENT_TIMEOUT, async {
        while live.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort mode should not leave the nested subtree detached");

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn control_is_unavailable_between_nested_incarnations() {
    let fail = Arc::new(Notify::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let nested = SupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .child(ChildSpec::new("leaf", {
            let fail = fail.clone();
            let attempts = attempts.clone();
            move |ctx| {
                let fail = fail.clone();
                let attempts = attempts.clone();
                let starts_tx = starts_tx.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    starts_tx.send(attempt).expect("test receiver dropped");
                    if attempt == 0 {
                        fail.notified().await;
                        Err(common::test_error("escalate nested supervisor"))
                    } else {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }
        }))
        .build()
        .expect("valid nested supervisor");

    let handle = SupervisorBuilder::new()
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart_intensity(
                RestartIntensity::new(5, Duration::from_secs(30))
                    .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(500))),
            ),
        )
        .build()
        .expect("valid outer supervisor")
        .spawn();

    let nested_handle = handle
        .supervisor("nested")
        .expect("stable nested handle should exist");
    let mut events = handle.subscribe();
    assert_eq!(common::recv_event(&mut starts_rx).await, 0);

    fail.notify_one();
    loop {
        if let SupervisorEvent::ChildExited { id, status, .. } =
            common::recv_supervisor_event(&mut events).await
            && id == "nested"
        {
            assert!(
                matches!(status, ExitStatusView::Failed(_)),
                "nested escalation should surface as a failed child exit, got {status:?}"
            );
            break;
        }
    }

    // The old incarnation has unbound its control slot and the 500ms restart
    // backoff has not elapsed: control must fail fast instead of queueing.
    let err = nested_handle
        .add_child(ChildSpec::new("late", |_ctx| async move { Ok(()) }))
        .await
        .expect_err("control between incarnations should be unavailable");
    assert_eq!(err, ControlError::Unavailable);

    // Once the new incarnation binds, the same stable handle works again.
    assert_eq!(common::recv_event(&mut starts_rx).await, 1);
    assert_eq!(nested_handle.snapshot().state, SupervisorStateView::Running);
    assert_eq!(
        nested_handle
            .add_child(ChildSpec::new("rebound", |_| async { Ok(()) }))
            .await,
        Err(ControlError::UnsupportedByScopeKind {
            operation: ControlOperation::AddChild,
            kind: ScopeKind::Ordered,
        }),
        "the stable control channel should bind to the replacement incarnation"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn grandchild_stable_handle_survives_middle_supervisor_restart() {
    let fail = Arc::new(Notify::new());
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let leafsup = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid grandchild supervisor");

    let mid = SupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .child(ChildSpec::new("fuse", {
            let fail = fail.clone();
            move |_ctx| {
                let fail = fail.clone();
                async move {
                    fail.notified().await;
                    Err(common::test_error("escalate middle supervisor"))
                }
            }
        }))
        .supervisor("leafsup", leafsup)
        .build()
        .expect("valid middle supervisor");

    let handle = SupervisorBuilder::new()
        .supervisor("mid", mid)
        .build()
        .expect("valid outer supervisor")
        .spawn();

    let mid_handle = handle.supervisor("mid").expect("stable mid handle");
    let grand_handle = mid_handle
        .supervisor("leafsup")
        .expect("stable grandchild handle reachable through the mid handle");
    let baseline_seq = grand_handle.snapshot().lifecycle_seq;
    let mut grand_snapshots = grand_handle.subscribe_snapshots();
    let mut grand_events = grand_handle.subscribe();

    common::recv_event(&mut starts_rx).await;
    fail.notify_one();

    // The old grandchild incarnation is shut down with mid's teardown, then
    // the restarted mid brings up a new incarnation bound to the same stable
    // channels — the pre-restart subscription observes both.
    loop {
        if matches!(
            common::recv_supervisor_event(&mut grand_events).await,
            SupervisorEvent::SupervisorStopped
        ) {
            break;
        }
    }
    loop {
        if matches!(
            common::recv_supervisor_event(&mut grand_events).await,
            SupervisorEvent::SupervisorStarted
        ) {
            break;
        }
    }
    common::recv_event(&mut starts_rx).await;

    grand_snapshots
        .wait_for(|snapshot| {
            snapshot.state == SupervisorStateView::Running
                && snapshot.lifecycle_seq > baseline_seq
                && snapshot
                    .child("worker")
                    .is_some_and(|worker| worker.started)
        })
        .await
        .expect("stable snapshot channel should publish the replacement incarnation");
    assert_eq!(
        grand_handle
            .add_child(ChildSpec::new("rebound", |_| async { Ok(()) }))
            .await,
        Err(ControlError::UnsupportedByScopeKind {
            operation: ControlOperation::AddChild,
            kind: ScopeKind::Ordered,
        }),
        "the grandchild control channel should bind to the replacement incarnation"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn fatal_supervisor_failure_hard_cascades_through_nested_supervisors() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let live = common::LiveFlag::new();
    let leaf_live = live.clone();
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("leaf", move |_ctx| {
            let started_tx = started_tx.clone();
            let live = leaf_live.clone();
            async move {
                let _guard = live.guard();
                started_tx.send(()).expect("test receiver dropped");
                std::future::pending().await
            }
        }))
        .build()
        .expect("valid nested supervisor");
    let root = SupervisorBuilder::new()
        .supervisor("nested", nested)
        .child(
            ChildSpec::new("fatal", |_| async { Err(common::test_error("fatal")) })
                .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid root supervisor");

    let handle = root.spawn();
    common::recv_event(&mut started_rx).await;
    assert_eq!(
        handle.wait().await,
        Err(SupervisorError::RestartIntensityExceeded)
    );
    timeout(common::EVENT_TIMEOUT, async {
        while live.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fatal parent failure should not detach a live nested subtree");
}

/// A parent learns a nested supervisor's snapshot only from publications the
/// nested incarnation forwards to it; it never reads that state back. A nested
/// incarnation whose first computed view already matches the baseline its
/// parent bound must therefore still forward that view, or it stays invisible.
#[tokio::test]
async fn nested_supervisor_view_reaches_the_parent_without_diverging_from_its_baseline() {
    let nested = SupervisorBuilder::new()
        .build()
        .expect("valid nested supervisor");

    let outer = SupervisorBuilder::new()
        .supervisor("nested", SupervisorSpec::new(nested))
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    handle.wait_started().await.expect("startup should succeed");

    let mut snapshots = handle.subscribe_snapshots();
    tokio::time::timeout(
        Duration::from_secs(2),
        snapshots.wait_for(|snapshot| {
            snapshot
                .child("nested")
                .is_some_and(|child| child.supervisor.is_some())
        }),
    )
    .await
    .expect("parent snapshot should carry the nested supervisor's view")
    .expect("snapshot watch should stay open");

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}
