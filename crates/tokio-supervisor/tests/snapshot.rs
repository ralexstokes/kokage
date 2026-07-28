use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_supervisor::{
    BackoffPolicy, ChildMembershipView, ChildSnapshot, ChildSpec, ChildStateView,
    DynamicSupervisorBuilder, ExitStatusView, RestartIntensity, RestartPolicy, ScopeKind,
    SupervisorBuilder, SupervisorSnapshot, SupervisorSpec, SupervisorStateView,
};

mod common;

use common::{ObservedEvent, wait_for_snapshot};

#[tokio::test]
async fn initial_snapshot_is_immediately_available_and_preserves_child_order() {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("alpha", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .child(ChildSpec::new("beta", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let snapshot = handle.snapshot();

    assert_eq!(snapshot.kind, ScopeKind::Ordered);
    assert_eq!(snapshot.state, SupervisorStateView::Running);
    assert_eq!(child_ids(&snapshot), vec!["alpha", "beta"]);
    assert_eq!(snapshot.children[0].lineage, 0);
    assert_eq!(snapshot.children[1].lineage, 1);
    for entry in &snapshot.children {
        assert_eq!(entry.membership, ChildMembershipView::Active);
        assert_eq!(entry.last_exit, None);
        assert_eq!(entry.restart_count, 0);
        assert_eq!(entry.next_restart_in, None);
    }

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn nested_supervisors_allocate_lineages_independently() {
    let nested = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid nested supervisor");
    let outer = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid outer supervisor");
    let handle = outer.spawn();

    handle
        .add_child(ChildSpec::new("anchor", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("anchor added");
    handle
        .add_supervisor(SupervisorSpec::new("nested", nested))
        .await
        .expect("nested supervisor added");
    let nested_handle = handle
        .supervisor("nested")
        .expect("nested handle available");
    nested_handle
        .add_child(ChildSpec::new("seed", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("nested seed added");
    nested_handle
        .add_child(ChildSpec::new("late", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("nested dynamic child added");

    let outer_snapshot = handle.snapshot();
    assert_eq!(
        outer_snapshot
            .child("nested")
            .expect("nested supervisor visible")
            .lineage,
        1
    );
    let nested_snapshot = nested_handle.snapshot();
    assert_eq!(
        nested_snapshot
            .child("seed")
            .expect("nested seed visible")
            .lineage,
        0
    );
    assert_eq!(
        nested_snapshot
            .child("late")
            .expect("nested dynamic child visible")
            .lineage,
        1
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn snapshot_shows_restart_state_and_last_exit() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let flaky_child = ChildSpec::new("flaky", move |ctx| {
        let attempts = attempts.clone();
        let starts_tx = starts_tx.clone();
        async move {
            starts_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(
        RestartIntensity::new(5, Duration::from_secs(1))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(200))),
    );

    let supervisor = SupervisorBuilder::new()
        .child(flaky_child)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);

    let restarting = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "flaky").is_some_and(|child| {
            child.state == ChildStateView::Stopped
                && child.restart_count == 1
                && matches!(
                    child.last_exit.as_ref(),
                    Some(ExitStatusView::Failed(message)) if message.contains("boom")
                )
                && child.next_restart_in.is_some()
        })
    })
    .await;
    let flaky = child(&restarting, "flaky").expect("flaky child should exist");
    let lineage = flaky.lineage;
    assert!(
        flaky
            .next_restart_in
            .expect("restart delay should be visible")
            <= Duration::from_millis(200)
    );

    assert_eq!(common::recv_event(&mut starts_rx).await, 1);
    let running_again = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "flaky").is_some_and(|child| {
            child.generation == 1
                && child.state == ChildStateView::Running
                && child.restart_count == 1
                && child.next_restart_in.is_none()
        })
    })
    .await;
    assert!(matches!(
        child(&running_again, "flaky")
            .expect("flaky child should exist")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Failed(message)) if message.contains("boom")
    ));
    assert_eq!(
        child(&running_again, "flaky")
            .expect("flaky child should exist")
            .lineage,
        lineage,
        "restarting the same membership must retain its lineage"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn snapshot_shows_removing_membership_during_child_removal() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let release_for_child = release.clone();
    let removable = ChildSpec::new("removable", move |ctx| {
        let started_tx = started_tx.clone();
        let cancelled_tx = cancelled_tx.clone();
        let release = release_for_child.clone();
        async move {
            started_tx.send(()).expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver dropped");
            release.notified().await;
            Ok(())
        }
    });

    let supervisor = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();
    handle
        .add_child(removable)
        .await
        .expect("removable child should be accepted");
    handle
        .add_child(ChildSpec::new("keeper", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("keeper child should be accepted");

    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removable").await });

    common::recv_event(&mut cancelled_rx).await;

    let removing = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "removable").is_some_and(|entry| {
            entry.membership == ChildMembershipView::Removing
                && entry.state == ChildStateView::Stopping
        })
    })
    .await;
    let removable = child(&removing, "removable").expect("removable child should exist");
    assert_eq!(removable.membership, ChildMembershipView::Removing);
    assert_eq!(removable.state, ChildStateView::Stopping);

    release.notify_one();
    remove_task
        .await
        .expect("remove task should join")
        .expect("child removal should succeed");

    let removed = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "removable").is_none() && child(snapshot, "keeper").is_some()
    })
    .await;
    assert!(child(&removed, "removable").is_none());

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn root_snapshot_includes_nested_supervisor_tree() {
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

    let outer = SupervisorBuilder::new()
        .child(ChildSpec::new("anchor", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .supervisor(SupervisorSpec::new("nested", nested))
        .build()
        .expect("valid outer supervisor");

    let handle = outer.spawn();
    let mut snapshots = handle.subscribe_snapshots();

    common::recv_event(&mut leaf_started_rx).await;

    let snapshot = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "nested").is_some_and(|entry| {
            entry.state == ChildStateView::Running
                && entry.supervisor.as_ref().is_some_and(|nested| {
                    nested.state == SupervisorStateView::Running
                        && child(nested, "leaf").is_some_and(|leaf| {
                            leaf.state == ChildStateView::Running
                                && leaf.membership == ChildMembershipView::Active
                        })
                })
        })
    })
    .await;
    let nested = child(&snapshot, "nested")
        .expect("nested supervisor child should exist")
        .supervisor
        .as_ref()
        .expect("nested snapshot should be present");
    assert_eq!(nested.state, SupervisorStateView::Running);
    assert!(child(nested, "leaf").is_some());

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn stopped_snapshot_remains_available_after_shutdown() {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();

    let _ = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "worker").is_some_and(|child| child.state == ChildStateView::Running)
    })
    .await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");

    let snapshot = handle.snapshot();
    assert_eq!(snapshot.state, SupervisorStateView::Stopped);
    assert_eq!(
        child(&snapshot, "worker")
            .expect("worker child should remain visible")
            .state,
        ChildStateView::Stopped
    );
}

#[tokio::test]
async fn snapshot_reports_stopping_while_shutdown_drains_children() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let release_for_child = Arc::clone(&release);
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", move |ctx| {
            let cancelled_tx = cancelled_tx.clone();
            let release = Arc::clone(&release_for_child);
            async move {
                ctx.shutdown_token().cancelled().await;
                cancelled_tx.send(()).expect("test receiver dropped");
                release.notified().await;
                Ok(())
            }
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();
    let _ = wait_for_snapshot(&mut snapshots, |snapshot| {
        child(snapshot, "worker").is_some_and(|child| child.state == ChildStateView::Running)
    })
    .await;

    handle.shutdown();
    common::recv_event(&mut cancelled_rx).await;
    let stopping = wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot.state == SupervisorStateView::Stopping
    })
    .await;
    assert_eq!(stopping.state, SupervisorStateView::Stopping);

    release.notify_one();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn completed_children_leave_the_supervisor_idle_until_shutdown() {
    let supervisor = SupervisorBuilder::new()
        .child(
            ChildSpec::new("temporary", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();
    let snapshot = wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot
            .children
            .iter()
            .all(|child| child.state == ChildStateView::Stopped)
    })
    .await;
    assert_eq!(snapshot.state, SupervisorStateView::Running);
    assert!(matches!(
        child(&snapshot, "temporary")
            .expect("temporary child should remain visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Completed)
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
    assert_eq!(handle.snapshot().state, SupervisorStateView::Stopped);
}

#[tokio::test]
async fn events_observe_already_published_snapshot_state() {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = common::event_watch(&handle);

    loop {
        match timeout(common::EVENT_TIMEOUT, events.recv())
            .await
            .expect("timed out waiting for supervisor event")
            .expect("supervisor event stream should remain open")
        {
            ObservedEvent::ChildStarted { id, generation, .. }
                if id == "worker" && generation == 0 =>
            {
                let snapshot = handle.snapshot();
                let worker = child(&snapshot, "worker").expect("worker child should exist");
                assert_eq!(snapshot.state, SupervisorStateView::Running);
                assert_eq!(worker.generation, 0);
                assert_eq!(worker.state, ChildStateView::Running);
                break;
            }
            _ => {}
        }
    }

    handle.shutdown();

    loop {
        if timeout(common::EVENT_TIMEOUT, events.recv())
            .await
            .expect("timed out waiting for supervisor event")
            .expect("supervisor event stream should remain open")
            == ObservedEvent::SupervisorStopped
        {
            let snapshot = handle.snapshot();
            assert_eq!(snapshot.state, SupervisorStateView::Stopped);
            assert_eq!(
                child(&snapshot, "worker")
                    .expect("worker child should remain visible")
                    .state,
                ChildStateView::Stopped
            );
            break;
        }
    }

    handle.wait().await.expect("shutdown should succeed");
}

fn child<'a>(snapshot: &'a SupervisorSnapshot, id: &str) -> Option<&'a ChildSnapshot> {
    snapshot.children.iter().find(|child| child.id == id)
}

fn child_ids(snapshot: &SupervisorSnapshot) -> Vec<&str> {
    snapshot
        .children
        .iter()
        .map(|child| child.id.as_str())
        .collect()
}
