use std::time::Duration;

use crate::supervisor::{
    ChildEventKind, ChildSpec, CompletionError, LifecycleEventKind, RestartPolicy,
    SnapshotRecvError, Supervisor, TaskSpec,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn lifecycle_observation_aligns_snapshot_before_stream_consumption() {
    let supervisor = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = supervisor.handle();
    let observation = handle.observe_lifecycle();
    let baseline = observation.snapshot.lifecycle_seq;
    assert!(observation.snapshot.child("worker").is_some());
    let mut events = observation.events;
    let running = supervisor.build().expect("supervisor builds").spawn();

    let added_seq = timeout(WAIT, async {
        loop {
            let event = events.next().await.expect("lifecycle remains open");
            assert!(event.scope_path.is_empty());
            if let LifecycleEventKind::Child(child) = event.kind
                && child.child_id == "worker"
                && matches!(child.kind, ChildEventKind::Added)
            {
                break child.seq;
            }
        }
    })
    .await
    .expect("the projected child is added");
    assert!(added_seq > baseline);

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn lifecycle_is_recursive_by_default_and_direct_children_is_a_depth_filter() {
    let nested = Supervisor::ordered()
        .child(TaskSpec::new("leaf", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("nested supervisor builds");
    let supervisor = Supervisor::ordered().child_spec(ChildSpec::supervisor("nested", nested));
    let handle = supervisor.handle();
    let mut recursive = handle.watch_lifecycle();
    let mut direct = handle.watch_lifecycle().direct_children();
    let running = supervisor.build().expect("supervisor builds").spawn();

    let nested_started = timeout(WAIT, async {
        loop {
            let event = recursive
                .next()
                .await
                .expect("recursive watch remains open");
            if matches!(
                event.kind,
                LifecycleEventKind::Child(ref child)
                    if child.child_id == "leaf"
                        && matches!(child.kind, ChildEventKind::Started { .. })
            ) {
                break event;
            }
        }
    })
    .await
    .expect("nested child start is observed");
    assert_eq!(nested_started.scope_path.len(), 1);
    assert_eq!(nested_started.scope_path[0].id, "nested");

    let direct_started = timeout(WAIT, async {
        loop {
            let event = direct.next().await.expect("direct watch remains open");
            assert!(event.scope_path.is_empty());
            if matches!(
                event.kind,
                LifecycleEventKind::Child(ref child)
                    if child.child_id == "nested"
                        && matches!(child.kind, ChildEventKind::Started { .. })
            ) {
                break event;
            }
        }
    })
    .await
    .expect("direct child start is observed");
    assert!(direct_started.scope_path.is_empty());

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn wait_for_child_accepts_snapshot_predicates() {
    let supervisor = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = supervisor.handle();
    let mut snapshots = handle.subscribe_snapshots();
    let running = supervisor.build().expect("supervisor builds").spawn();

    let worker = timeout(
        WAIT,
        snapshots.wait_for_child("worker", |child| child.state.is_running()),
    )
    .await
    .expect("worker becomes running")
    .expect("snapshot stream remains open");
    assert_eq!(worker.id, "worker");

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn wait_for_child_terminates_when_an_observed_membership_is_removed() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let dynamic = running
        .handle()
        .dynamic()
        .expect("dynamic capability is present");
    dynamic
        .add_child(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("worker is added");
    running
        .handle()
        .wait_started()
        .await
        .expect("worker starts");

    let mut snapshots = running.handle().subscribe_snapshots();
    let mut wait = Box::pin(snapshots.wait_for_child("worker", |_| false));
    assert!(timeout(Duration::from_millis(20), &mut wait).await.is_err());
    dynamic
        .remove_child("worker")
        .await
        .expect("worker is removed");
    assert_eq!(
        timeout(WAIT, wait).await.expect("removal ends the wait"),
        Err(SnapshotRecvError::ChildRemoved)
    );

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn static_completion_wait_rejects_unknown_children() {
    let supervisor = Supervisor::ordered().child(TaskSpec::new("known", |_| async { Ok(()) }));
    let handle = supervisor.handle();

    assert_eq!(
        handle.wait_for_children(["missing"]).await,
        Err(CompletionError::UnknownChild {
            child_id: "missing".to_owned(),
        })
    );

    assert!(matches!(
        handle.shutdown_when_children_complete(["missing"]),
        Err(CompletionError::UnknownChild { child_id }) if child_id == "missing"
    ));
}

#[tokio::test]
async fn explicitly_dynamic_completion_wait_accepts_future_membership() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let waiter = tokio::spawn({
        let handle = running.handle();
        async move { handle.wait_for_future_children(["job"]).await }
    });

    running
        .handle()
        .dynamic()
        .expect("dynamic capability")
        .add_child(TaskSpec::new("job", |_| async { Ok(()) }).restart(RestartPolicy::never()))
        .await
        .expect("future child is added");

    assert_eq!(
        timeout(WAIT, waiter)
            .await
            .expect("completion wait finishes")
            .expect("completion task joins"),
        Ok(())
    );
    running.shutdown_and_wait().await.expect("clean shutdown");
}
