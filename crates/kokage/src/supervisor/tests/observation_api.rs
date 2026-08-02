use std::{sync::Arc, time::Duration};

use crate::supervisor::{
    ChildEventKind, ChildObservationUpdate, ChildSpec, CompletionError, LifecycleEventKind,
    RestartPolicy, Shutdown, SnapshotRecvError, Supervisor, TaskSpec,
};
use tokio::{sync::Notify, time::timeout};

const WAIT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn lifecycle_observation_aligns_snapshot_before_stream_consumption() {
    let supervisor = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = supervisor.handle();
    let observation = handle.observe_children();
    let baseline = observation.snapshot.lifecycle_seq;
    assert!(observation.snapshot.child("worker").is_some());
    let mut events = observation.events;
    let running = supervisor.build().expect("supervisor builds").spawn();

    let reflected_seq = timeout(WAIT, async {
        loop {
            let update = events.next().await.expect("observation remains open");
            match update {
                ChildObservationUpdate::Transition(child)
                    if child.child_id == "worker"
                        && matches!(child.kind, ChildEventKind::Added) =>
                {
                    break child.seq;
                }
                ChildObservationUpdate::Reset { snapshot, dropped }
                    if snapshot.lifecycle_seq > baseline =>
                {
                    assert_eq!(dropped, 0);
                    assert!(snapshot.child("worker").is_some());
                    break snapshot.lifecycle_seq;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the projected child is reflected by a transition or reset");
    assert!(reflected_seq > baseline);

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn child_observation_snapshot_source_survives_rebinding_and_closes_with_identity() {
    let leaf_builder = Supervisor::dynamic();
    let leaf = leaf_builder.handle();
    let leaf_supervisor = leaf_builder.build().expect("leaf supervisor builds");
    let crash_middle = Arc::new(Notify::new());
    let bomb_crash = Arc::clone(&crash_middle);
    let middle = Supervisor::ordered()
        .child_spec(ChildSpec::supervisor("leaf", leaf_supervisor))
        .child(
            TaskSpec::new("bomb", move |_| {
                let crash = Arc::clone(&bomb_crash);
                async move {
                    crash.notified().await;
                    Err(std::io::Error::other("middle boom").into())
                }
            })
            .restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(60)))
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("middle supervisor builds");
    let running = Supervisor::ordered()
        .child_spec(
            ChildSpec::supervisor("middle", middle)
                .restart(RestartPolicy::on_failure().limit(5, Duration::from_secs(60))),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("initial tree starts");

    let observation = leaf.observe_children();
    let baseline = observation.snapshot.lifecycle_seq;
    let mut updates = observation.events;

    crash_middle.notify_one();
    timeout(
        WAIT,
        root.subscribe_snapshots()
            .wait_for_child("middle", |child| {
                child.generation >= 1 && child.state.is_running()
            }),
    )
    .await
    .expect("middle replacement starts")
    .expect("root snapshot source remains open");

    let reincarnation_reset = timeout(WAIT, async {
        loop {
            let update = updates.next().await.expect("leaf observation remains open");
            if let ChildObservationUpdate::Reset { snapshot, dropped } = update
                && dropped == 0
            {
                break snapshot;
            }
        }
    })
    .await
    .expect("leaf reincarnation yields a reset");
    assert_eq!(reincarnation_reset, leaf.snapshot());

    let dynamic = leaf.dynamic().expect("leaf retains its dynamic capability");
    for index in 0..70 {
        let id = format!("overflow-{index}");
        dynamic
            .add_child(TaskSpec::new(id.clone(), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await
            .expect("temporary child is added");
        dynamic
            .remove_child(&id)
            .await
            .expect("temporary child is removed");
    }

    let reset = timeout(WAIT, async {
        loop {
            let update = updates.next().await.expect("leaf observation remains open");
            if let ChildObservationUpdate::Reset { snapshot, dropped } = update
                && dropped > 0
            {
                break (snapshot, dropped);
            }
        }
    })
    .await
    .expect("overflow yields a reset after stable identity rebinding");
    assert!(reset.1 > 0);
    assert!(reset.0.lifecycle_seq > baseline);
    assert!(reset.0.lifecycle_seq >= leaf.snapshot().lifecycle_seq);

    running.shutdown_and_wait().await.expect("clean shutdown");
    timeout(WAIT, async { while updates.next().await.is_some() {} })
        .await
        .expect("stable observation closes with the terminal identity");
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
    let mut recursive = handle.subscribe_lifecycle();
    let mut direct = handle.subscribe_lifecycle().direct_children();
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
