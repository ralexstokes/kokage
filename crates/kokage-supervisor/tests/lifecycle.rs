use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage_supervisor::{
    BackoffPolicy, ChildSpec, LifecycleEvent, LifecycleEventKind, LifecycleWatch, RestartConfig,
    RestartPolicy, Supervisor, SupervisorError,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

fn failure(message: &'static str) -> kokage_supervisor::BoxError {
    Box::new(io::Error::other(message))
}

async fn next_matching(
    watch: &mut LifecycleWatch,
    mut predicate: impl FnMut(&LifecycleEvent) -> bool,
) -> LifecycleEvent {
    timeout(WAIT, async {
        loop {
            let event = watch.next().await.expect("lifecycle stream remains open");
            if predicate(&event) {
                break event;
            }
        }
    })
    .await
    .expect("matching lifecycle event arrives")
}

#[tokio::test]
async fn pre_spawn_watch_aligns_added_and_started_with_the_projected_snapshot() {
    let builder = Supervisor::ordered().child(ChildSpec::task("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let baseline = handle.snapshot();
    let declared = baseline.child("worker").expect("worker is projected");
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.spawn().expect("supervisor spawns");

    let added = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildAdded { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    let started = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted {
                ref child_id,
                generation: 0,
                ..
            } if child_id == "worker"
        )
    })
    .await;

    assert_eq!(added.seq(), Some(baseline.lifecycle_seq + 1));
    assert_eq!(started.seq(), added.seq().map(|seq| seq + 1));
    assert!(matches!(
        added.kind,
        LifecycleEventKind::ChildAdded { lineage, .. } if lineage == declared.lineage
    ));

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restart_transitions_preserve_exit_schedule_start_order_and_exit_shape() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let mut restart = RestartConfig::new(3, Duration::from_secs(1));
    restart.backoff = BackoffPolicy::Fixed(Duration::from_millis(1));
    let builder = Supervisor::ordered().child(
        ChildSpec::task("flaky", move |ctx| {
            let attempts = Arc::clone(&child_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(failure("first run fails"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })
        .restart(RestartPolicy::OnFailure)
        .restart_config(restart),
    );
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.spawn().expect("supervisor spawns");
    let mut transitions = Vec::new();

    timeout(WAIT, async {
        while transitions.len() < 3 {
            let event = watch.next().await.expect("watch remains open");
            match event.kind {
                LifecycleEventKind::ChildExited {
                    child_id,
                    generation: 0,
                    exit,
                    ..
                } if child_id == "flaky" => {
                    assert!(exit.failure_message().is_some());
                    assert!(!exit.cancelled());
                    transitions.push("exited");
                }
                LifecycleEventKind::ChildRestartScheduled {
                    child_id,
                    generation: 0,
                    ..
                } if child_id == "flaky" => transitions.push("scheduled"),
                LifecycleEventKind::ChildStarted {
                    child_id,
                    generation: 1,
                    ..
                } if child_id == "flaky" => transitions.push("started"),
                _ => {}
            }
        }
    })
    .await
    .expect("restart sequence completes");

    assert_eq!(transitions, ["exited", "scheduled", "started"]);
    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn recursive_paths_follow_nested_supervisor_reincarnation_identity() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let nested = Supervisor::ordered()
        .restart_config(RestartConfig::new(0, Duration::from_secs(1)))
        .child(ChildSpec::task("leaf", move |ctx| {
            let attempts = Arc::clone(&child_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(failure("terminate first nested incarnation"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .build()
        .expect("nested supervisor builds");
    let builder = Supervisor::ordered()
        .restart_config(RestartConfig::new(3, Duration::from_secs(1)))
        .child(ChildSpec::supervisor("nested", nested).restart(RestartPolicy::OnFailure));
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle();
    let running = builder.spawn().expect("supervisor spawns");

    let first = next_matching(&mut watch, |event| {
        matches!(event.kind, LifecycleEventKind::SupervisorStarted)
            && event
                .supervisor_path
                .first()
                .is_some_and(|segment| segment.id == "nested" && segment.generation == 0)
    })
    .await;
    let replacement = next_matching(&mut watch, |event| {
        matches!(event.kind, LifecycleEventKind::SupervisorStarted)
            && event
                .supervisor_path
                .first()
                .is_some_and(|segment| segment.id == "nested" && segment.generation == 1)
    })
    .await;

    assert_eq!(first.supervisor_path.len(), 1);
    assert_eq!(replacement.supervisor_path.len(), 1);
    assert_eq!(
        first.supervisor_path[0].lineage, replacement.supervisor_path[0].lineage,
        "restarts retain the parent membership lineage"
    );

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_removal_emits_cancelled_exit_before_removed_for_one_lineage() {
    let running = Supervisor::dynamic().spawn().expect("supervisor spawns");
    let mut watch = running.watch_lifecycle().direct_children();
    running
        .dynamic()
        .expect("dynamic capability")
        .add_child(ChildSpec::task("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("child is added");
    let started = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    let lineage = match started.kind {
        LifecycleEventKind::ChildStarted { lineage, .. } => lineage,
        _ => unreachable!(),
    };

    running
        .dynamic()
        .expect("dynamic capability")
        .remove_child("worker")
        .await
        .expect("child is removed");
    let exited = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    let removed = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildRemoved { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    assert!(matches!(
        exited.kind,
        LifecycleEventKind::ChildExited { lineage: observed, ref exit, .. }
            if observed == lineage && exit.cancelled()
    ));
    assert!(matches!(
        removed.kind,
        LifecycleEventKind::ChildRemoved { lineage: observed, .. } if observed == lineage
    ));

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restart_intensity_failure_is_an_in_band_scope_event() {
    let builder = Supervisor::ordered()
        .restart_config(RestartConfig::new(0, Duration::from_secs(1)))
        .child(ChildSpec::task("always-fails", |_| async {
            Err(failure("no restart budget"))
        }));
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle();
    let running = builder.spawn().expect("supervisor spawns");

    let intensity = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::RestartIntensityExceeded { .. }
        )
    })
    .await;
    assert!(intensity.supervisor_path.is_empty());
    assert!(matches!(
        running.wait().await,
        Err(SupervisorError::RestartIntensityExceeded)
    ));
}

#[tokio::test]
async fn shutdown_drains_in_reverse_and_the_watch_closes_after_staged_events() {
    let mut builder = Supervisor::ordered();
    for id in ["first", "second", "third"] {
        builder = builder.child(ChildSpec::task(id, |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    }
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.spawn().expect("supervisor spawns");
    running.wait_started().await.expect("children start");
    running.shutdown_and_wait().await.expect("clean shutdown");

    let mut exited = Vec::new();
    let mut stopped = false;
    timeout(WAIT, async {
        while let Some(event) = watch.next().await {
            match event.kind {
                LifecycleEventKind::ChildExited { child_id, .. } => exited.push(child_id),
                LifecycleEventKind::SupervisorStopped => stopped = true,
                _ => {}
            }
        }
    })
    .await
    .expect("terminal watch drains and closes");
    assert_eq!(exited, ["third", "second", "first"]);
    assert!(stopped);
}

#[tokio::test]
async fn overflow_accumulates_one_tree_wide_lag_marker_and_snapshot_realigns() {
    let running = Supervisor::dynamic().spawn().expect("supervisor spawns");
    let mut watch = running.watch_lifecycle().direct_children();

    for index in 0..70 {
        let id = format!("child-{index}");
        running
            .dynamic()
            .expect("dynamic capability")
            .add_child(ChildSpec::task(id.clone(), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await
            .expect("child is added");
        running
            .dynamic()
            .expect("dynamic capability")
            .remove_child(&id)
            .await
            .expect("child is removed");
    }

    let lagged = timeout(WAIT, watch.next())
        .await
        .expect("lag marker arrives")
        .expect("watch remains open");
    assert!(lagged.supervisor_path.is_empty());
    assert!(matches!(
        lagged.kind,
        LifecycleEventKind::Lagged { dropped } if dropped > 1
    ));
    let snapshot = running.snapshot();
    assert!(snapshot.children.is_empty());
    assert!(snapshot.lifecycle_seq >= 140);

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn direct_children_is_a_depth_filter_on_the_recursive_vocabulary() {
    let nested = Supervisor::ordered()
        .child(ChildSpec::task("leaf", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("nested supervisor builds");
    let builder = Supervisor::ordered().child(ChildSpec::supervisor("nested", nested));
    let handle = builder.handle();
    let mut tree = handle.watch_lifecycle();
    let mut direct = handle.watch_lifecycle().direct_children();
    let running = builder.spawn().expect("supervisor spawns");

    let leaf = next_matching(&mut tree, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "leaf"
        )
    })
    .await;
    assert_eq!(leaf.supervisor_path.len(), 1);
    assert_eq!(leaf.supervisor_path[0].id, "nested");

    let nested = next_matching(&mut direct, |event| {
        assert!(event.supervisor_path.is_empty());
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "nested"
        )
    })
    .await;
    assert!(nested.supervisor_path.is_empty());

    running.shutdown_and_wait().await.expect("clean shutdown");
}
