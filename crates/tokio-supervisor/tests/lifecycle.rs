use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, time::timeout};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, ChildStateView, CompletionGuard, DynamicSupervisorBuilder,
    LifecycleEvent, LifecyclePathSegment, LifecycleWatch, RestartIntensity, RestartPolicy,
    ShutdownMode, ShutdownPolicy, Strategy, SupervisorBuilder, SupervisorSpec,
};

mod common;

use common::{shutdown, wait_for_snapshot};

async fn next_event(watch: &mut tokio_supervisor::LifecycleWatch) -> LifecycleEvent {
    timeout(common::EVENT_TIMEOUT, watch.next())
        .await
        .expect("timed out waiting for lifecycle event")
        .expect("lifecycle watch closed before the expected event")
}

async fn next_for(
    watch: &mut tokio_supervisor::LifecycleWatch,
    id: &str,
    predicate: impl Fn(&LifecycleEvent) -> bool,
) -> LifecycleEvent {
    loop {
        let event = next_event(watch).await;
        if event.child_id() == Some(id) && predicate(&event) {
            return event;
        }
    }
}

async fn next_recursive(watch: &mut LifecycleWatch) -> LifecycleEvent {
    timeout(common::EVENT_TIMEOUT, watch.next())
        .await
        .expect("timed out waiting for recursive lifecycle event")
        .expect("recursive lifecycle watch closed before the expected event")
}

async fn next_recursive_for(
    watch: &mut LifecycleWatch,
    predicate: impl Fn(&LifecycleEvent) -> bool,
) -> LifecycleEvent {
    loop {
        let event = next_recursive(watch).await;
        if predicate(&event) {
            return event;
        }
    }
}

async fn wait_for_end(watch: &mut LifecycleWatch) {
    timeout(common::EVENT_TIMEOUT, async {
        while watch.next().await.is_some() {}
    })
    .await
    .expect("timed out waiting for lifecycle watch to end");
}

async fn assert_stays_open(watch: &mut LifecycleWatch, message: &str) {
    let ended = timeout(common::QUIET_TIMEOUT, async {
        while watch.next().await.is_some() {}
    })
    .await;
    assert!(ended.is_err(), "{message}");
}

fn path(event: &LifecycleEvent) -> &[LifecyclePathSegment] {
    event.supervisor_path().unwrap_or_default()
}

fn seq(event: &LifecycleEvent) -> u64 {
    event.seq().expect("child transition has a sequence")
}

fn lineage(event: &LifecycleEvent) -> u64 {
    event.lineage().expect("child transition has a lineage")
}

fn total_restarts(event: &LifecycleEvent) -> u64 {
    event
        .total_restarts()
        .expect("child or restart event has a restart count")
}

fn child_restart_count(event: &LifecycleEvent) -> u64 {
    event
        .child_restart_count()
        .expect("child or restart event has a child restart count")
}

#[tokio::test]
async fn recursive_watch_reports_supervisor_transitions_and_restart_backoff() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let builder = SupervisorBuilder::new()
        .restart_intensity(
            RestartIntensity::new(5, Duration::from_secs(60))
                .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(40))),
        )
        .child(
            ChildSpec::new("worker", move |ctx| {
                let crash = Arc::clone(&child_crash);
                async move {
                    if ctx.generation() == 0 {
                        crash.notified().await;
                        return Err(common::test_error("boom"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure),
        );
    let retained = builder.handle();
    let mut tree = retained.watch_lifecycle_recursive();
    let mut direct = retained.watch_lifecycle();
    let handle = builder.build().expect("valid supervisor").spawn();

    let started = next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::SupervisorStarted { supervisor_path }
                if supervisor_path.is_empty()
        )
    })
    .await;
    assert!(path(&started).is_empty());
    handle.wait_started().await.expect("startup succeeds");
    for _ in 0..2 {
        assert!(!matches!(
            next_event(&mut direct).await,
            LifecycleEvent::SupervisorStarted { .. }
                | LifecycleEvent::SupervisorStopping { .. }
                | LifecycleEvent::SupervisorStopped { .. }
        ));
    }

    crash.notify_one();
    let exited = next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::Exited {
                supervisor_path,
                child_id,
                generation: 0,
                ..
            } if supervisor_path.is_empty() && child_id == "worker"
        )
    })
    .await;
    assert!(path(&exited).is_empty());
    let scheduled = next_recursive(&mut tree).await;
    assert!(path(&scheduled).is_empty());
    assert!(matches!(
        scheduled,
        LifecycleEvent::RestartScheduled {
            ref child_id,
            lineage: 0,
            generation: 0,
            delay,
            total_restarts: 1,
            child_restart_count: 1,
            ..
        } if child_id == "worker" && delay == Duration::from_millis(40)
    ));
    assert_eq!(tree.started_after(&[], "worker", 0).await, Some(1));

    handle.shutdown();
    next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::SupervisorStopping { supervisor_path }
                if supervisor_path.is_empty()
        )
    })
    .await;
    next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::SupervisorStopped { supervisor_path }
                if supervisor_path.is_empty()
        )
    })
    .await;
    handle.wait().await.expect("shutdown succeeds");
    assert_eq!(tree.next().await, None);
}

#[tokio::test]
async fn recursive_watch_reattaches_with_new_path_after_ancestor_recreation() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_middle = Arc::new(Notify::new());
    let builder = SupervisorBuilder::new().supervisor(
        SupervisorSpec::new("middle", middle_supervisor(&crash_leaf, &crash_middle))
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
    );
    let retained = builder.handle();
    let mut tree = retained.watch_lifecycle_recursive();
    let handle = builder.build().expect("valid supervisor").spawn();
    handle.wait_started().await.expect("startup succeeds");

    let initial = next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::Started {
                child_id,
                generation: 0,
                ..
            } if child_id == "worker"
        )
    })
    .await;
    assert_eq!(path(&initial).len(), 2);
    assert_eq!(path(&initial)[0].id, "middle");
    assert_eq!(path(&initial)[0].lineage, 0);
    assert_eq!(path(&initial)[0].generation, 0);
    assert_eq!(path(&initial)[1].id, "leaf");
    assert_eq!(path(&initial)[1].generation, 0);

    crash_middle.notify_one();
    let recreated = next_recursive_for(&mut tree, |event| {
        event
            .supervisor_path()
            .unwrap_or_default()
            .first()
            .is_some_and(|segment| segment.id == "middle" && segment.generation == 1)
            && matches!(
                event,
                LifecycleEvent::Started {
                    child_id,
                    generation: 0,
                    ..
                } if child_id == "worker"
            )
    })
    .await;
    assert_eq!(path(&recreated).len(), 2);
    assert_eq!(path(&recreated)[0].lineage, 0);
    assert_eq!(path(&recreated)[0].generation, 1);
    assert_eq!(path(&recreated)[1].id, "leaf");
    assert!(path(&recreated)[1].lineage > path(&initial)[1].lineage);

    shutdown(handle).await;
}

#[tokio::test]
async fn started_after_matches_a_constructed_nested_path() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |ctx| {
                let crash = Arc::clone(&child_crash);
                async move {
                    if ctx.generation() == 0 {
                        crash.notified().await;
                        return Err(common::test_error("boom"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid nested supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(SupervisorSpec::new("nested", nested))
        .build()
        .expect("valid root supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle_recursive();

    crash.notify_one();
    let nested_path = [LifecyclePathSegment::new("nested", 0, 0)];
    assert_eq!(
        lifecycle.started_after(&nested_path, "worker", 0).await,
        Some(1)
    );

    shutdown(handle).await;
}

#[tokio::test]
async fn recursive_watch_path_distinguishes_reinserted_subtree_membership() {
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut tree = handle.watch_lifecycle_recursive();

    let first_lineage = handle
        .add_supervisor(SupervisorSpec::new("nested", idle_supervisor()))
        .await
        .expect("first nested supervisor added");
    let first = next_recursive_for(&mut tree, |event| {
        matches!(event, LifecycleEvent::SupervisorStarted { .. })
            && event
                .supervisor_path()
                .unwrap_or_default()
                .first()
                .is_some_and(|segment| segment.id == "nested")
    })
    .await;
    assert_eq!(path(&first).len(), 1);
    assert_eq!(path(&first)[0].lineage, first_lineage);
    assert_eq!(path(&first)[0].generation, 0);

    handle
        .remove_child("nested")
        .await
        .expect("first nested supervisor removed");
    let second_lineage = handle
        .add_supervisor(SupervisorSpec::new("nested", idle_supervisor()))
        .await
        .expect("replacement nested supervisor added");
    let second = next_recursive_for(&mut tree, |event| {
        matches!(event, LifecycleEvent::SupervisorStarted { .. })
            && event
                .supervisor_path()
                .unwrap_or_default()
                .first()
                .is_some_and(|segment| segment.id == "nested" && segment.lineage == second_lineage)
    })
    .await;
    assert!(second_lineage > first_lineage);
    assert_eq!(path(&second).len(), 1);
    assert_eq!(path(&second)[0].generation, 0);

    shutdown(handle).await;
}

#[tokio::test]
async fn recursive_watch_overflow_is_one_tree_wide_in_band_marker() {
    const RESTARTS: usize = 80;
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let child = ChildSpec::new("storm", move |ctx| {
        let attempts = Arc::clone(&child_attempts);
        async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < RESTARTS {
                return Err(common::test_error("again"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(RestartIntensity::new(100, Duration::from_secs(60)));
    let builder = DynamicSupervisorBuilder::new();
    let retained = builder.handle();
    let mut tree = retained.watch_lifecycle_recursive();
    let handle = builder.build().expect("valid supervisor").spawn();
    handle.wait_started().await.expect("startup succeeds");
    handle.add_child(child).await.expect("dynamic add succeeds");
    let mut snapshots = handle.subscribe_snapshots();
    wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot
            .child("storm")
            .is_some_and(|child| child.generation == RESTARTS as u64 && child.started)
    })
    .await;

    let lagged = next_recursive(&mut tree).await;
    assert!(matches!(lagged, LifecycleEvent::Lagged { dropped: 116 }));
    let last_start = next_recursive_for(&mut tree, |event| {
        matches!(
            event,
            LifecycleEvent::Started {
                child_id,
                generation,
                ..
            } if child_id == "storm" && *generation == RESTARTS as u64
        )
    })
    .await;
    assert!(path(&last_start).is_empty());

    shutdown(handle).await;
}

#[tokio::test]
async fn restart_is_an_ordered_exit_schedule_started_sequence() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let child = ChildSpec::new("worker", move |ctx| {
        let crash = Arc::clone(&child_crash);
        async move {
            if ctx.generation() == 0 {
                crash.notified().await;
                return Err(common::test_error("boom"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let baseline = handle.snapshot();
    let mut lifecycle = handle.watch_lifecycle();

    crash.notify_one();

    let exited = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { generation: 0, .. })
    })
    .await;
    let scheduled = next_event(&mut lifecycle).await;
    assert!(matches!(
        scheduled,
        LifecycleEvent::RestartScheduled {
            ref child_id,
            generation: 0,
            total_restarts: 1,
            child_restart_count: 1,
            ..
        } if child_id == "worker"
    ));
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 1, .. })
    })
    .await;
    assert_eq!(seq(&exited), baseline.lifecycle_seq + 1);
    assert_eq!(seq(&started), seq(&exited) + 1);
    assert_eq!(total_restarts(&exited), 0);
    assert_eq!(total_restarts(&started), 1);
    assert_eq!(child_restart_count(&started), 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn direct_watch_reports_restart_intensity_failure() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let handle = SupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60)))
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash = Arc::clone(&child_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("boom"))
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();

    crash.notify_one();
    next_for(&mut lifecycle, "worker", |event| {
        matches!(event, LifecycleEvent::Exited { generation: 0, .. })
    })
    .await;
    assert!(matches!(
        next_event(&mut lifecycle).await,
        LifecycleEvent::RestartIntensityExceeded { .. }
    ));
    assert!(handle.wait().await.is_err());
}

#[tokio::test]
async fn started_after_reports_membership_removal() {
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("worker is added");
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;

    handle
        .remove_child("worker")
        .await
        .expect("worker removal succeeds");
    assert_eq!(lifecycle.started_after(&[], "worker", baseline).await, None);

    shutdown(handle).await;
}

#[tokio::test]
async fn readiness_gated_started_is_emitted_only_after_ready() {
    let release = Arc::new(Notify::new());
    let child_release = Arc::clone(&release);
    let child = ChildSpec::new("worker", move |ctx| {
        let release = Arc::clone(&child_release);
        async move {
            release.notified().await;
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();

    timeout(
        common::QUIET_TIMEOUT,
        next_for(&mut lifecycle, "worker", |kind| {
            matches!(kind, LifecycleEvent::Started { .. })
        }),
    )
    .await
    .expect_err("no post-baseline Started event is emitted before readiness");
    release.notify_one();
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    assert!(matches!(
        started,
        LifecycleEvent::Started { generation: 0, .. }
    ));

    shutdown(handle).await;
}

#[tokio::test]
async fn remove_on_exit_emits_exited_before_removed() {
    let finish = Arc::new(Notify::new());
    let child_finish = Arc::clone(&finish);
    let child = ChildSpec::new("ephemeral", move |_ctx| {
        let finish = Arc::clone(&child_finish);
        async move {
            finish.notified().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Never)
    .remove_on_exit(true);
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();

    finish.notify_one();
    let exited = next_for(&mut lifecycle, "ephemeral", |kind| {
        matches!(kind, LifecycleEvent::Exited { generation: 0, .. })
    })
    .await;
    let removed = next_for(&mut lifecycle, "ephemeral", |kind| {
        matches!(kind, LifecycleEvent::Removed { .. })
    })
    .await;
    assert_eq!(seq(&removed), seq(&exited) + 1);
    assert!(handle.snapshot().child("ephemeral").is_none());

    shutdown(handle).await;
}

#[tokio::test]
async fn cooperative_remove_publishes_removed_before_reply() {
    let worker = ChildSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .shutdown(ShutdownPolicy::new(
        Duration::from_secs(1),
        ShutdownMode::CooperativeStrict,
    ));
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.add_child(worker).await.expect("worker added");
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();
    let remover = handle.clone();
    let removal = tokio::spawn(async move { remover.remove_child("worker").await });

    let removed = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Removed { .. })
    })
    .await;
    timeout(common::EVENT_TIMEOUT, removal)
        .await
        .expect("remove command resolves")
        .expect("remove task joins")
        .expect("remove succeeds");
    assert!(handle.snapshot().lifecycle_seq >= seq(&removed));
    assert!(handle.snapshot().child("worker").is_none());

    shutdown(handle).await;
}

#[tokio::test]
async fn dynamic_add_then_remove_has_gap_free_membership_lifecycle() {
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();
    handle
        .add_child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("dynamic child added");
    handle.wait_started().await.expect("dynamic child starts");

    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Added { .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    handle
        .remove_child("worker")
        .await
        .expect("dynamic child removed");
    let exited = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { generation: 0, .. })
    })
    .await;
    let removed = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Removed { .. })
    })
    .await;

    assert_eq!(seq(&started), seq(&added) + 1);
    assert_eq!(seq(&exited), seq(&started) + 1);
    assert_eq!(seq(&removed), seq(&exited) + 1);
    assert_eq!(lineage(&removed), lineage(&added));

    shutdown(handle).await;
}

#[tokio::test]
async fn overflow_collapses_into_one_lagged_marker_and_counters_resync() {
    const RESTARTS: usize = 80;
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let child = ChildSpec::new("storm", move |ctx| {
        let attempts = Arc::clone(&child_attempts);
        async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < RESTARTS {
                return Err(common::test_error("again"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(RestartIntensity::new(100, Duration::from_secs(60)));
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();
    handle.add_child(child).await.expect("dynamic add succeeds");
    let mut snapshots = handle.subscribe_snapshots();
    wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot
            .child("storm")
            .is_some_and(|child| child.generation == RESTARTS as u64 && child.started)
    })
    .await;

    let lagged = next_event(&mut lifecycle).await;
    assert!(matches!(lagged, LifecycleEvent::Lagged { dropped: 115 }));
    let mut last_seq = None;
    loop {
        let event = next_event(&mut lifecycle).await;
        if let Some(event_seq) = event.seq() {
            if let Some(last_seq) = last_seq {
                assert_eq!(event_seq, last_seq + 1);
            }
            last_seq = Some(event_seq);
        }
        if matches!(event, LifecycleEvent::Started { generation, .. } if generation == RESTARTS as u64)
        {
            assert_eq!(event.seq(), Some(162));
            assert_eq!(event.total_restarts(), Some(RESTARTS as u64));
            assert_eq!(event.child_restart_count(), Some(RESTARTS as u64));
            break;
        }
    }

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_snapshot_filter_is_gap_free_under_concurrent_churn() {
    const MEMBERS: usize = 12;
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();
    let churn = handle.clone();
    let task = tokio::spawn(async move {
        for index in 0..MEMBERS {
            let id = format!("member-{index}");
            churn
                .add_child(
                    ChildSpec::new(id, |_ctx| async { Ok(()) })
                        .restart(RestartPolicy::Never)
                        .remove_on_exit(true),
                )
                .await
                .expect("dynamic insertion succeeds");
        }
    });
    let baseline = handle.snapshot();
    task.await.expect("churn task joins");
    let final_seq = handle.snapshot().lifecycle_seq;
    let mut observed = Vec::new();
    while observed.last().copied().unwrap_or(baseline.lifecycle_seq) < final_seq {
        let event = next_event(&mut lifecycle).await;
        if seq(&event) <= baseline.lifecycle_seq {
            continue;
        }
        observed.push(seq(&event));
    }
    assert!(final_seq > baseline.lifecycle_seq);
    assert_eq!(observed.first().copied(), Some(baseline.lifecycle_seq + 1));
    assert!(observed.windows(2).all(|pair| pair[1] == pair[0] + 1));

    shutdown(handle).await;
}

#[tokio::test]
async fn nested_sequence_and_counters_continue_across_ancestor_recreation() {
    let crash_worker = Arc::new(Notify::new());
    let crash_fatal = Arc::new(Notify::new());
    let worker_crash = Arc::clone(&crash_worker);
    let fatal_crash = Arc::clone(&crash_fatal);
    let middle = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |ctx| {
                let crash = Arc::clone(&worker_crash);
                async move {
                    if ctx.generation() == 0 {
                        crash.notified().await;
                        return Err(common::test_error("worker boom"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .shutdown(ShutdownPolicy::abort()),
        )
        .child(
            ChildSpec::new("fatal", move |_ctx| {
                let crash = Arc::clone(&fatal_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("fatal boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60)))
            .shutdown(ShutdownPolicy::abort()),
        )
        .build()
        .expect("valid middle supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            SupervisorSpec::new("middle", middle)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid root supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let middle = handle.supervisor("middle").expect("middle handle");
    let initial_worker_lineage = middle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .lineage;
    let mut lifecycle = middle.watch_lifecycle();

    crash_worker.notify_one();
    let restarted = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 1, .. })
    })
    .await;
    assert_eq!(total_restarts(&restarted), 1);
    crash_fatal.notify_one();
    let fatal_exit = next_for(&mut lifecycle, "fatal", |kind| {
        matches!(kind, LifecycleEvent::Exited { generation: 0, .. })
    })
    .await;
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Added { .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    assert_eq!(seq(&added), seq(&fatal_exit) + 1);
    assert!(seq(&started) > seq(&added));
    assert!(lineage(&added) > initial_worker_lineage);
    assert_eq!(lineage(&started), lineage(&added));
    assert_eq!(total_restarts(&added), 2);
    assert_eq!(total_restarts(&started), 2);

    shutdown(handle).await;
}

#[tokio::test]
async fn pre_spawn_snapshot_declaration_is_followed_by_added_and_started() {
    let release = Arc::new(Notify::new());
    let gate_release = Arc::clone(&release);
    let gate = ChildSpec::new("gate", move |ctx| {
        let release = Arc::clone(&gate_release);
        async move {
            release.notified().await;
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .wait_for_ready();
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid nested supervisor");
    let handle = SupervisorBuilder::new()
        .child(gate)
        .supervisor(SupervisorSpec::new("nested", nested))
        .build()
        .expect("valid supervisor")
        .spawn();
    let nested = handle
        .supervisor("nested")
        .expect("stable nested handle exists while start is queued");
    let mut lifecycle = nested.watch_lifecycle();
    let baseline = nested.snapshot();
    assert_eq!(baseline.lifecycle_seq, 0);
    let declared = baseline
        .child("worker")
        .expect("declared child is projected");
    assert_eq!(declared.state, ChildStateView::Starting);
    assert!(!declared.started);

    release.notify_one();
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Added { .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    assert_eq!(seq(&added), baseline.lifecycle_seq + 1);
    assert_eq!(lineage(&added), declared.lineage);
    assert_eq!(seq(&started), seq(&added) + 1);
    assert_eq!(lineage(&started), declared.lineage);

    shutdown(handle).await;
}

#[tokio::test]
async fn lifecycle_watch_drains_staged_events_before_ending() {
    let handle = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();
    handle.shutdown();
    handle.wait().await.expect("shutdown succeeds");

    let exited = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    assert!(matches!(exited, LifecycleEvent::Exited { .. }));
    assert_eq!(
        timeout(common::EVENT_TIMEOUT, lifecycle.next())
            .await
            .expect("watch ends after staged events"),
        None
    );
}

#[tokio::test]
async fn removing_nested_supervisor_closes_its_lifecycle_watch() {
    let nested = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid nested supervisor");
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_supervisor(SupervisorSpec::new("nested", nested))
        .await
        .expect("nested supervisor added");
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let mut lifecycle = nested.watch_lifecycle();

    handle
        .remove_child("nested")
        .await
        .expect("nested removal succeeds");
    while lifecycle.next().await.is_some() {}
    shutdown(handle).await;
}

#[tokio::test]
async fn group_revivable_nested_watch_stays_open_and_resumes() {
    let complete_leaf = Arc::new(Notify::new());
    let crash_sibling = Arc::new(Notify::new());
    let leaf_complete = Arc::clone(&complete_leaf);
    let leaf_builder = SupervisorBuilder::new().child(
        ChildSpec::new("worker", move |_ctx| {
            let complete = Arc::clone(&leaf_complete);
            async move {
                complete.notified().await;
                Ok(())
            }
        })
        .restart(RestartPolicy::OnFailure)
        .shutdown(ShutdownPolicy::abort()),
    );
    let _leaf_finished = leaf_builder.handle().shutdown_on_completion(["worker"]);
    let leaf = leaf_builder.build().expect("valid leaf supervisor");
    let sibling_crash = Arc::clone(&crash_sibling);
    let handle = SupervisorBuilder::new()
        .strategy(tokio_supervisor::Strategy::OneForAll)
        .supervisor(SupervisorSpec::new("leaf", leaf).restart(RestartPolicy::OnFailure))
        .child(
            ChildSpec::new("sibling", move |_ctx| {
                let crash = Arc::clone(&sibling_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("sibling boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .shutdown(ShutdownPolicy::abort()),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let leaf = handle.supervisor("leaf").expect("leaf handle");
    let mut lifecycle = leaf.watch_lifecycle();

    complete_leaf.notify_one();
    next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    assert_stays_open(
        &mut lifecycle,
        "group-revivable stable identity should remain open",
    )
    .await;

    crash_sibling.notify_one();
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Added { .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    assert_eq!(seq(&started), seq(&added) + 1);

    shutdown(handle).await;
    wait_for_end(&mut lifecycle).await;
}

#[tokio::test]
async fn non_restarted_nested_stop_closes_lifecycle_watch() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash = Arc::clone(&child_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("worker boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid nested supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(SupervisorSpec::new("nested", nested).restart(RestartPolicy::Never))
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let mut lifecycle = nested.watch_lifecycle();

    crash.notify_one();
    next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    wait_for_end(&mut lifecycle).await;

    shutdown(handle).await;
}

#[tokio::test]
async fn parent_stop_closes_watch_while_stable_handle_is_retained() {
    let handle = SupervisorBuilder::new()
        .supervisor(SupervisorSpec::new("nested", idle_supervisor()))
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let mut lifecycle = nested.watch_lifecycle();

    shutdown(handle).await;
    wait_for_end(&mut lifecycle).await;
    assert_eq!(nested.snapshot().total_restarts, 0);
}

#[tokio::test]
async fn lifecycle_watch_survives_restartable_ancestor_reincarnation() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_middle = Arc::new(Notify::new());
    let handle = SupervisorBuilder::new()
        .supervisor(
            SupervisorSpec::new("middle", middle_supervisor(&crash_leaf, &crash_middle))
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let middle = handle.supervisor("middle").expect("middle handle");
    let leaf = middle.supervisor("leaf").expect("leaf handle");
    let mut lifecycle = leaf.watch_lifecycle();

    crash_leaf.notify_one();
    let first_exit = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    assert_stays_open(
        &mut lifecycle,
        "restartable ancestor should keep provisional identity open",
    )
    .await;

    crash_middle.notify_one();
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Added { .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Started { generation: 0, .. })
    })
    .await;
    assert!(seq(&added) > seq(&first_exit));
    assert_eq!(seq(&started), seq(&added) + 1);

    crash_leaf.notify_one();
    let second_exit = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    assert!(seq(&second_exit) > seq(&started));
    assert_eq!(total_restarts(&second_exit), 1);

    shutdown(handle).await;
    wait_for_end(&mut lifecycle).await;
}

#[tokio::test]
async fn ancestor_reincarnation_closes_orphaned_dynamic_lifecycle_watch() {
    let crash_middle = Arc::new(Notify::new());
    let middle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid dynamic middle supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            SupervisorSpec::new("middle", middle)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid root supervisor")
        .spawn();
    handle.wait_started().await.expect("root starts");

    let middle = handle.supervisor("middle").expect("middle handle");
    let bomb_crash = Arc::clone(&crash_middle);
    middle
        .add_child(
            ChildSpec::new("bomb", move |_ctx| {
                let crash = Arc::clone(&bomb_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("middle boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .await
        .expect("bomb added");
    middle
        .add_supervisor(SupervisorSpec::new("orphan", idle_supervisor()))
        .await
        .expect("dynamic descendant added");
    let orphan = middle.supervisor("orphan").expect("orphan handle");
    let mut lifecycle = orphan.watch_lifecycle();
    let mut middle_snapshots = middle.subscribe_snapshots();

    crash_middle.notify_one();
    wait_for_end(&mut lifecycle).await;
    wait_for_snapshot(&mut middle_snapshots, |snapshot| {
        snapshot.total_restarts == 1 && snapshot.child("orphan").is_none()
    })
    .await;

    shutdown(handle).await;
}

#[tokio::test]
async fn rest_for_one_closes_head_but_defers_tail_terminality() {
    let complete_head = Arc::new(Notify::new());
    let complete_tail = Arc::new(Notify::new());
    let (head_supervisor, _head_finished) = completing_supervisor(&complete_head);
    let (tail_supervisor, _tail_finished) = completing_supervisor(&complete_tail);
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::RestForOne)
        .supervisor(SupervisorSpec::new("head", head_supervisor).restart(RestartPolicy::OnFailure))
        .supervisor(SupervisorSpec::new("tail", tail_supervisor).restart(RestartPolicy::OnFailure))
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let head = handle.supervisor("head").expect("head handle");
    let tail = handle.supervisor("tail").expect("tail handle");
    let mut head_lifecycle = head.watch_lifecycle();
    let mut tail_lifecycle = tail.watch_lifecycle();

    complete_head.notify_one();
    next_for(&mut head_lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    wait_for_end(&mut head_lifecycle).await;
    assert_eq!(
        handle.snapshot().state,
        tokio_supervisor::SupervisorStateView::Running
    );

    complete_tail.notify_one();
    next_for(&mut tail_lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEvent::Exited { .. })
    })
    .await;
    assert_stays_open(
        &mut tail_lifecycle,
        "later RestForOne position should remain provisionally revivable",
    )
    .await;

    shutdown(handle).await;
    wait_for_end(&mut tail_lifecycle).await;
}

fn idle_supervisor() -> tokio_supervisor::Supervisor {
    SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid idle supervisor")
}

fn middle_supervisor(
    crash_leaf: &Arc<Notify>,
    crash_middle: &Arc<Notify>,
) -> tokio_supervisor::Supervisor {
    let leaf_crash = Arc::clone(crash_leaf);
    let leaf = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash = Arc::clone(&leaf_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("leaf boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60)))
            .shutdown(ShutdownPolicy::abort()),
        )
        .build()
        .expect("valid leaf supervisor");
    let middle_crash = Arc::clone(crash_middle);
    SupervisorBuilder::new()
        .supervisor(SupervisorSpec::new("leaf", leaf).restart(RestartPolicy::Never))
        .child(
            ChildSpec::new("bomb", move |_ctx| {
                let crash = Arc::clone(&middle_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("middle boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60)))
            .shutdown(ShutdownPolicy::abort()),
        )
        .build()
        .expect("valid middle supervisor")
}

/// A nested supervisor that stops itself once its worker finishes.
///
/// The returned guard must outlive the tree: dropping it cancels the watch.
fn completing_supervisor(
    complete: &Arc<Notify>,
) -> (tokio_supervisor::Supervisor, CompletionGuard) {
    let complete = Arc::clone(complete);
    let builder = SupervisorBuilder::new().child(
        ChildSpec::new("worker", move |_ctx| {
            let complete = Arc::clone(&complete);
            async move {
                complete.notified().await;
                Ok(())
            }
        })
        .restart(RestartPolicy::OnFailure),
    );
    let finished = builder.handle().shutdown_on_completion(["worker"]);
    (
        builder.build().expect("valid completing supervisor"),
        finished,
    )
}

/// `wait_started` must give up at a `Lagged` marker even when the marker's
/// envelope names a different child. The marker stands for a discarded prefix
/// that may have carried the awaited `Started`, so scanning past it on an id
/// mismatch would wait for a transition this watch can no longer deliver.
#[tokio::test]
async fn started_after_reports_a_start_lost_to_overflow() {
    const RESTARTS: usize = 80;
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();

    handle
        .add_child(ChildSpec::new("quiet", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("dynamic add succeeds");

    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    handle
        .add_child(
            ChildSpec::new("storm", move |ctx| {
                let attempts = Arc::clone(&child_attempts);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) < RESTARTS {
                        return Err(common::test_error("again"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(100, Duration::from_secs(60))),
        )
        .await
        .expect("dynamic add succeeds");

    let mut snapshots = handle.subscribe_snapshots();
    wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot
            .child("storm")
            .is_some_and(|child| child.generation == RESTARTS as u64 && child.started)
    })
    .await;

    // The storm has evicted every "quiet" transition, so the marker that now
    // fronts the buffer is stamped with a "storm" envelope.
    let waited = timeout(
        common::EVENT_TIMEOUT,
        lifecycle.started_after(&[], "quiet", 0),
    )
    .await
    .expect("started_after must not outlive the transitions it awaits");
    assert_eq!(waited, None);

    shutdown(handle).await;
}
