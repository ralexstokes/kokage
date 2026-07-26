use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, time::timeout};
use tokio_supervisor::{
    ChildSpec, ChildStateView, LifecycleEvent, LifecycleEventKind, RestartIntensity, RestartPolicy,
    ShutdownMode, ShutdownPolicy, StartMode, Strategy, SupervisorBuilder, SupervisorSpec,
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
    predicate: impl Fn(&LifecycleEventKind) -> bool,
) -> LifecycleEvent {
    loop {
        let event = next_event(watch).await;
        if event.child_id == id && predicate(&event.kind) {
            return event;
        }
    }
}

#[tokio::test]
async fn restart_is_an_ordered_exit_started_pair() {
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
        matches!(kind, LifecycleEventKind::Exited { generation: 0, .. })
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 1 })
    })
    .await;
    assert_eq!(exited.seq, baseline.lifecycle_seq + 1);
    assert_eq!(started.seq, exited.seq + 1);
    assert_eq!(exited.total_restarts, 0);
    assert_eq!(started.total_restarts, 1);
    assert_eq!(started.child_restart_count, 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn wait_started_reports_membership_removal() {
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
    let baseline = handle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .generation;

    handle
        .remove_child("worker")
        .await
        .expect("worker removal succeeds");
    assert_eq!(lifecycle.wait_started("worker", baseline).await, None);

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
            matches!(kind, LifecycleEventKind::Started { .. })
        }),
    )
    .await
    .expect_err("no post-baseline Started event is emitted before readiness");
    release.notify_one();
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 0 })
    })
    .await;
    assert!(matches!(
        started.kind,
        LifecycleEventKind::Started { generation: 0 }
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
        matches!(kind, LifecycleEventKind::Exited { generation: 0, .. })
    })
    .await;
    let removed = next_for(&mut lifecycle, "ephemeral", |kind| {
        matches!(kind, LifecycleEventKind::Removed)
    })
    .await;
    assert_eq!(removed.seq, exited.seq + 1);
    assert!(handle.snapshot().child("ephemeral").is_none());

    shutdown(handle).await;
}

#[tokio::test]
async fn cooperative_remove_publishes_removed_before_reply() {
    let handle = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(ShutdownPolicy::new(
                Duration::from_secs(1),
                ShutdownMode::CooperativeStrict,
            )),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let mut lifecycle = handle.watch_lifecycle();
    let remover = handle.clone();
    let removal = tokio::spawn(async move { remover.remove_child("worker").await });

    let removed = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Removed)
    })
    .await;
    timeout(common::EVENT_TIMEOUT, removal)
        .await
        .expect("remove command resolves")
        .expect("remove task joins")
        .expect("remove succeeds");
    assert!(handle.snapshot().lifecycle_seq >= removed.seq);
    assert!(handle.snapshot().child("worker").is_none());

    shutdown(handle).await;
}

#[tokio::test]
async fn queued_child_can_be_added_then_removed_without_starting() {
    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    let first = ChildSpec::new("first", move |ctx| {
        let release = Arc::clone(&first_release);
        async move {
            release.notified().await;
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(first)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();
    handle
        .add_child(ChildSpec::new("queued", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("queued child insertion succeeds");
    handle
        .remove_child("queued")
        .await
        .expect("queued child removal succeeds");

    let added = next_for(&mut lifecycle, "queued", |kind| {
        matches!(kind, LifecycleEventKind::Added)
    })
    .await;
    let removed = next_for(&mut lifecycle, "queued", |kind| {
        matches!(kind, LifecycleEventKind::Removed)
    })
    .await;
    assert_eq!(removed.seq, added.seq + 1);

    release.notify_one();
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
    let handle = SupervisorBuilder::new()
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
    assert_eq!(lagged.seq, 35);
    assert!(matches!(
        lagged.kind,
        LifecycleEventKind::Lagged { dropped: 35 }
    ));
    assert_eq!(lagged.total_restarts, 16);
    assert_eq!(lagged.child_restart_count, 16);
    let first_retained = next_event(&mut lifecycle).await;
    assert_eq!(first_retained.seq, 36);
    assert_eq!(first_retained.total_restarts, 17);

    let mut last = first_retained;
    while last.seq < 162 {
        let event = next_event(&mut lifecycle).await;
        assert_eq!(event.seq, last.seq + 1);
        last = event;
    }
    assert_eq!(last.total_restarts, RESTARTS as u64);
    assert_eq!(last.child_restart_count, RESTARTS as u64);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_snapshot_filter_is_gap_free_under_concurrent_churn() {
    const MEMBERS: usize = 12;
    let handle = SupervisorBuilder::new()
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
        if event.seq <= baseline.lifecycle_seq {
            continue;
        }
        observed.push(event.seq);
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
            .restart(RestartPolicy::OnFailure),
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
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid middle supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid root supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let middle = handle.supervisor("middle").expect("middle handle");
    let initial_worker_epoch = middle
        .snapshot()
        .child("worker")
        .expect("worker is supervised")
        .membership_epoch;
    let mut lifecycle = middle.watch_lifecycle();

    crash_worker.notify_one();
    let restarted = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 1 })
    })
    .await;
    assert_eq!(restarted.total_restarts, 1);
    crash_fatal.notify_one();
    let fatal_exit = next_for(&mut lifecycle, "fatal", |kind| {
        matches!(kind, LifecycleEventKind::Exited { generation: 0, .. })
    })
    .await;
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Added)
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 0 })
    })
    .await;
    assert_eq!(added.seq, fatal_exit.seq + 1);
    assert!(started.seq > added.seq);
    assert!(added.membership_epoch > initial_worker_epoch);
    assert_eq!(started.membership_epoch, added.membership_epoch);
    assert_eq!(added.total_restarts, 2);
    assert_eq!(started.total_restarts, 2);

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
        .start_mode(StartMode::Sequential)
        .child(gate)
        .supervisor("nested", nested)
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
        matches!(kind, LifecycleEventKind::Added)
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 0 })
    })
    .await;
    assert_eq!(added.seq, baseline.lifecycle_seq + 1);
    assert_eq!(added.membership_epoch, declared.membership_epoch);
    assert_eq!(started.seq, added.seq + 1);
    assert_eq!(started.membership_epoch, declared.membership_epoch);

    shutdown(handle).await;
}

#[tokio::test]
async fn closure_drains_staged_events_and_closed_does_not_consume() {
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

    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("closed resolves at root terminality");
    let exited = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    assert!(matches!(exited.kind, LifecycleEventKind::Exited { .. }));
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
    let handle = SupervisorBuilder::new()
        .supervisor("nested", nested)
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let mut lifecycle = nested.watch_lifecycle();

    handle
        .remove_child("nested")
        .await
        .expect("nested removal succeeds");
    while lifecycle.next().await.is_some() {}
    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("nested lifecycle closes eagerly on removal");

    shutdown(handle).await;
}

#[tokio::test]
async fn group_revivable_nested_watch_stays_open_and_resumes() {
    let complete_leaf = Arc::new(Notify::new());
    let crash_sibling = Arc::new(Notify::new());
    let leaf_complete = Arc::clone(&complete_leaf);
    let leaf = SupervisorBuilder::new()
        .auto_shutdown(tokio_supervisor::AutoShutdown::AnySignificant)
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let complete = Arc::clone(&leaf_complete);
                async move {
                    complete.notified().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .significant(),
        )
        .build()
        .expect("valid leaf supervisor");
    let sibling_crash = Arc::clone(&crash_sibling);
    let handle = SupervisorBuilder::new()
        .strategy(tokio_supervisor::Strategy::OneForAll)
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::OnFailure),
        )
        .child(
            ChildSpec::new("sibling", move |_ctx| {
                let crash = Arc::clone(&sibling_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("sibling boom"))
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let leaf = handle.supervisor("leaf").expect("leaf handle");
    let mut lifecycle = leaf.watch_lifecycle();

    complete_leaf.notify_one();
    next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    timeout(common::QUIET_TIMEOUT, lifecycle.closed())
        .await
        .expect_err("group-revivable stable identity remains open");

    crash_sibling.notify_one();
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Added)
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 0 })
    })
    .await;
    assert_eq!(started.seq, added.seq + 1);

    shutdown(handle).await;
    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("root shutdown closes revived nested watch");
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
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let mut lifecycle = nested.watch_lifecycle();

    crash.notify_one();
    next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("non-restarted nested identity closes");

    shutdown(handle).await;
}

#[tokio::test]
async fn parent_stop_closes_watch_while_stable_handle_is_retained() {
    let handle = SupervisorBuilder::new()
        .supervisor("nested", idle_supervisor())
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let nested = handle.supervisor("nested").expect("nested handle");
    let lifecycle = nested.watch_lifecycle();

    shutdown(handle).await;
    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("root terminality closes descendant watch");
    assert_eq!(nested.snapshot().total_restarts, 0);
}

#[tokio::test]
async fn lifecycle_watch_survives_restartable_ancestor_reincarnation() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_middle = Arc::new(Notify::new());
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle_supervisor(&crash_leaf, &crash_middle))
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
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    timeout(common::QUIET_TIMEOUT, lifecycle.closed())
        .await
        .expect_err("restartable ancestor keeps provisional identity open");

    crash_middle.notify_one();
    let added = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Added)
    })
    .await;
    let started = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Started { generation: 0 })
    })
    .await;
    assert!(added.seq > first_exit.seq);
    assert_eq!(started.seq, added.seq + 1);

    crash_leaf.notify_one();
    let second_exit = next_for(&mut lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    assert!(second_exit.seq > started.seq);
    assert_eq!(second_exit.total_restarts, 1);

    shutdown(handle).await;
    timeout(common::EVENT_TIMEOUT, lifecycle.closed())
        .await
        .expect("root terminality closes revived descendant watch");
}

#[tokio::test]
async fn ancestor_reincarnation_closes_orphaned_and_displaced_dynamic_watches() {
    let crash_middle = Arc::new(Notify::new());
    let bomb_crash = Arc::clone(&crash_middle);
    let middle_supervisor = SupervisorBuilder::new()
        .supervisor("slot", idle_supervisor())
        .child(
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
        .build()
        .expect("valid middle supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle_supervisor)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.wait_started().await.expect("startup succeeds");
    let middle = handle.supervisor("middle").expect("middle handle");

    middle
        .remove_child("slot")
        .await
        .expect("static slot removal succeeds");
    middle
        .add_supervisor("slot", idle_supervisor())
        .await
        .expect("dynamic collision add succeeds");
    middle
        .add_supervisor("orphan", idle_supervisor())
        .await
        .expect("dynamic orphan add succeeds");
    let displaced = middle.supervisor("slot").expect("dynamic slot handle");
    let orphaned = middle.supervisor("orphan").expect("dynamic orphan handle");
    let displaced_lifecycle = displaced.watch_lifecycle();
    let orphaned_lifecycle = orphaned.watch_lifecycle();
    let mut middle_snapshots = middle.subscribe_snapshots();

    crash_middle.notify_one();
    timeout(common::EVENT_TIMEOUT, displaced_lifecycle.closed())
        .await
        .expect("static reconciliation closes displaced dynamic identity");
    timeout(common::EVENT_TIMEOUT, orphaned_lifecycle.closed())
        .await
        .expect("ancestor reincarnation closes orphaned dynamic identity");

    wait_for_snapshot(&mut middle_snapshots, |snapshot| {
        snapshot.total_restarts == 1
            && snapshot
                .child("bomb")
                .is_some_and(|bomb| bomb.state == ChildStateView::Running)
            && snapshot
                .descendant(["slot", "worker"])
                .is_some_and(|worker| worker.state == ChildStateView::Running)
            && snapshot.child("orphan").is_none()
    })
    .await;
    let fresh_slot = middle.supervisor("slot").expect("fresh static identity");
    let fresh_lifecycle = fresh_slot.watch_lifecycle();
    timeout(common::QUIET_TIMEOUT, fresh_lifecycle.closed())
        .await
        .expect_err("fresh static identity remains live");

    shutdown(handle).await;
}

#[tokio::test]
async fn rest_for_one_closes_head_but_defers_tail_terminality() {
    let complete_head = Arc::new(Notify::new());
    let complete_tail = Arc::new(Notify::new());
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::RestForOne)
        .supervisor(
            "head",
            SupervisorSpec::new(completing_supervisor(&complete_head))
                .restart(RestartPolicy::OnFailure),
        )
        .supervisor(
            "tail",
            SupervisorSpec::new(completing_supervisor(&complete_tail))
                .restart(RestartPolicy::OnFailure),
        )
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
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    timeout(common::EVENT_TIMEOUT, head_lifecycle.closed())
        .await
        .expect("first RestForOne position cannot be revived");
    assert_eq!(
        handle.snapshot().state,
        tokio_supervisor::SupervisorStateView::Running
    );

    complete_tail.notify_one();
    next_for(&mut tail_lifecycle, "worker", |kind| {
        matches!(kind, LifecycleEventKind::Exited { .. })
    })
    .await;
    timeout(common::QUIET_TIMEOUT, tail_lifecycle.closed())
        .await
        .expect_err("later RestForOne position remains provisionally revivable");

    shutdown(handle).await;
    timeout(common::EVENT_TIMEOUT, tail_lifecycle.closed())
        .await
        .expect("root terminality closes deferred tail identity");
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
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid leaf supervisor");
    let middle_crash = Arc::clone(crash_middle);
    SupervisorBuilder::new()
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::Never),
        )
        .child(
            ChildSpec::new("bomb", move |_ctx| {
                let crash = Arc::clone(&middle_crash);
                async move {
                    crash.notified().await;
                    Err(common::test_error("middle boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid middle supervisor")
}

fn completing_supervisor(complete: &Arc<Notify>) -> tokio_supervisor::Supervisor {
    let complete = Arc::clone(complete);
    SupervisorBuilder::new()
        .auto_shutdown(tokio_supervisor::AutoShutdown::AnySignificant)
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let complete = Arc::clone(&complete);
                async move {
                    complete.notified().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .significant(),
        )
        .build()
        .expect("valid completing supervisor")
}

/// `wait_started` must give up at a `Lagged` marker even when the marker's
/// envelope names a different child. The marker stands for a discarded prefix
/// that may have carried the awaited `Started`, so scanning past it on an id
/// mismatch would wait for a transition this watch can no longer deliver.
#[tokio::test]
async fn wait_started_reports_a_start_lost_to_overflow() {
    const RESTARTS: usize = 80;
    let handle = SupervisorBuilder::new()
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
    let waited = timeout(common::EVENT_TIMEOUT, lifecycle.wait_started("quiet", 0))
        .await
        .expect("wait_started must not outlive the transitions it awaits");
    assert_eq!(waited, None);

    shutdown(handle).await;
}
