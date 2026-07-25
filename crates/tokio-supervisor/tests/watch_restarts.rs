#![allow(deprecated)]

use std::{sync::Arc, time::Duration};

use tokio::{sync::Notify, time::timeout};
use tokio_supervisor::{
    ChildSpec, ChildStateView, DynamicSupervisorBuilder, RestartIntensity, RestartPolicy, Strategy,
    SupervisorBuilder, SupervisorSpec,
};

mod common;

use common::{
    fail_on_generations, failing_child, shutdown, wait_for_child_running, wait_for_snapshot,
};

#[tokio::test]
async fn watch_restarts_reports_each_restart_as_a_delta() {
    let trigger_failure = Arc::new(Notify::new());
    let child = fail_on_generations("worker", trigger_failure.clone(), 3);
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut restarts = handle.watch_restarts();
    assert_eq!(restarts.observed(), 0);

    let mut snapshots = handle.subscribe_snapshots();
    for generation in 1..=3 {
        trigger_failure.notify_one();
        let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
            .await
            .expect("restart watch should observe the restart")
            .expect("supervisor is still running");
        assert_eq!(delta, 1);
        wait_for_child_running(&mut snapshots, "worker", generation).await;
    }
    assert_eq!(restarts.observed(), 3);
    assert_eq!(handle.snapshot().total_restarts, 3);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_restarts_baseline_excludes_prior_restarts() {
    let trigger_failure = Arc::new(Notify::new());
    let child = fail_on_generations("worker", trigger_failure.clone(), 2);
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut snapshots = handle.subscribe_snapshots();
    trigger_failure.notify_one();
    wait_for_child_running(&mut snapshots, "worker", 1).await;

    let mut restarts = handle.watch_restarts();
    assert_eq!(restarts.observed(), 1);
    timeout(common::QUIET_TIMEOUT, restarts.next())
        .await
        .expect_err("watch must not report restarts recorded before creation");

    trigger_failure.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the restart")
        .expect("supervisor is still running");
    assert_eq!(delta, 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_restarts_ends_after_supervisor_stops() {
    let handle = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut restarts = handle.watch_restarts();
    shutdown(handle).await;

    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after shutdown");
    assert_eq!(observed, None);
}

#[tokio::test]
async fn total_restarts_survives_child_removal() {
    let trigger_failure = Arc::new(Notify::new());
    let child = fail_on_generations("flaky", trigger_failure.clone(), 2);
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_child(child)
        .await
        .expect("flaky child should be accepted");
    handle
        .add_child(ChildSpec::new("steady", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("steady child should be accepted");

    let mut snapshots = handle.subscribe_snapshots();
    for generation in 1..=2 {
        trigger_failure.notify_one();
        wait_for_child_running(&mut snapshots, "flaky", generation).await;
    }

    handle
        .remove_child("flaky")
        .await
        .expect("child removal should succeed");
    let snapshot =
        wait_for_snapshot(&mut snapshots, |snapshot| snapshot.child("flaky").is_none()).await;

    // The removed child's per-child count is gone, but the supervisor-level
    // cumulative counter keeps every recorded restart.
    let per_child_sum: u64 = snapshot
        .children
        .iter()
        .map(|child| child.restart_count)
        .sum();
    assert_eq!(per_child_sum, 0);
    assert_eq!(snapshot.total_restarts, 2);

    shutdown(handle).await;
}

#[tokio::test]
async fn one_for_all_sibling_respawns_do_not_count_as_restarts() {
    let trigger_failure = Arc::new(Notify::new());
    let failing = fail_on_generations("failing", trigger_failure.clone(), 1)
        .restart_intensity(RestartIntensity::new(5, Duration::from_secs(1)));
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(failing)
        .child(
            ChildSpec::new("sibling", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut snapshots = handle.subscribe_snapshots();
    trigger_failure.notify_one();
    wait_for_child_running(&mut snapshots, "failing", 1).await;
    wait_for_child_running(&mut snapshots, "sibling", 1).await;

    // Both children were respawned, but only the exiting child's restart was
    // scheduled; the counter matches the intensity-window semantics rather
    // than the number of `ChildRestarted` events.
    assert_eq!(handle.snapshot().total_restarts, 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn clean_exits_restarted_under_policy_always_are_counted() {
    let trigger_exit = Arc::new(Notify::new());
    let trigger_for_child = trigger_exit.clone();
    let child = ChildSpec::new("worker", move |ctx| {
        let trigger_exit = trigger_for_child.clone();
        async move {
            if ctx.generation() == 0 {
                trigger_exit.notified().await;
                return Ok(());
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);
    let handle = SupervisorBuilder::new()
        .child(child)
        .build()
        .expect("valid supervisor")
        .spawn();

    let mut restarts = handle.watch_restarts();
    trigger_exit.notify_one();

    // The clean exit is restarted under `Always`, so it is a scheduled
    // restart and counts — the counter tracks the intensity window, not
    // failures.
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the restart")
        .expect("supervisor is still running");
    assert_eq!(delta, 1);
    assert_eq!(handle.snapshot().total_restarts, 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn nested_counter_is_monotonic_across_incarnations() {
    let trigger_failure = Arc::new(Notify::new());
    let trigger_for_child = trigger_failure.clone();
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let trigger_failure = trigger_for_child.clone();
                async move {
                    trigger_failure.notified().await;
                    Err(common::test_error("boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(1, Duration::from_secs(60))),
        )
        .build()
        .expect("valid nested supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor("nested", nested)
        .build()
        .expect("valid supervisor")
        .spawn();

    let nested_handle = handle.supervisor("nested").expect("nested supervisor");
    let mut restarts = nested_handle.watch_restarts();
    let mut nested_snapshots = nested_handle.subscribe_snapshots();

    // First crash: restart #1 fits the nested intensity budget.
    trigger_failure.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the restart")
        .expect("nested supervisor keeps running");
    assert_eq!(delta, 1);
    wait_for_child_running(&mut nested_snapshots, "worker", 1).await;

    // Second crash: restart #2 exceeds the nested intensity budget, so the
    // nested supervisor itself fails and is restarted by the parent. Its
    // replacement incarnation must resume the counter, not reset it.
    trigger_failure.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the recorded restart")
        .expect("stable channel outlives the incarnation");
    assert_eq!(delta, 1);
    wait_for_snapshot(&mut nested_snapshots, |snapshot| {
        snapshot.total_restarts == 2
            && snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Running)
    })
    .await;

    // Third crash inside the fresh incarnation (fresh intensity window).
    trigger_failure.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the restart")
        .expect("nested supervisor keeps running");
    assert_eq!(delta, 1);

    assert_eq!(restarts.observed(), 3);
    assert_eq!(nested_handle.snapshot().total_restarts, 3);
    // The nested supervisor's own restart is the parent's, not the child's.
    assert_eq!(handle.snapshot().total_restarts, 1);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_restarts_ends_when_nested_child_is_removed() {
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_supervisor("nested", idle_supervisor())
        .await
        .expect("nested supervisor added");

    let nested_handle = handle.supervisor("nested").expect("nested supervisor");
    let mut restarts = nested_handle.watch_restarts();

    handle
        .remove_child("nested")
        .await
        .expect("removal should succeed");

    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after removal");
    assert_eq!(observed, None);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_restarts_ends_when_parent_stops_while_stable_handle_is_held() {
    let handle = SupervisorBuilder::new()
        .supervisor("nested", idle_supervisor())
        .build()
        .expect("valid supervisor")
        .spawn();

    // Keeping the stable handle alive keeps the stable channels alive; the
    // watch must still terminate once the parent has stopped for good.
    let nested_handle = handle.supervisor("nested").expect("nested supervisor");
    let mut restarts = nested_handle.watch_restarts();

    shutdown(handle).await;

    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after parent shutdown");
    assert_eq!(observed, None);
    assert_eq!(nested_handle.snapshot().total_restarts, 0);
}

#[tokio::test]
async fn watch_restarts_ends_when_nested_child_stops_without_restart() {
    let trigger_failure = Arc::new(Notify::new());
    let trigger_for_child = trigger_failure.clone();
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let trigger_failure = trigger_for_child.clone();
                async move {
                    trigger_failure.notified().await;
                    Err(common::test_error("boom"))
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
            tokio_supervisor::SupervisorSpec::new(nested).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    let nested_handle = handle.supervisor("nested").expect("nested supervisor");
    let mut restarts = nested_handle.watch_restarts();

    // The worker's failure schedules a restart (which is recorded, so the
    // watch reports it) but exceeds the zero intensity budget; the nested
    // supervisor fails and, under `RestartPolicy::Never`, is never respawned
    // — after the final delta the watch must terminate instead of hanging.
    trigger_failure.notify_one();

    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should report the recorded restart");
    assert_eq!(observed, Some(1));

    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after terminal stop");
    assert_eq!(observed, None);

    shutdown(handle).await;
}

#[tokio::test]
async fn watch_survives_ancestor_reincarnation() {
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

    let middle_handle = handle.supervisor("middle").expect("middle supervisor");
    let leaf_handle = middle_handle.supervisor("leaf").expect("leaf supervisor");
    let mut restarts = leaf_handle.watch_restarts();
    let mut leaf_snapshots = leaf_handle.subscribe_snapshots();

    // The leaf's worker failure schedules one recorded restart, exceeds the
    // zero budget, and the leaf fails; the middle supervisor never restarts
    // it (`RestartPolicy::Never`).
    crash_leaf.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should report the recorded restart")
        .expect("leaf identity is still alive");
    assert_eq!(delta, 1);

    // The middle supervisor is not the root: its judgment is provisional, so
    // the leaf's stable channels must stay open — an ancestor restart can
    // recreate the leaf from static configuration.
    timeout(common::QUIET_TIMEOUT, restarts.next())
        .await
        .expect_err("watch must stay open under a live restartable ancestor");

    // Fail the middle supervisor; the root restarts it, which recreates the
    // leaf on the same stable channels with the counter carried forward.
    crash_middle.notify_one();
    wait_for_snapshot(&mut leaf_snapshots, |snapshot| {
        snapshot.total_restarts == 1
            && snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Running)
    })
    .await;

    // The revived leaf keeps reporting restarts through the same watch.
    crash_leaf.notify_one();
    let delta = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should observe the revived leaf's restart")
        .expect("leaf identity is still alive");
    assert_eq!(delta, 1);
    assert_eq!(restarts.observed(), 2);
    assert_eq!(leaf_handle.snapshot().total_restarts, 2);

    // Root shutdown is final and cascades through the middle supervisor.
    shutdown(handle).await;
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after root shutdown");
    assert_eq!(observed, None);
}

#[tokio::test]
async fn orphaned_dynamic_child_watch_ends_after_ancestor_reincarnation() {
    let crash_middle = Arc::new(Notify::new());
    let middle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid dynamic supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle)
                .restart(RestartPolicy::OnFailure)
                .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .expect("startup should not time out")
        .expect("startup should succeed");
    let middle_handle = handle.supervisor("middle").expect("middle supervisor");
    middle_handle
        .add_child(failing_child("bomb", &crash_middle, "middle boom"))
        .await
        .expect("dynamic bomb should be added");
    middle_handle
        .add_supervisor("dyn", idle_supervisor())
        .await
        .expect("dynamic add should succeed");
    let dyn_handle = middle_handle.supervisor("dyn").expect("dynamic supervisor");
    let mut restarts = dyn_handle.watch_restarts();

    // The middle supervisor's replacement incarnation is rebuilt from static
    // configuration, so the dynamically added child is never spawned again:
    // its watch must terminate rather than hang.
    crash_middle.notify_one();
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after the orphaning restart");
    assert_eq!(observed, None);

    shutdown(handle).await;
}

#[tokio::test]
async fn never_chain_terminates_without_root_shutdown() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_for_leaf = crash_leaf.clone();
    let leaf = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash_leaf = crash_for_leaf.clone();
                async move {
                    crash_leaf.notified().await;
                    Err(common::test_error("leaf boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid leaf supervisor");
    let middle = SupervisorBuilder::new()
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid middle supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    let middle_handle = handle.supervisor("middle").expect("middle supervisor");
    let leaf_handle = middle_handle.supervisor("leaf").expect("leaf supervisor");
    let mut restarts = leaf_handle.watch_restarts();

    // The leaf fails and is never restarted. Because the middle supervisor
    // is itself `Never` under the root, no ancestor reincarnation can revive
    // the leaf: the watch must terminate even though the root and the middle
    // supervisor keep running.
    crash_leaf.notify_one();
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should report the recorded restart");
    assert_eq!(observed, Some(1));
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve for an unrevivable chain");
    assert_eq!(observed, None);
    assert_eq!(
        middle_handle.snapshot().state,
        tokio_supervisor::SupervisorStateView::Running
    );

    shutdown(handle).await;
}

#[tokio::test]
async fn group_restart_revives_cleanly_exited_supervisor_child() {
    let complete_leaf = Arc::new(Notify::new());
    let crash_sibling = Arc::new(Notify::new());

    let complete_for_leaf = complete_leaf.clone();
    let leaf = SupervisorBuilder::new()
        .auto_shutdown(tokio_supervisor::AutoShutdown::AnySignificant)
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let complete_leaf = complete_for_leaf.clone();
                async move {
                    complete_leaf.notified().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .significant(),
        )
        .build()
        .expect("valid leaf supervisor");
    let crash_for_sibling = crash_sibling.clone();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::OnFailure),
        )
        .child(
            ChildSpec::new("sibling", move |_ctx| {
                let crash_sibling = crash_for_sibling.clone();
                async move {
                    crash_sibling.notified().await;
                    Err(common::test_error("sibling boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    let leaf_handle = handle.supervisor("leaf").expect("leaf supervisor");
    let mut restarts = leaf_handle.watch_restarts();
    let mut leaf_snapshots = leaf_handle.subscribe_snapshots();

    // The leaf auto-shuts down cleanly; under `OnFailure` it is not
    // restarted. But under `OneForAll` a later sibling failure respawns it,
    // so its stable identity must NOT be closed — even at the root.
    complete_leaf.notify_one();
    wait_for_snapshot(&mut leaf_snapshots, |snapshot| {
        snapshot.state == tokio_supervisor::SupervisorStateView::Stopped
    })
    .await;
    timeout(common::QUIET_TIMEOUT, restarts.next())
        .await
        .expect_err("watch must stay open: a group restart can revive the child");

    crash_sibling.notify_one();
    wait_for_snapshot(&mut leaf_snapshots, |snapshot| {
        snapshot.state == tokio_supervisor::SupervisorStateView::Running
            && snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Running)
    })
    .await;

    shutdown(handle).await;
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve after root shutdown");
    assert_eq!(observed, None);
}

#[tokio::test]
async fn dynamic_never_supervisor_breaks_the_revival_chain() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_for_leaf = crash_leaf.clone();
    let leaf = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash_leaf = crash_for_leaf.clone();
                async move {
                    crash_leaf.notified().await;
                    Err(common::test_error("leaf boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid leaf supervisor");
    let dynamic = SupervisorBuilder::new()
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid dynamic supervisor");
    let middle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid dynamic supervisor");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            SupervisorSpec::new(middle).restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor")
        .spawn();

    timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .expect("startup should not time out")
        .expect("startup should succeed");
    let middle_handle = handle.supervisor("middle").expect("middle supervisor");
    middle_handle
        .add_supervisor(
            "dyn",
            SupervisorSpec::new(dynamic).restart(RestartPolicy::Never),
        )
        .await
        .expect("dynamic add should succeed");

    let dyn_handle = middle_handle.supervisor("dyn").expect("dynamic supervisor");
    let leaf_handle = dyn_handle.supervisor("leaf").expect("leaf supervisor");
    let mut restarts = leaf_handle.watch_restarts();

    // The middle supervisor is revivable, but a reincarnation of it would
    // orphan the dynamically added `Never` supervisor rather than recreate
    // it — the revival chain is broken at the dynamic edge, so the leaf's
    // stop is final and its watch must terminate while every supervisor
    // above it keeps running.
    crash_leaf.notify_one();
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should report the recorded restart");
    assert_eq!(observed, Some(1));
    let observed = timeout(common::EVENT_TIMEOUT, restarts.next())
        .await
        .expect("restart watch should resolve across the dynamic Never edge");
    assert_eq!(observed, None);
    assert_eq!(
        dyn_handle.snapshot().state,
        tokio_supervisor::SupervisorStateView::Running
    );

    shutdown(handle).await;
}

#[tokio::test]
async fn rest_for_one_first_child_stop_is_final() {
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

    let head_handle = handle.supervisor("head").expect("head supervisor");
    let tail_handle = handle.supervisor("tail").expect("tail supervisor");
    let mut head_restarts = head_handle.watch_restarts();
    let mut tail_restarts = tail_handle.watch_restarts();

    // Under `RestForOne` a group restart respawns only the suffix from the
    // failing position, so nothing can ever revive the first child: its
    // clean, non-restarted stop is final and the watch must end eagerly,
    // while the supervisor keeps running.
    complete_head.notify_one();
    let observed = timeout(common::EVENT_TIMEOUT, head_restarts.next())
        .await
        .expect("first child's watch should resolve eagerly");
    assert_eq!(observed, None);
    assert_eq!(
        handle.snapshot().state,
        tokio_supervisor::SupervisorStateView::Running
    );

    // A later position is conservatively kept open: an earlier sibling's
    // failure could still revive it, so its watch closes only when the
    // supervisor stops.
    complete_tail.notify_one();
    timeout(common::QUIET_TIMEOUT, tail_restarts.next())
        .await
        .expect_err("later-position stop must stay open while the supervisor runs");

    shutdown(handle).await;
    let observed = timeout(common::EVENT_TIMEOUT, tail_restarts.next())
        .await
        .expect("later-position watch should resolve after supervisor stop");
    assert_eq!(observed, None);
}

fn completing_supervisor(complete: &Arc<Notify>) -> tokio_supervisor::Supervisor {
    let complete = complete.clone();
    SupervisorBuilder::new()
        .auto_shutdown(tokio_supervisor::AutoShutdown::AnySignificant)
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let complete = complete.clone();
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

fn middle_supervisor(
    crash_leaf: &Arc<Notify>,
    crash_middle: &Arc<Notify>,
) -> tokio_supervisor::Supervisor {
    let crash_leaf = crash_leaf.clone();
    let leaf = SupervisorBuilder::new()
        .child(
            ChildSpec::new("worker", move |_ctx| {
                let crash_leaf = crash_leaf.clone();
                async move {
                    crash_leaf.notified().await;
                    Err(common::test_error("leaf boom"))
                }
            })
            .restart(RestartPolicy::OnFailure)
            .restart_intensity(RestartIntensity::new(0, Duration::from_secs(60))),
        )
        .build()
        .expect("valid leaf supervisor");

    SupervisorBuilder::new()
        .supervisor(
            "leaf",
            SupervisorSpec::new(leaf).restart(RestartPolicy::Never),
        )
        .child(failing_child("bomb", crash_middle, "middle boom"))
        .build()
        .expect("valid middle supervisor")
}

fn idle_supervisor() -> tokio_supervisor::Supervisor {
    SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid nested supervisor")
}
