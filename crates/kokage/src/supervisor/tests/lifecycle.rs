use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{
    Backoff, ChildSpec, Guard, LifecycleEvent, LifecycleEventKind, LifecycleWatch, Restart,
    Shutdown, Strategy, Supervisor, SupervisorError, TaskSpec,
};
use tokio::{sync::Notify, time::timeout};

const WAIT: Duration = Duration::from_secs(2);

fn failure(message: &'static str) -> crate::supervisor::BoxError {
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

async fn wait_closed(watch: &mut LifecycleWatch, phase: &str) {
    timeout(WAIT, async { while watch.next().await.is_some() {} })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"));
}

async fn assert_stays_open(watch: &mut LifecycleWatch, phase: &str) {
    assert!(
        timeout(Duration::from_millis(100), async {
            while watch.next().await.is_some() {}
        })
        .await
        .is_err(),
        "{phase}"
    );
}

#[tokio::test]
async fn pre_spawn_watch_aligns_added_and_started_with_the_projected_snapshot() {
    let builder = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let baseline = handle.snapshot();
    let declared = baseline.child("worker").expect("worker is projected");
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();

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
    let restart = Restart::on_failure()
        .limit(3, Duration::from_secs(1))
        .backoff(Backoff::fixed(Duration::from_millis(50)));
    let builder = Supervisor::ordered().child(
        TaskSpec::new("flaky", move |ctx| {
            let attempts = Arc::clone(&child_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(failure("first run fails"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })
        .restart(restart),
    );
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();
    let mut transitions = Vec::new();

    timeout(WAIT, async {
        while transitions.len() < 3 {
            let event = watch.next().await.expect("watch remains open");
            match event.kind {
                LifecycleEventKind::ChildExited {
                    seq,
                    child_id,
                    generation: 0,
                    total_restarts,
                    child_restart_count,
                    exit,
                    ..
                } if child_id == "flaky" => {
                    assert!(exit.failure_message().is_some());
                    assert!(!exit.cancelled());
                    transitions.push(("exited", seq, total_restarts, child_restart_count));
                }
                LifecycleEventKind::ChildRestartScheduled {
                    seq,
                    child_id,
                    generation: 0,
                    total_restarts,
                    child_restart_count,
                    ..
                } if child_id == "flaky" => {
                    let snapshot = handle.snapshot();
                    let child = snapshot.child("flaky").expect("flaky remains declared");
                    assert_eq!(snapshot.lifecycle_seq, seq);
                    assert_eq!(child.restart_count, child_restart_count);
                    assert!(child.next_restart_in.is_some());
                    transitions.push(("scheduled", seq, total_restarts, child_restart_count));
                }
                LifecycleEventKind::ChildStarted {
                    seq,
                    child_id,
                    generation: 1,
                    total_restarts,
                    child_restart_count,
                    ..
                } if child_id == "flaky" => {
                    transitions.push(("started", seq, total_restarts, child_restart_count));
                }
                _ => {}
            }
        }
    })
    .await
    .expect("restart sequence completes");

    assert_eq!(
        transitions
            .iter()
            .map(|transition| transition.0)
            .collect::<Vec<_>>(),
        ["exited", "scheduled", "started"]
    );
    assert_eq!(transitions[1].1, transitions[0].1 + 1);
    assert_eq!(transitions[2].1, transitions[1].1 + 1);
    assert_eq!((transitions[0].2, transitions[0].3), (0, 0));
    assert_eq!((transitions[1].2, transitions[1].3), (1, 1));
    assert_eq!((transitions[2].2, transitions[2].3), (1, 1));
    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn recursive_paths_follow_nested_supervisor_reincarnation_identity() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let nested = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1)))
        .child(TaskSpec::new("leaf", move |ctx| {
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
        .default_restart(Restart::on_failure().limit(3, Duration::from_secs(1)))
        .child_spec(ChildSpec::supervisor("nested", nested));
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle();
    let running = builder.build().expect("supervisor builds").spawn();

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
async fn nested_sequences_and_counters_continue_across_ancestor_recreation() {
    let crash_worker = Arc::new(Notify::new());
    let crash_fatal = Arc::new(Notify::new());
    let worker_crash = Arc::clone(&crash_worker);
    let fatal_crash = Arc::clone(&crash_fatal);
    let middle = Supervisor::ordered()
        .child(
            TaskSpec::new("worker", move |ctx| {
                let crash = Arc::clone(&worker_crash);
                async move {
                    if ctx.generation() == 0 {
                        crash.notified().await;
                        return Err(failure("worker boom"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(Restart::on_failure())
            .shutdown(Shutdown::abort()),
        )
        .child(
            TaskSpec::new("fatal", move |_| {
                let crash = Arc::clone(&fatal_crash);
                async move {
                    crash.notified().await;
                    Err(failure("fatal boom"))
                }
            })
            .restart(Restart::on_failure().limit(0, Duration::from_secs(60)))
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("middle supervisor builds");
    let running = Supervisor::ordered()
        .child_spec(
            ChildSpec::supervisor("middle", middle)
                .restart(Restart::on_failure().limit(5, Duration::from_secs(60))),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let middle = root.supervisor("middle").expect("middle handle exists");
    let initial_lineage = middle
        .snapshot()
        .child("worker")
        .expect("worker is declared")
        .lineage;
    let mut watch = middle.watch_lifecycle();

    crash_worker.notify_one();
    let restarted = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted {
                ref child_id,
                generation: 1,
                ..
            } if child_id == "worker"
        )
    })
    .await;
    assert_eq!(restarted.total_restarts(), Some(1));

    crash_fatal.notify_one();
    let fatal_exit = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited {
                ref child_id,
                generation: 0,
                ..
            } if child_id == "fatal"
        )
    })
    .await;
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
    assert_eq!(added.seq(), fatal_exit.seq().map(|seq| seq + 1));
    assert!(started.seq() > added.seq());
    let added_lineage = match &added.kind {
        LifecycleEventKind::ChildAdded { lineage, .. } => *lineage,
        _ => unreachable!(),
    };
    let started_lineage = match &started.kind {
        LifecycleEventKind::ChildStarted { lineage, .. } => *lineage,
        _ => unreachable!(),
    };
    assert!(added_lineage > initial_lineage);
    assert_eq!(started_lineage, added_lineage);
    assert_eq!(added.total_restarts(), Some(2));
    assert_eq!(started.total_restarts(), Some(2));

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_removal_emits_cancelled_exit_before_removed_for_one_lineage() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let mut watch = running.handle().watch_lifecycle().direct_children();
    running
        .handle()
        .dynamic()
        .expect("dynamic capability")
        .add_child(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("child is added");
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
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    let lineage = match started.kind {
        LifecycleEventKind::ChildStarted { lineage, .. } => lineage,
        _ => unreachable!(),
    };

    running
        .handle()
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
    assert_eq!(started.seq(), added.seq().map(|seq| seq + 1));
    assert_eq!(removed.seq(), exited.seq().map(|seq| seq + 1));

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn readiness_gated_child_started_is_emitted_only_after_ready() {
    let release = Arc::new(Notify::new());
    let child_release = Arc::clone(&release);
    let builder = Supervisor::ordered().child(
        TaskSpec::new("worker", move |ctx| {
            let release = Arc::clone(&child_release);
            async move {
                release.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })
        .wait_for_ready(),
    );
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();

    timeout(Duration::from_millis(100), async {
        next_matching(&mut watch, |event| {
            matches!(
                event.kind,
                LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "worker"
            )
        })
        .await
    })
    .await
    .expect_err("ChildStarted remains gated on readiness");

    release.notify_one();
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
    assert!(started.seq().is_some());

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn cooperative_remove_publishes_removed_before_the_command_reply() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let handle = running.handle();
    let dynamic = handle.dynamic().expect("scope is dynamic");
    dynamic
        .add_child(
            TaskSpec::new("worker", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(crate::supervisor::Shutdown::drain_for(Duration::from_secs(
                1,
            ))),
        )
        .await
        .expect("worker is added");
    handle.wait_started().await.expect("worker starts");
    let mut watch = handle.watch_lifecycle().direct_children();
    let removal = tokio::spawn(async move { dynamic.remove_child("worker").await });

    let removed = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildRemoved { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    timeout(WAIT, removal)
        .await
        .expect("remove command resolves")
        .expect("remove task joins")
        .expect("remove succeeds");
    assert!(handle.snapshot().lifecycle_seq >= removed.seq().expect("removed sequence"));
    assert!(handle.snapshot().child("worker").is_none());

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restart_intensity_failure_is_an_in_band_scope_event() {
    let builder = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1)))
        .child(TaskSpec::new("always-fails", |_| async {
            Err(failure("no restart budget"))
        }));
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle();
    let running = builder.build().expect("supervisor builds").spawn();

    let intensity = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::RestartIntensityExceeded { .. }
        )
    })
    .await;
    assert!(intensity.supervisor_path.is_empty());
    assert!(intensity.total_restarts().is_some());
    assert!(matches!(
        running.wait().await,
        Err(SupervisorError::RestartIntensityExceeded)
    ));
}

#[tokio::test]
async fn shutdown_drains_in_reverse_and_the_watch_closes_after_staged_events() {
    let mut builder = Supervisor::ordered();
    for id in ["first", "second", "third"] {
        builder = builder.child(TaskSpec::new(id, |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    }
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();
    running
        .handle()
        .wait_started()
        .await
        .expect("children start");
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
    let builder = Supervisor::dynamic();
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();
    next_matching(&mut watch, |event| {
        matches!(event.kind, LifecycleEventKind::SupervisorStarted)
    })
    .await;
    let baseline = running.handle().snapshot().lifecycle_seq;

    for index in 0..70 {
        let id = format!("child-{index}");
        running
            .handle()
            .dynamic()
            .expect("dynamic capability")
            .add_child(TaskSpec::new(id.clone(), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await
            .expect("child is added");
        running
            .handle()
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
    let snapshot = running.handle().snapshot();
    assert!(snapshot.children.is_empty());
    assert!(snapshot.lifecycle_seq >= 140);
    let LifecycleEventKind::Lagged { dropped } = lagged.kind else {
        panic!("first retained event is the lag marker");
    };

    let mut suffix = Vec::new();
    while suffix.last().copied() != Some(snapshot.lifecycle_seq) {
        let event = timeout(WAIT, watch.next())
            .await
            .expect("retained suffix arrives")
            .expect("watch remains open");
        let seq = event
            .seq()
            .expect("overflow suffix contains child transitions");
        suffix.push(seq);
    }
    assert_eq!(
        dropped + suffix.len() as u64,
        snapshot.lifecycle_seq - baseline,
        "lag marker accounts for exactly the discarded transition prefix"
    );
    assert_eq!(
        suffix,
        (snapshot.lifecycle_seq - suffix.len() as u64 + 1..=snapshot.lifecycle_seq)
            .collect::<Vec<_>>(),
        "the retained suffix stays sequence-contiguous"
    );

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn nested_churn_cannot_overflow_a_direct_child_watch() {
    let nested_builder = Supervisor::dynamic();
    let nested_handle = nested_builder.handle();
    let nested = nested_builder.build().expect("nested supervisor builds");
    let running = Supervisor::dynamic()
        .build()
        .expect("root supervisor builds")
        .spawn();
    let mut direct = running.handle().watch_lifecycle().direct_children();
    let root_dynamic = running.handle().dynamic().expect("root is a dynamic scope");
    root_dynamic
        .add_child_spec(ChildSpec::supervisor("nested", nested))
        .await
        .expect("nested scope is added");
    nested_handle
        .wait_started()
        .await
        .expect("nested scope starts");
    let nested_dynamic = nested_handle.dynamic().expect("nested scope is dynamic");

    for index in 0..80 {
        let id = format!("nested-{index}");
        nested_dynamic
            .add_child(TaskSpec::new(id.clone(), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await
            .expect("nested child is added");
        nested_dynamic
            .remove_child(&id)
            .await
            .expect("nested child is removed");
    }

    root_dynamic
        .add_child(TaskSpec::new("root-peer", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("root child is added");

    let peer = timeout(WAIT, async {
        loop {
            let event = direct.next().await.expect("direct watch remains open");
            assert!(event.supervisor_path.is_empty());
            assert!(
                !matches!(event.kind, LifecycleEventKind::Lagged { .. }),
                "nested-only traffic must not consume the direct buffer"
            );
            if matches!(
                event.kind,
                LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "root-peer"
            ) {
                break event;
            }
        }
    })
    .await
    .expect("the direct root transition survives nested churn");
    assert!(peer.seq().is_some());

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_sequences_are_gap_free_under_concurrent_dynamic_churn() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let handle = running.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let baseline = handle.snapshot().lifecycle_seq;
    let dynamic = handle.dynamic().expect("scope is dynamic");
    let mut churn = tokio::task::JoinSet::new();

    for index in 0..16 {
        let dynamic = dynamic.clone();
        churn.spawn(async move {
            let id = format!("child-{index}");
            dynamic
                .add_child(TaskSpec::new(id.clone(), |ctx| async move {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }))
                .await
                .expect("child is added");
            dynamic.remove_child(id).await.expect("child is removed");
        });
    }
    while let Some(result) = churn.join_next().await {
        result.expect("churn task joins");
    }

    let final_seq = handle.snapshot().lifecycle_seq;
    let mut observed = Vec::new();
    while observed.last().copied() != Some(final_seq) {
        let event = timeout(WAIT, watch.next())
            .await
            .expect("transition arrives")
            .expect("watch remains open");
        assert!(!matches!(event.kind, LifecycleEventKind::Lagged { .. }));
        if let Some(seq) = event.seq() {
            observed.push(seq);
        }
    }
    assert_eq!(observed, (baseline + 1..=final_seq).collect::<Vec<_>>());

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn removed_nested_watch_closes_and_same_id_reinsertion_gets_a_new_path_lineage() {
    let running = Supervisor::dynamic()
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    let dynamic = root.dynamic().expect("root is dynamic");
    let mut root_watch = root.watch_lifecycle();

    let first_builder = Supervisor::ordered().child(TaskSpec::new("leaf", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let first_handle = first_builder.handle();
    let mut first_watch = first_handle.watch_lifecycle();
    dynamic
        .add_child_spec(ChildSpec::supervisor(
            "nested",
            first_builder.build().expect("first nested scope builds"),
        ))
        .await
        .expect("first nested scope is added");
    let first = next_matching(&mut root_watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "leaf"
        )
    })
    .await;
    let first_lineage = first.supervisor_path[0].lineage;

    dynamic
        .remove_child("nested")
        .await
        .expect("first nested scope is removed");
    timeout(WAIT, async { while first_watch.next().await.is_some() {} })
        .await
        .expect("removed nested identity closes its watch");

    let second_builder = Supervisor::ordered().child(TaskSpec::new("leaf", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    dynamic
        .add_child_spec(ChildSpec::supervisor(
            "nested",
            second_builder.build().expect("second nested scope builds"),
        ))
        .await
        .expect("second nested scope is added");
    let second = next_matching(&mut root_watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "leaf"
        ) && event.supervisor_path[0].lineage > first_lineage
    })
    .await;
    assert_eq!(second.supervisor_path[0].id, "nested");
    assert!(second.supervisor_path[0].lineage > first_lineage);

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn group_revivable_nested_watch_stays_open_and_resumes() {
    let complete_leaf = Arc::new(Notify::new());
    let crash_sibling = Arc::new(Notify::new());
    let leaf_complete = Arc::clone(&complete_leaf);
    let leaf_builder = Supervisor::ordered().child(
        TaskSpec::new("worker", move |_| {
            let complete = Arc::clone(&leaf_complete);
            async move {
                complete.notified().await;
                Ok(())
            }
        })
        .restart(Restart::on_failure())
        .shutdown(Shutdown::abort()),
    );
    let _leaf_finished = leaf_builder
        .handle()
        .completions(["worker"])
        .then_shutdown();
    let leaf = leaf_builder.build().expect("leaf supervisor builds");
    let sibling_crash = Arc::clone(&crash_sibling);
    let running = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child_spec(ChildSpec::supervisor("leaf", leaf).restart(Restart::on_failure()))
        .child(
            TaskSpec::new("sibling", move |_| {
                let crash = Arc::clone(&sibling_crash);
                async move {
                    crash.notified().await;
                    Err(failure("sibling boom"))
                }
            })
            .restart(Restart::on_failure())
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let leaf = root.supervisor("leaf").expect("leaf handle exists");
    let mut watch = leaf.watch_lifecycle();

    complete_leaf.notify_one();
    next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    assert_stays_open(
        &mut watch,
        "a group-revivable nested identity must remain open",
    )
    .await;

    crash_sibling.notify_one();
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
    assert_eq!(started.seq(), added.seq().map(|seq| seq + 1));

    running.shutdown_and_wait().await.expect("clean shutdown");
    wait_closed(&mut watch, "root shutdown to close revived nested watch").await;
}

#[tokio::test]
async fn never_policy_nested_stop_closes_its_watch() {
    let crash = Arc::new(Notify::new());
    let child_crash = Arc::clone(&crash);
    let nested = Supervisor::ordered()
        .child(
            TaskSpec::new("worker", move |_| {
                let crash = Arc::clone(&child_crash);
                async move {
                    crash.notified().await;
                    Err(failure("worker boom"))
                }
            })
            .restart(Restart::on_failure().limit(0, Duration::from_secs(60))),
        )
        .build()
        .expect("nested supervisor builds");
    let running = Supervisor::ordered()
        .child_spec(ChildSpec::supervisor("nested", nested).restart(Restart::never()))
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let nested = root.supervisor("nested").expect("nested handle exists");
    let mut watch = nested.watch_lifecycle();

    crash.notify_one();
    next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    wait_closed(&mut watch, "Never-policy nested identity to close").await;

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn parent_stop_closes_nested_watch_while_handle_is_retained() {
    let running = Supervisor::ordered()
        .child_spec(ChildSpec::supervisor("nested", idle_supervisor()))
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let nested = root.supervisor("nested").expect("nested handle exists");
    let mut watch = nested.watch_lifecycle();

    running.shutdown_and_wait().await.expect("clean shutdown");
    wait_closed(&mut watch, "root terminality to close descendant watch").await;
    assert_eq!(nested.snapshot().total_restarts, 0);
}

#[tokio::test]
async fn nested_watch_survives_restartable_ancestor_reincarnation() {
    let crash_leaf = Arc::new(Notify::new());
    let crash_middle = Arc::new(Notify::new());
    let running = Supervisor::ordered()
        .child_spec(
            ChildSpec::supervisor("middle", middle_supervisor(&crash_leaf, &crash_middle))
                .restart(Restart::on_failure().limit(5, Duration::from_secs(60))),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let middle = root.supervisor("middle").expect("middle handle exists");
    let leaf = middle.supervisor("leaf").expect("leaf handle exists");
    let mut watch = leaf.watch_lifecycle();

    crash_leaf.notify_one();
    let first_exit = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    assert_stays_open(
        &mut watch,
        "a restartable ancestor keeps the provisional identity open",
    )
    .await;

    crash_middle.notify_one();
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
    assert!(added.seq() > first_exit.seq());
    assert_eq!(started.seq(), added.seq().map(|seq| seq + 1));

    crash_leaf.notify_one();
    let second_exit = next_matching(&mut watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    assert!(second_exit.seq() > started.seq());
    assert_eq!(second_exit.total_restarts(), Some(1));

    running.shutdown_and_wait().await.expect("clean shutdown");
    wait_closed(
        &mut watch,
        "root shutdown to close revived descendant watch",
    )
    .await;
}

#[tokio::test]
async fn ancestor_reincarnation_closes_orphaned_dynamic_watch() {
    let crash_middle = Arc::new(Notify::new());
    let middle = Supervisor::dynamic()
        .build()
        .expect("dynamic middle supervisor builds");
    let running = Supervisor::ordered()
        .child_spec(
            ChildSpec::supervisor("middle", middle)
                .restart(Restart::on_failure().limit(5, Duration::from_secs(60))),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");

    let middle = root.supervisor("middle").expect("middle handle exists");
    let bomb_crash = Arc::clone(&crash_middle);
    let dynamic = middle.dynamic().expect("middle is dynamic");
    dynamic
        .add_child(
            TaskSpec::new("bomb", move |_| {
                let crash = Arc::clone(&bomb_crash);
                async move {
                    crash.notified().await;
                    Err(failure("middle boom"))
                }
            })
            .restart(Restart::on_failure().limit(0, Duration::from_secs(60))),
        )
        .await
        .expect("bomb is added");
    dynamic
        .add_child_spec(ChildSpec::supervisor("orphan", idle_supervisor()))
        .await
        .expect("orphan is added");
    let orphan = middle.supervisor("orphan").expect("orphan handle exists");
    let mut watch = orphan.watch_lifecycle();
    let mut middle_snapshots = middle.subscribe_snapshots();

    crash_middle.notify_one();
    wait_closed(
        &mut watch,
        "orphaned watch to close after ancestor reincarnation",
    )
    .await;
    timeout(
        WAIT,
        middle_snapshots.wait_for(|snapshot| {
            snapshot.total_restarts == 1 && snapshot.child("orphan").is_none()
        }),
    )
    .await
    .expect("replacement snapshot arrives")
    .expect("snapshot stream remains open");

    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn rest_for_one_closes_head_but_defers_tail_terminality() {
    let complete_head = Arc::new(Notify::new());
    let complete_tail = Arc::new(Notify::new());
    let (head_supervisor, _head_finished) = completing_supervisor(&complete_head);
    let (tail_supervisor, _tail_finished) = completing_supervisor(&complete_tail);
    let running = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child_spec(ChildSpec::supervisor("head", head_supervisor).restart(Restart::on_failure()))
        .child_spec(ChildSpec::supervisor("tail", tail_supervisor).restart(Restart::on_failure()))
        .build()
        .expect("root supervisor builds")
        .spawn();
    let root = running.handle();
    root.wait_started().await.expect("root starts");
    let head = root.supervisor("head").expect("head handle exists");
    let tail = root.supervisor("tail").expect("tail handle exists");
    let mut head_watch = head.watch_lifecycle();
    let mut tail_watch = tail.watch_lifecycle();

    complete_head.notify_one();
    next_matching(&mut head_watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    wait_closed(&mut head_watch, "RestForOne head watch to close").await;
    assert_eq!(
        root.snapshot().state,
        crate::supervisor::SupervisorStateView::Running
    );

    complete_tail.notify_one();
    next_matching(&mut tail_watch, |event| {
        matches!(
            event.kind,
            LifecycleEventKind::ChildExited { ref child_id, .. } if child_id == "worker"
        )
    })
    .await;
    assert_stays_open(
        &mut tail_watch,
        "the RestForOne tail identity remains provisionally revivable",
    )
    .await;

    running.shutdown_and_wait().await.expect("clean shutdown");
    wait_closed(
        &mut tail_watch,
        "root shutdown to close deferred tail watch",
    )
    .await;
}

fn idle_supervisor() -> crate::supervisor::Supervisor {
    Supervisor::ordered()
        .child(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("idle supervisor builds")
}

fn middle_supervisor(
    crash_leaf: &Arc<Notify>,
    crash_middle: &Arc<Notify>,
) -> crate::supervisor::Supervisor {
    let leaf_crash = Arc::clone(crash_leaf);
    let leaf = Supervisor::ordered()
        .child(
            TaskSpec::new("worker", move |_| {
                let crash = Arc::clone(&leaf_crash);
                async move {
                    crash.notified().await;
                    Err(failure("leaf boom"))
                }
            })
            .restart(Restart::on_failure().limit(0, Duration::from_secs(60)))
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("leaf supervisor builds");
    let middle_crash = Arc::clone(crash_middle);
    Supervisor::ordered()
        .child_spec(ChildSpec::supervisor("leaf", leaf).restart(Restart::never()))
        .child(
            TaskSpec::new("bomb", move |_| {
                let crash = Arc::clone(&middle_crash);
                async move {
                    crash.notified().await;
                    Err(failure("middle boom"))
                }
            })
            .restart(Restart::on_failure().limit(0, Duration::from_secs(60)))
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("middle supervisor builds")
}

fn completing_supervisor(complete: &Arc<Notify>) -> (crate::supervisor::Supervisor, Guard) {
    let complete = Arc::clone(complete);
    let builder = Supervisor::ordered().child(
        TaskSpec::new("worker", move |_| {
            let complete = Arc::clone(&complete);
            async move {
                complete.notified().await;
                Ok(())
            }
        })
        .restart(Restart::on_failure()),
    );
    let finished = builder.handle().completions(["worker"]).then_shutdown();
    (
        builder.build().expect("completing supervisor builds"),
        finished,
    )
}

#[tokio::test]
async fn direct_children_is_a_depth_filter_on_the_recursive_vocabulary() {
    let nested = Supervisor::ordered()
        .child(TaskSpec::new("leaf", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("nested supervisor builds");
    let builder = Supervisor::ordered().child_spec(ChildSpec::supervisor("nested", nested));
    let handle = builder.handle();
    let mut tree = handle.watch_lifecycle();
    let mut direct = handle.watch_lifecycle().direct_children();
    let running = builder.build().expect("supervisor builds").spawn();

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
