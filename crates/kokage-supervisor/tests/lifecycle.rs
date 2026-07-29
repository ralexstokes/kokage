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
use tokio::{sync::Notify, time::timeout};

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
    let restart = RestartConfig::new(3, Duration::from_secs(1))
        .backoff(BackoffPolicy::Fixed(Duration::from_millis(1)));
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
    let mut watch = running.handle().watch_lifecycle().direct_children();
    running
        .handle()
        .dynamic()
        .expect("dynamic capability")
        .add_child(ChildSpec::task("worker", |ctx| async move {
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
        ChildSpec::task("worker", move |ctx| {
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
    let running = builder.spawn().expect("supervisor spawns");

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
    let running = Supervisor::dynamic().spawn().expect("supervisor spawns");
    let handle = running.handle();
    let dynamic = handle.dynamic().expect("scope is dynamic");
    dynamic
        .add_child(
            ChildSpec::task("worker", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(kokage_supervisor::ShutdownPolicy::Cooperative {
                grace: Duration::from_secs(1),
            }),
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
        builder = builder.child(ChildSpec::task(id, |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    }
    let handle = builder.handle();
    let mut watch = handle.watch_lifecycle().direct_children();
    let running = builder.spawn().expect("supervisor spawns");
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
    let running = builder.spawn().expect("supervisor spawns");
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
            .add_child(ChildSpec::task(id.clone(), |ctx| async move {
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
        .spawn()
        .expect("root supervisor spawns");
    let mut direct = running.handle().watch_lifecycle().direct_children();
    let root_dynamic = running.handle().dynamic().expect("root is a dynamic scope");
    root_dynamic
        .add_child(ChildSpec::supervisor("nested", nested))
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
            .add_child(ChildSpec::task(id.clone(), |ctx| async move {
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
        .add_child(ChildSpec::task("root-peer", |ctx| async move {
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
    let running = Supervisor::dynamic().spawn().expect("supervisor spawns");
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
                .add_child(ChildSpec::task(id.clone(), |ctx| async move {
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
        .spawn()
        .expect("root supervisor spawns");
    let root = running.handle();
    let dynamic = root.dynamic().expect("root is dynamic");
    let mut root_watch = root.watch_lifecycle();

    let first_builder = Supervisor::ordered().child(ChildSpec::task("leaf", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let first_handle = first_builder.handle();
    let mut first_watch = first_handle.watch_lifecycle();
    dynamic
        .add_child(ChildSpec::supervisor(
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

    let second_builder = Supervisor::ordered().child(ChildSpec::task("leaf", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    dynamic
        .add_child(ChildSpec::supervisor(
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
