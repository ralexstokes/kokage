use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::{
    sync::{Barrier, Notify, mpsc},
    time::{Duration, sleep, timeout},
};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, ChildStateView, ExitStatusView, RestartIntensity, RestartPolicy,
    ShutdownMode, ShutdownPolicy, Strategy, SupervisorBuilder, SupervisorEvent,
};

mod common;

#[tokio::test]
async fn group_restart_finalizes_and_skips_a_removing_sibling() {
    let trigger_failure = Arc::new(Notify::new());
    let removing_cancelled = Arc::new(Notify::new());
    let release_removing = Arc::new(Notify::new());
    let peer_cancelled = Arc::new(Notify::new());
    let removing_attempts = Arc::new(AtomicUsize::new(0));
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();

    let trigger = ChildSpec::new("trigger", {
        let trigger_failure = Arc::clone(&trigger_failure);
        move |ctx| {
            let trigger_failure = Arc::clone(&trigger_failure);
            let trigger_tx = trigger_tx.clone();
            async move {
                trigger_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                if ctx.generation() == 0 {
                    trigger_failure.notified().await;
                    Err(common::test_error("restart group"))
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .restart(RestartPolicy::OnFailure);
    let removing = ChildSpec::new("removing", {
        let removing_cancelled = Arc::clone(&removing_cancelled);
        let release_removing = Arc::clone(&release_removing);
        let removing_attempts = Arc::clone(&removing_attempts);
        move |ctx| {
            let removing_cancelled = Arc::clone(&removing_cancelled);
            let release_removing = Arc::clone(&release_removing);
            let removing_attempts = Arc::clone(&removing_attempts);
            async move {
                removing_attempts.fetch_add(1, Ordering::SeqCst);
                ctx.shutdown_token().cancelled().await;
                removing_cancelled.notify_one();
                release_removing.notified().await;
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::new(
        Duration::from_secs(1),
        ShutdownMode::CooperativeStrict,
    ));
    let peer = ChildSpec::new("peer", {
        let peer_cancelled = Arc::clone(&peer_cancelled);
        move |ctx| {
            let peer_cancelled = Arc::clone(&peer_cancelled);
            async move {
                ctx.shutdown_token().cancelled().await;
                peer_cancelled.notify_one();
                Ok(())
            }
        }
    });
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(removing)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removing").await });
    removing_cancelled.notified().await;
    trigger_failure.notify_one();
    timeout(Duration::from_secs(1), peer_cancelled.notified())
        .await
        .expect("group drain should include the active sibling");
    release_removing.notify_one();

    timeout(Duration::from_secs(1), remove_task)
        .await
        .expect("group drain should resolve pending removal")
        .expect("remove task should join")
        .expect("removal completed within its grace");
    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);
    assert_eq!(removing_attempts.load(Ordering::SeqCst), 1);
    assert!(handle.snapshot().child("removing").is_none());

    handle.shutdown_and_wait().await.expect("shutdown succeeds");
}

#[tokio::test]
async fn group_drain_reports_a_removals_own_expired_strict_grace() {
    let trigger_failure = Arc::new(Notify::new());
    let removing_cancelled = Arc::new(Notify::new());
    let peer_cancelled = Arc::new(Notify::new());
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();

    let trigger = ChildSpec::new("trigger", {
        let trigger_failure = Arc::clone(&trigger_failure);
        move |ctx| {
            let trigger_failure = Arc::clone(&trigger_failure);
            let trigger_tx = trigger_tx.clone();
            async move {
                trigger_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                if ctx.generation() == 0 {
                    trigger_failure.notified().await;
                    Err(common::test_error("restart group"))
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .restart(RestartPolicy::OnFailure);
    let removing = ChildSpec::new("removing", {
        let removing_cancelled = Arc::clone(&removing_cancelled);
        move |ctx| {
            let removing_cancelled = Arc::clone(&removing_cancelled);
            async move {
                ctx.shutdown_token().cancelled().await;
                removing_cancelled.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::new(
        Duration::from_millis(50),
        ShutdownMode::CooperativeStrict,
    ));
    let peer = ChildSpec::new("peer", {
        let peer_cancelled = Arc::clone(&peer_cancelled);
        move |ctx| {
            let peer_cancelled = Arc::clone(&peer_cancelled);
            async move {
                ctx.shutdown_token().cancelled().await;
                if ctx.generation() == 0 {
                    peer_cancelled.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::new(
        Duration::from_millis(150),
        ShutdownMode::CooperativeThenAbort,
    ));
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(removing)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removing").await });
    removing_cancelled.notified().await;
    trigger_failure.notify_one();
    timeout(Duration::from_secs(1), peer_cancelled.notified())
        .await
        .expect("group drain should start before the individual grace expires");

    let error = timeout(Duration::from_secs(1), remove_task)
        .await
        .expect("group drain should finalize the removal")
        .expect("remove task should join")
        .expect_err("the removal's strict grace expired during the group drain");
    assert_eq!(
        error,
        tokio_supervisor::ControlError::ShutdownTimedOut("removing".to_owned())
    );
    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);

    handle.shutdown_and_wait().await.expect("shutdown succeeds");
}

#[tokio::test]
async fn restartable_child_failure_restarts_the_whole_group() {
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let trigger = ChildSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut trigger_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn control_plane_remains_available_after_group_restart() {
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let (dynamic_tx, mut dynamic_rx) = mpsc::unbounded_channel();
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let trigger = ChildSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut trigger_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle
        .add_child(ChildSpec::new("dynamic", move |ctx| {
            let dynamic_tx = dynamic_tx.clone();
            async move {
                dynamic_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .await
        .expect("control plane should remain available after group restart");

    assert_eq!(common::recv_event(&mut dynamic_rx).await, 0);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn completed_temporary_child_is_not_respawned_during_group_restart() {
    let release_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let (temporary_tx, mut temporary_rx) = mpsc::unbounded_channel();
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let temporary = ChildSpec::new("temporary", move |ctx| {
        let temporary_tx = temporary_tx.clone();
        async move {
            temporary_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            Ok(())
        }
    })
    .restart(RestartPolicy::Never);

    let release_failure_for_child = release_failure.clone();
    let trigger = ChildSpec::new("trigger", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(temporary)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_event(&mut temporary_rx).await, 0);
    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);
    assert_eq!(common::recv_event(&mut peer_rx).await, 0);

    release_failure.notify_one();

    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);
    assert_eq!(common::recv_event(&mut peer_rx).await, 1);
    common::assert_no_event(&mut temporary_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn one_for_all_does_not_overlap_old_and_new_generations() {
    let live_instances = Arc::new(AtomicUsize::new(0));
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let trigger = ChildSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", move |ctx| {
        let live_instances = live_instances.clone();
        let peer_tx = peer_tx.clone();
        async move {
            let active = live_instances.fetch_add(1, Ordering::SeqCst) + 1;
            peer_tx
                .send((ctx.generation(), active))
                .expect("test receiver dropped");

            ctx.shutdown_token().cancelled().await;
            live_instances.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);
    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);

    let peer_events = common::recv_n(&mut peer_rx, 2).await;
    assert_eq!(peer_events, vec![(0, 1), (1, 1)]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn one_for_all_restarts_after_aborting_stubborn_cooperative_then_abort_peer() {
    let release_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let peer_live_flag = common::LiveFlag::new();

    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let release_failure_for_child = release_failure.clone();
    let trigger = ChildSpec::new("trigger", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let trigger_attempts = trigger_attempts.clone();
        async move {
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .shutdown(ShutdownPolicy::new(
        common::SHORT_GRACE,
        ShutdownMode::CooperativeThenAbort,
    ));

    let peer_live_flag_for_child = peer_live_flag.clone();
    let peer = ChildSpec::new("stubborn-peer", move |ctx| {
        let peer_live_flag = peer_live_flag_for_child.clone();
        let peer_tx = peer_tx.clone();
        async move {
            let _guard = peer_live_flag.guard();
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            std::future::pending::<()>().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always)
    .shutdown(ShutdownPolicy::new(
        common::SHORT_GRACE,
        ShutdownMode::CooperativeThenAbort,
    ));

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_event(&mut peer_rx).await, 0);
    release_failure.notify_one();
    assert_eq!(common::recv_event(&mut peer_rx).await, 1);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
    assert!(
        !peer_live_flag.is_live(),
        "stubborn peer should be dropped before wait resolves"
    );
}

#[tokio::test]
async fn superseded_group_failure_leaves_latest_child_exits_completed() {
    let release_failure = Arc::new(Notify::new());
    let finish_generation_one = Arc::new(Barrier::new(3));
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let peer_attempts = Arc::new(AtomicUsize::new(0));

    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let release_failure_for_child = release_failure.clone();
    let finish_generation_one_for_trigger = finish_generation_one.clone();
    let trigger = ChildSpec::new("trigger", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let finish_generation_one = finish_generation_one_for_trigger.clone();
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart group"));
            }

            finish_generation_one.wait().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let finish_generation_one_for_peer = finish_generation_one.clone();
    let peer = ChildSpec::new("peer", move |ctx| {
        let finish_generation_one = finish_generation_one_for_peer.clone();
        let peer_attempts = peer_attempts.clone();
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if peer_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ctx.shutdown_token().cancelled().await;
                return Err(common::test_error("drained old generation"));
            }

            finish_generation_one.wait().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);
    assert_eq!(common::recv_event(&mut peer_rx).await, 0);

    release_failure.notify_one();

    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);
    assert_eq!(common::recv_event(&mut peer_rx).await, 1);

    finish_generation_one.wait().await;

    let mut snapshots = handle.subscribe_snapshots();
    let completed = timeout(
        Duration::from_secs(1),
        snapshots.wait_for(|snapshot| {
            snapshot
                .children
                .iter()
                .all(|child| child.state == ChildStateView::Stopped)
        }),
    )
    .await
    .expect("children should stop after generation one completes")
    .expect("snapshot stream should remain open")
    .clone();
    assert!(
        completed
            .children
            .iter()
            .all(|child| { matches!(child.last_exit.as_ref(), Some(ExitStatusView::Completed)) })
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn group_restart_uses_the_failing_child_restart_intensity() {
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let trigger = ChildSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        let trigger_tx = trigger_tx.clone();
        async move {
            trigger_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(RestartIntensity::new(1, Duration::from_secs(1)));

    let peer = ChildSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always)
    .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)));

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut trigger_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn triggering_child_restart_scheduled_precedes_child_restart_events() {
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let trigger = ChildSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        async move {
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartPolicy::Always);

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .restart_intensity(
            RestartIntensity::new(2, Duration::from_secs(1))
                .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(40))),
        )
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut events = handle.subscribe();

    let mut sequence = Vec::new();
    let mut saw_trigger_restart = false;
    let mut saw_peer_restart = false;

    while !(saw_trigger_restart && saw_peer_restart) {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::ChildExited { id, generation, .. }
                if id == "trigger" && generation == 0 =>
            {
                sequence.push("trigger_exited");
            }
            SupervisorEvent::ChildRestartScheduled {
                id,
                generation,
                delay,
                ..
            } if id == "trigger" && generation == 0 => {
                assert_eq!(delay, Duration::from_millis(40));
                sequence.push("trigger_restart_scheduled");
            }
            SupervisorEvent::ChildStarted { id, generation, .. }
                if id == "trigger" && generation == 1 =>
            {
                sequence.push("trigger_started");
            }
            SupervisorEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "trigger" && old_generation == 0 && new_generation == 1 => {
                saw_trigger_restart = true;
                sequence.push("trigger_restarted");
            }
            SupervisorEvent::ChildStarted { id, generation, .. }
                if id == "peer" && generation == 1 =>
            {
                sequence.push("peer_started");
            }
            SupervisorEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "peer" && old_generation == 0 && new_generation == 1 => {
                saw_peer_restart = true;
                sequence.push("peer_restarted");
            }
            _ => {}
        }
    }

    let trigger_scheduled = sequence
        .iter()
        .position(|event| *event == "trigger_restart_scheduled")
        .expect("triggering child restart should be scheduled");
    let trigger_exited = sequence
        .iter()
        .position(|event| *event == "trigger_exited")
        .expect("trigger exit should be observed");
    let trigger_started = sequence
        .iter()
        .position(|event| *event == "trigger_started")
        .expect("trigger restart start should be observed");
    let trigger_restarted = sequence
        .iter()
        .position(|event| *event == "trigger_restarted")
        .expect("trigger restart should be observed");
    let peer_started = sequence
        .iter()
        .position(|event| *event == "peer_started")
        .expect("peer restart start should be observed");
    let peer_restarted = sequence
        .iter()
        .position(|event| *event == "peer_restarted")
        .expect("peer restart should be observed");

    assert!(
        trigger_exited < trigger_scheduled,
        "failing child exit must precede restart scheduling: {sequence:?}"
    );
    assert!(
        trigger_scheduled < trigger_restarted && trigger_scheduled < peer_restarted,
        "trigger restart must be scheduled before any child restart completes: {sequence:?}"
    );
    assert!(
        trigger_started < trigger_restarted,
        "trigger restart event ordering regressed: {sequence:?}"
    );
    assert!(
        peer_started < peer_restarted,
        "peer restart event ordering regressed: {sequence:?}"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn removing_failed_child_abandons_pending_group_restart() {
    let (replacement_tx, mut replacement_rx) = mpsc::unbounded_channel();
    let backoff = Duration::from_millis(80);

    let trigger = ChildSpec::new("trigger", |_ctx| async move {
        Err(common::test_error("restart group later"))
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartPolicy::Always);

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .restart_intensity(
            RestartIntensity::new(2, Duration::from_secs(1))
                .with_backoff(BackoffPolicy::Fixed(backoff)),
        )
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut events = handle.subscribe();

    loop {
        if matches!(
            common::recv_supervisor_event(&mut events).await,
            SupervisorEvent::ChildRestartScheduled { ref id, .. } if id == "trigger"
        ) {
            break;
        }
    }

    handle
        .remove_child("trigger")
        .await
        .expect("failed child removal should succeed during group backoff");
    handle
        .add_child(ChildSpec::new("replacement", move |ctx| {
            let replacement_tx = replacement_tx.clone();
            async move {
                replacement_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .await
        .expect("replacement child should be accepted");

    assert_eq!(common::recv_event(&mut replacement_rx).await, 0);
    sleep(backoff + common::QUIET_TIMEOUT).await;
    common::assert_no_event(&mut replacement_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn rapid_failures_during_group_restart_do_not_schedule_a_second_group_restart() {
    let release_trigger_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let peer_attempts = Arc::new(AtomicUsize::new(0));

    let release_trigger_failure_for_child = release_trigger_failure.clone();
    let trigger = ChildSpec::new("trigger", move |ctx| {
        let release_trigger_failure = release_trigger_failure_for_child.clone();
        let trigger_attempts = trigger_attempts.clone();
        async move {
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_trigger_failure.notified().await;
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::new("peer", move |ctx| {
        let peer_attempts = peer_attempts.clone();
        async move {
            if peer_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ctx.shutdown_token().cancelled().await;
                return Err(common::test_error(
                    "peer failed while group restart drained",
                ));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .restart_intensity(
            RestartIntensity::new(2, Duration::from_secs(1))
                .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(40))),
        )
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut events = handle.subscribe();

    release_trigger_failure.notify_one();

    let mut trigger_restart_scheduled = 0usize;
    let mut saw_trigger_restart = false;
    let mut saw_peer_restart = false;

    while !(saw_trigger_restart && saw_peer_restart) {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::ChildRestartScheduled { id, .. } if id == "trigger" => {
                trigger_restart_scheduled += 1;
            }
            SupervisorEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "trigger" && old_generation == 0 && new_generation == 1 => {
                saw_trigger_restart = true;
            }
            SupervisorEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "peer" && old_generation == 0 && new_generation == 1 => {
                saw_peer_restart = true;
            }
            _ => {}
        }
    }

    assert_eq!(
        trigger_restart_scheduled, 1,
        "drained failures should not schedule an additional group restart"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}
