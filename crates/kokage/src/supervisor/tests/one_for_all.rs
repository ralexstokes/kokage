use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::supervisor::{
    Backoff, RestartMode, RestartPolicy, Shutdown, Strategy, Supervisor, TaskSpec,
};
use tokio::{
    sync::{Barrier, Notify, mpsc},
    time::{Duration, timeout},
};

use super::common;
use common::ObservedEvent;

#[tokio::test]
async fn group_restart_drains_in_reverse_then_respawns_through_readiness_gates() {
    let fail = Arc::new(Notify::new());
    let release_tail = Arc::new(Notify::new());
    let release_middle = Arc::new(Notify::new());
    let release_middle_ready = Arc::new(Notify::new());
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let (middle_started_tx, mut middle_started_rx) = mpsc::unbounded_channel();
    let (tail_started_tx, mut tail_started_rx) = mpsc::unbounded_channel();

    let trigger_fail = Arc::clone(&fail);
    let trigger = TaskSpec::new("trigger", move |ctx| {
        let fail = Arc::clone(&trigger_fail);
        async move {
            if ctx.generation() == 0 {
                fail.notified().await;
                Err(common::test_error("restart group"))
            } else {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    });
    let middle_cancelled_tx = cancelled_tx.clone();
    let middle_release = Arc::clone(&release_middle);
    let middle_ready = Arc::clone(&release_middle_ready);
    let middle = TaskSpec::new("middle", move |ctx| {
        let cancelled_tx = middle_cancelled_tx.clone();
        let release_middle = Arc::clone(&middle_release);
        let release_middle_ready = Arc::clone(&middle_ready);
        let middle_started_tx = middle_started_tx.clone();
        async move {
            middle_started_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() > 0 {
                release_middle_ready.notified().await;
            }
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send("middle").expect("test receiver dropped");
            release_middle.notified().await;
            Ok(())
        }
    })
    .wait_for_ready();
    let tail_release = Arc::clone(&release_tail);
    let tail = TaskSpec::new("tail", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        let release_tail = Arc::clone(&tail_release);
        let tail_started_tx = tail_started_tx.clone();
        async move {
            tail_started_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.mark_ready();
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send("tail").expect("test receiver dropped");
            release_tail.notified().await;
            Ok(())
        }
    })
    .wait_for_ready();
    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(middle)
        .child(tail)
        .build()
        .expect("ordered group builds")
        .spawn();
    let handle = handle_owner.handle();
    handle
        .wait_started()
        .await
        .expect("initial sequence starts");
    assert_eq!(common::recv_event(&mut middle_started_rx).await, 0);
    assert_eq!(common::recv_event(&mut tail_started_rx).await, 0);

    fail.notify_one();
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "tail");
    assert!(
        timeout(common::QUIET_TIMEOUT, cancelled_rx.recv())
            .await
            .is_err()
    );
    release_tail.notify_one();
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "middle");
    release_middle.notify_one();

    assert_eq!(common::recv_event(&mut middle_started_rx).await, 1);
    assert!(
        timeout(common::QUIET_TIMEOUT, tail_started_rx.recv())
            .await
            .is_err()
    );
    release_middle_ready.notify_one();
    assert_eq!(common::recv_event(&mut tail_started_rx).await, 1);

    handle.shutdown();
    release_tail.notify_one();
    release_middle.notify_one();
    handle.wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn restartable_child_failure_restarts_the_whole_group() {
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    });

    let peer = TaskSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartMode::Always);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_n(&mut trigger_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

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

    let temporary = TaskSpec::new("temporary", move |ctx| {
        let temporary_tx = temporary_tx.clone();
        async move {
            temporary_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            Ok(())
        }
    })
    .restart(RestartMode::Never);

    let release_failure_for_child = release_failure.clone();
    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let peer = TaskSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartMode::Always);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(temporary)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

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

    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let peer = TaskSpec::new("peer", move |ctx| {
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
    .restart(RestartMode::Always);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut trigger_rx).await, 0);
    assert_eq!(common::recv_event(&mut trigger_rx).await, 1);

    let peer_events = common::recv_n(&mut peer_rx, 2).await;
    assert_eq!(peer_events, vec![(0, 1), (1, 1)]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn one_for_all_escalates_a_stubborn_cooperative_peer_and_restarts() {
    let release_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let peer_live_flag = common::LiveFlag::new();

    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let release_failure_for_child = release_failure.clone();
    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure)
    .shutdown(Shutdown::graceful_for(common::SHORT_GRACE));

    let peer_live_flag_for_child = peer_live_flag.clone();
    let peer = TaskSpec::new("stubborn-peer", move |ctx| {
        let peer_live_flag = peer_live_flag_for_child.clone();
        let peer_tx = peer_tx.clone();
        async move {
            let _guard = peer_live_flag.guard();
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() == 0 {
                std::future::pending::<()>().await;
            } else {
                ctx.shutdown_token().cancelled().await;
            }
            Ok(())
        }
    })
    .restart(RestartMode::Always)
    .shutdown(Shutdown::graceful_for(common::SHORT_GRACE));

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut peer_rx).await, 0);
    release_failure.notify_one();
    assert_eq!(common::recv_event(&mut peer_rx).await, 1);
    assert!(
        peer_live_flag.is_live(),
        "the replacement starts only after the stubborn generation is dropped"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

/// A cooperative grace expiry must not skip reconciliation of an unrelated
/// abort-mode straggler. `Shutdown::abort()` promises an abort, not
/// preemption of a non-yielding future, so a child whose next poll boundary is
/// past the ordered drain's cursor window is reconciled against the drain
/// group's longest grace, exactly as it would be in a dynamic scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_restart_survives_an_abort_mode_child_that_joins_late() {
    let release_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let cooperative = TaskSpec::new("stubborn-cooperative", |ctx| async move {
        if ctx.generation() == 0 {
            std::future::pending::<()>().await;
        } else {
            ctx.shutdown_token().cancelled().await;
        }
        Ok(())
    })
    .restart(RestartMode::Always)
    .shutdown(Shutdown::graceful_for(Duration::from_millis(200)));

    let release_failure_for_child = release_failure.clone();
    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    // Blocks between polls, so its abort cannot land inside the cursor window.
    let late_peer = TaskSpec::new("late-abort-peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            loop {
                std::thread::sleep(Duration::from_millis(50));
                tokio::task::yield_now().await;
            }
        }
    })
    .restart(RestartMode::Always)
    .shutdown(Shutdown::abort());

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(cooperative)
        .child(trigger)
        .child(late_peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut peer_rx).await, 0);
    release_failure.notify_one();
    assert_eq!(
        common::recv_event(&mut peer_rx).await,
        1,
        "the group restart should complete rather than fail the supervisor"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
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
    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let finish_generation_one_for_peer = finish_generation_one.clone();
    let peer = TaskSpec::new("peer", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

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
                .all(|child| child.state.is_terminal())
        }),
    )
    .await
    .expect("children should stop after generation one completes")
    .expect("snapshot stream should remain open")
    .clone();
    assert!(completed.children.iter().all(|child| {
        child
            .state
            .last_exit()
            .is_some_and(|exit| exit.is_completed())
    }));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn group_restart_uses_the_failing_child_restart_intensity() {
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (trigger_tx, mut trigger_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();

    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart_policy(RestartPolicy::on_failure().limit(1, Duration::from_secs(1)));

    let peer = TaskSpec::new("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart_policy(RestartPolicy::always().limit(0, Duration::from_secs(1)));

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .default_restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(1)))
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_n(&mut trigger_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn triggering_child_restart_scheduled_precedes_child_restart_events() {
    let trigger_attempts = Arc::new(AtomicUsize::new(0));

    let trigger = TaskSpec::new("trigger", move |ctx| {
        let trigger_attempts = trigger_attempts.clone();
        async move {
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let peer = TaskSpec::new("peer", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartMode::Always);

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .default_restart(common::restart_with_backoff(
            2,
            Duration::from_secs(1),
            Backoff::fixed(Duration::from_millis(40)),
        ))
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    let mut sequence = Vec::new();
    let mut saw_trigger_restart = false;
    let mut saw_peer_restart = false;

    while !(saw_trigger_restart && saw_peer_restart) {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, generation, .. }
                if id == "trigger" && generation == 0 =>
            {
                sequence.push("trigger_exited");
            }
            ObservedEvent::ChildRestartScheduled {
                id,
                generation,
                delay,
                ..
            } if id == "trigger" && generation == 0 => {
                assert_eq!(delay, Duration::from_millis(40));
                sequence.push("trigger_restart_scheduled");
            }
            ObservedEvent::ChildStarted { id, generation, .. }
                if id == "trigger" && generation == 1 =>
            {
                sequence.push("trigger_started");
            }
            ObservedEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "trigger" && old_generation == 0 && new_generation == 1 => {
                saw_trigger_restart = true;
                sequence.push("trigger_restarted");
            }
            ObservedEvent::ChildStarted { id, generation, .. }
                if id == "peer" && generation == 1 =>
            {
                sequence.push("peer_started");
            }
            ObservedEvent::ChildRestarted {
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
async fn rapid_failures_during_group_restart_do_not_schedule_a_second_group_restart() {
    let release_trigger_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let peer_attempts = Arc::new(AtomicUsize::new(0));

    let release_trigger_failure_for_child = release_trigger_failure.clone();
    let trigger = TaskSpec::new("trigger", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let peer = TaskSpec::new("peer", move |ctx| {
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
    .restart(RestartMode::OnFailure);

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .default_restart(common::restart_with_backoff(
            2,
            Duration::from_secs(1),
            Backoff::fixed(Duration::from_millis(40)),
        ))
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    release_trigger_failure.notify_one();

    let mut trigger_restart_scheduled = 0usize;
    let mut saw_trigger_restart = false;
    let mut saw_peer_restart = false;

    while !(saw_trigger_restart && saw_peer_restart) {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestartScheduled { id, .. } if id == "trigger" => {
                trigger_restart_scheduled += 1;
            }
            ObservedEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "trigger" && old_generation == 0 && new_generation == 1 => {
                saw_trigger_restart = true;
            }
            ObservedEvent::ChildRestarted {
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
