use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::{
    sync::{Notify, mpsc},
    time::{Duration, sleep, timeout},
};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, ChildStateView, ExitStatusView, RestartIntensity, RestartPolicy,
    Strategy, Supervisor,
};

mod common;
use common::ObservedEvent;

#[tokio::test]
async fn sibling_restart_dispatches_during_another_childs_backoff() {
    let slow_failure = Arc::new(Notify::new());
    let fast_failure = Arc::new(Notify::new());
    let (slow_tx, mut slow_rx) = mpsc::unbounded_channel();
    let (fast_tx, mut fast_rx) = mpsc::unbounded_channel();

    let slow = ChildSpec::task("slow", {
        let slow_failure = Arc::clone(&slow_failure);
        move |ctx| {
            let slow_failure = Arc::clone(&slow_failure);
            let slow_tx = slow_tx.clone();
            async move {
                slow_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                if ctx.generation() == 0 {
                    slow_failure.notified().await;
                    Err(common::test_error("slow restart"))
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(
        RestartIntensity::new(4, Duration::from_secs(2))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_secs(30))),
    );
    let fast = ChildSpec::task("fast", {
        let fast_failure = Arc::clone(&fast_failure);
        move |ctx| {
            let fast_failure = Arc::clone(&fast_failure);
            let fast_tx = fast_tx.clone();
            async move {
                fast_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                if ctx.generation() == 0 {
                    fast_failure.notified().await;
                    Err(common::test_error("fast restart"))
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .restart(RestartPolicy::OnFailure);
    let handle = Supervisor::ordered()
        .child(slow)
        .child(fast)
        .build()
        .expect("valid supervisor")
        .spawn();
    assert_eq!(common::recv_event(&mut slow_rx).await, 0);
    assert_eq!(common::recv_event(&mut fast_rx).await, 0);
    let mut events = common::event_watch(&handle);

    slow_failure.notify_one();
    loop {
        if matches!(
            common::recv_supervisor_event(&mut events).await,
            ObservedEvent::ChildRestartScheduled { ref id, delay, .. }
                if id == "slow" && delay == Duration::from_secs(30)
        ) {
            break;
        }
    }

    fast_failure.notify_one();
    assert_eq!(common::recv_event(&mut fast_rx).await, 1);
    match slow_rx.try_recv() {
        Err(mpsc::error::TryRecvError::Empty) => {}
        Err(mpsc::error::TryRecvError::Disconnected) => {
            panic!("slow start channel closed during backoff")
        }
        Ok(generation) => panic!("slow child restarted early at generation {generation}"),
    }

    common::shutdown_and_wait(&handle, "sibling restart test shutdown")
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn failed_transient_child_restarts_and_sibling_keeps_running() {
    let (flaky_tx, mut flaky_rx) = mpsc::unbounded_channel();
    let (sibling_tx, mut sibling_rx) = mpsc::unbounded_channel();
    let sibling_ticks = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));

    let flaky_attempts = attempts.clone();
    let flaky = ChildSpec::task("flaky", move |ctx| {
        let flaky_attempts = flaky_attempts.clone();
        let flaky_tx = flaky_tx.clone();
        async move {
            flaky_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if flaky_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let sibling_ticks_for_child = sibling_ticks.clone();
    let sibling = ChildSpec::task("sibling", move |ctx| {
        let sibling_ticks_for_child = sibling_ticks_for_child.clone();
        let sibling_tx = sibling_tx.clone();
        async move {
            sibling_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => return Ok(()),
                    _ = sleep(Duration::from_millis(10)) => {
                        sibling_ticks_for_child.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
    });

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForOne)
        .child(flaky)
        .child(sibling)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut flaky_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_event(&mut sibling_rx).await, 0);
    common::assert_no_event(&mut sibling_rx).await;

    timeout(Duration::from_secs(1), async {
        while sibling_ticks.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sibling should keep running while flaky child restarts");

    handle.shutdown();
    common::wait(&handle, "failed transient child test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn permanent_child_restarts_after_completion() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let child = ChildSpec::task("permanent", move |ctx| {
        let attempts = attempts.clone();
        let starts_tx = starts_tx.clone();
        async move {
            starts_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(());
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = Supervisor::ordered()
        .child(child)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut starts_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    common::wait(&handle, "permanent child test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn temporary_child_does_not_restart() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("temporary", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("no restart"))
                }
            })
            .restart(RestartPolicy::Never),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut snapshots = handle.subscribe_snapshots();

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    let stopped = timeout(
        Duration::from_secs(1),
        snapshots.wait_for(|snapshot| {
            snapshot
                .child("temporary")
                .is_some_and(|child| child.state == ChildStateView::Stopped)
        }),
    )
    .await
    .expect("temporary child should stop")
    .expect("snapshot stream should remain open")
    .clone();
    assert!(matches!(
        stopped
            .child("temporary")
            .expect("temporary child remains visible")
            .last_exit
            .as_ref(),
        Some(ExitStatusView::Failed(message)) if message.contains("no restart")
    ));

    common::assert_no_event(&mut starts_rx).await;
    handle.shutdown();
    common::wait(&handle, "temporary child test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn child_restart_intensity_is_isolated_per_child() {
    let child_restart_intensity = RestartIntensity::new(1, Duration::from_secs(1));

    let (child_a_tx, mut child_a_rx) = mpsc::unbounded_channel();
    let (child_b_tx, mut child_b_rx) = mpsc::unbounded_channel();
    let child_a_attempts = Arc::new(AtomicUsize::new(0));
    let child_b_attempts = Arc::new(AtomicUsize::new(0));

    let child_a = ChildSpec::task("child-a", move |ctx| {
        let child_a_attempts = child_a_attempts.clone();
        let child_a_tx = child_a_tx.clone();
        async move {
            child_a_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if child_a_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("boom-a"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(child_restart_intensity);

    let child_b = ChildSpec::task("child-b", move |ctx| {
        let child_b_attempts = child_b_attempts.clone();
        let child_b_tx = child_b_tx.clone();
        async move {
            child_b_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if child_b_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("boom-b"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(child_restart_intensity);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForOne)
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .child(child_a)
        .child(child_b)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut child_a_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut child_b_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    common::wait(&handle, "restart event ordering test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn restart_events_follow_exit_schedule_start_restart_order() {
    let attempts = Arc::new(AtomicUsize::new(0));

    let handle = Supervisor::ordered()
        .restart_intensity(
            RestartIntensity::new(2, Duration::from_secs(1))
                .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(40))),
        )
        .child(
            ChildSpec::task("flaky", move |ctx| {
                let attempts = attempts.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(common::test_error("boom"))
                    } else {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut events = common::event_watch(&handle);

    let mut sequence = Vec::new();
    let mut saw_restart = false;

    while !saw_restart {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, generation, .. }
                if id == "flaky" && generation == 0 =>
            {
                sequence.push("exited");
            }
            ObservedEvent::ChildRestartScheduled {
                id,
                generation,
                delay,
                ..
            } if id == "flaky" && generation == 0 => {
                assert_eq!(delay, Duration::from_millis(40));
                sequence.push("scheduled");
            }
            ObservedEvent::ChildStarted { id, generation, .. }
                if id == "flaky" && generation == 1 =>
            {
                sequence.push("started");
            }
            ObservedEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
                ..
            } if id == "flaky" && old_generation == 0 && new_generation == 1 => {
                saw_restart = true;
                sequence.push("restarted");
            }
            _ => {}
        }
    }

    assert_eq!(
        sequence,
        vec!["exited", "scheduled", "started", "restarted"]
    );

    handle.shutdown();
    common::wait(&handle, "restart intensity test shutdown")
        .await
        .expect("shutdown should succeed");
}
