use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::supervisor::{Backoff, Restart, Supervisor, SupervisorError, TaskSpec};
use tokio::{
    sync::{Notify, mpsc},
    time::sleep,
};

use super::common;
use common::ObservedEvent;

#[tokio::test]
async fn repeated_failures_can_exceed_restart_intensity() {
    let supervisor = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(1, Duration::from_secs(1)))
        .child(TaskSpec::new("flaky", |_| async {
            Err(common::test_error("boom"))
        }))
        .build()
        .expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let err = handle
        .wait()
        .await
        .expect_err("supervisor should fail once restart intensity is exceeded");

    assert!(matches!(err, SupervisorError::RestartIntensityExceeded));
}

#[tokio::test]
async fn configured_backoff_delays_restart_attempts() {
    let supervisor = Supervisor::ordered()
        .default_restart(common::restart_with_backoff(
            1,
            Duration::from_secs(1),
            Backoff::fixed(Duration::from_millis(75)),
        ))
        .child(TaskSpec::new("flaky", |_| async {
            Err(common::test_error("boom"))
        }))
        .build()
        .expect("valid supervisor");

    let started = Instant::now();
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let err = handle
        .wait()
        .await
        .expect_err("supervisor should eventually fail once restart intensity is exceeded");

    assert!(matches!(err, SupervisorError::RestartIntensityExceeded));
    assert!(
        started.elapsed() >= Duration::from_millis(75),
        "fixed backoff should delay the first restart attempt"
    );
}

#[tokio::test]
async fn jittered_exponential_backoff_delays_restart_attempts() {
    let supervisor = Supervisor::ordered()
        .default_restart(common::restart_with_backoff(
            1,
            Duration::from_secs(1),
            Backoff::exponential_with_jitter(
                Duration::from_millis(80),
                2,
                Duration::from_millis(500),
            ),
        ))
        .child(TaskSpec::new("flaky", |_| async {
            Err(common::test_error("boom"))
        }))
        .build()
        .expect("valid supervisor");

    let started = Instant::now();
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let err = handle
        .wait()
        .await
        .expect_err("supervisor should eventually fail once restart intensity is exceeded");

    assert!(matches!(err, SupervisorError::RestartIntensityExceeded));
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "equal jitter should keep the first restart delay at least half the base value"
    );
}

#[tokio::test]
async fn exponential_backoff_delays_restart_attempts_by_expected_steps() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let running = Supervisor::ordered()
        .default_restart(common::restart_with_backoff(
            3,
            Duration::from_secs(1),
            Backoff::exponential(Duration::from_millis(40), 2, Duration::from_millis(200)),
        ))
        .child(TaskSpec::new("flaky", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send((ctx.generation(), Instant::now()))
                    .expect("test receiver dropped");
                if ctx.generation() < 2 {
                    Err(common::test_error("boom"))
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }))
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = running.handle();

    let (_, started_0) = common::recv_event(&mut starts_rx).await;
    let (_, started_1) = common::recv_event(&mut starts_rx).await;
    let (_, started_2) = common::recv_event(&mut starts_rx).await;

    assert!(
        started_1.duration_since(started_0) >= Duration::from_millis(35),
        "first exponential delay should be close to base"
    );
    assert!(
        started_2.duration_since(started_1) >= Duration::from_millis(75),
        "second exponential delay should be close to base * factor"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn backoff_attempts_survive_window_eviction_and_reset_after_a_long_run() {
    let release = Arc::new(Notify::new());
    let release_for_child = release.clone();

    let running = Supervisor::ordered()
        .default_restart(common::restart_with_backoff(
            5,
            Duration::from_millis(200),
            Backoff::exponential(Duration::from_millis(50), 4, Duration::from_secs(2)),
        ))
        .child(TaskSpec::new("flaky", move |ctx| {
            let release = release_for_child.clone();
            async move {
                match ctx.generation() {
                    0 => {
                        release.notified().await;
                        Err(common::test_error("boom"))
                    }
                    // Fast failures: by the third restart the earlier
                    // timestamps have aged out of the 200ms window, but
                    // the consecutive-restart counter must keep growing.
                    1 | 2 => Err(common::test_error("boom")),
                    // Outlive the window, then fail: this is the only
                    // thing that resets the counter.
                    3 => {
                        sleep(Duration::from_millis(300)).await;
                        Err(common::test_error("boom"))
                    }
                    _ => {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }
        }))
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = running.handle();

    let mut events = common::event_watch(&handle);
    release.notify_one();

    let mut delays = Vec::new();
    while delays.len() < 4 {
        if let ObservedEvent::ChildRestartScheduled { delay, .. } =
            common::recv_supervisor_event(&mut events).await
        {
            delays.push(delay);
        }
    }

    assert_eq!(
        delays,
        vec![
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::from_millis(800),
            Duration::from_millis(50),
        ],
        "backoff must not shrink as intensity timestamps age out, and must \
         reset after a run outlives the intensity window"
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn child_restart_intensity_override_controls_backoff() {
    let supervisor = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1)))
        .child(
            TaskSpec::new("flaky", |_| async { Err(common::test_error("boom")) }).restart(
                common::restart_with_backoff(
                    1,
                    Duration::from_secs(1),
                    Backoff::fixed(Duration::from_millis(75)),
                ),
            ),
        )
        .build()
        .expect("valid supervisor");

    let started = Instant::now();
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let err = handle
        .wait()
        .await
        .expect_err("supervisor should eventually fail once the child override is exceeded");

    assert!(matches!(err, SupervisorError::RestartIntensityExceeded));
    assert!(
        started.elapsed() >= Duration::from_millis(75),
        "child-specific backoff should delay the first restart attempt"
    );
}

#[tokio::test]
async fn restart_intensity_is_tracked_per_child_for_one_for_one() {
    let alpha_attempts = Arc::new(AtomicUsize::new(0));
    let beta_attempts = Arc::new(AtomicUsize::new(0));
    let (alpha_tx, mut alpha_rx) = mpsc::unbounded_channel();
    let (beta_tx, mut beta_rx) = mpsc::unbounded_channel();

    let alpha = TaskSpec::new("alpha", move |ctx| {
        let alpha_attempts = alpha_attempts.clone();
        let alpha_tx = alpha_tx.clone();
        async move {
            alpha_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if alpha_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("alpha boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let beta = TaskSpec::new("beta", move |ctx| {
        let beta_attempts = beta_attempts.clone();
        let beta_tx = beta_tx.clone();
        async move {
            beta_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if beta_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(common::test_error("beta boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let handle_owner = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(1, Duration::from_secs(1)))
        .child(alpha)
        .child(beta)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_n(&mut alpha_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut beta_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn child_restart_intensity_override_is_enforced() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let running = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(10, Duration::from_secs(1)))
        .child(
            TaskSpec::new("flaky", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("boom"))
                }
            })
            .restart(Restart::on_failure().limit(1, Duration::from_secs(1))),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = running.handle();

    assert_eq!(common::recv_n(&mut starts_rx, 2).await, vec![0, 1]);
    common::assert_no_event(&mut starts_rx).await;

    let err = handle
        .wait()
        .await
        .expect_err("child-specific restart limit should fail the supervisor");

    assert!(matches!(err, SupervisorError::RestartIntensityExceeded));
}

#[tokio::test]
async fn restart_budget_recovers_after_failures_age_out_of_window() {
    let release_second_failure = Arc::new(Notify::new());
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let release_second_failure_for_child = release_second_failure.clone();
    let running = Supervisor::ordered()
        .default_restart(Restart::on_failure().limit(1, Duration::from_millis(100)))
        .child(TaskSpec::new("flaky", move |ctx| {
            let release_second_failure = release_second_failure_for_child.clone();
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                match ctx.generation() {
                    0 => Err(common::test_error("first boom")),
                    1 => {
                        release_second_failure.notified().await;
                        Err(common::test_error("second boom"))
                    }
                    _ => {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }
        }))
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = running.handle();

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    assert_eq!(common::recv_event(&mut starts_rx).await, 1);

    sleep(Duration::from_millis(150)).await;
    release_second_failure.notify_one();

    assert_eq!(common::recv_event(&mut starts_rx).await, 2);
    common::assert_no_event(&mut starts_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn restart_intensity_is_tracked_per_failing_child_for_one_for_all() {
    let release_alpha = Arc::new(Notify::new());
    let release_beta = Arc::new(Notify::new());
    let (alpha_tx, mut alpha_rx) = mpsc::unbounded_channel();
    let (beta_tx, mut beta_rx) = mpsc::unbounded_channel();

    let release_alpha_for_child = release_alpha.clone();
    let alpha = TaskSpec::new("alpha", move |ctx| {
        let release_alpha = release_alpha_for_child.clone();
        let alpha_tx = alpha_tx.clone();
        async move {
            alpha_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() == 0 {
                release_alpha.notified().await;
                return Err(common::test_error("alpha boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let release_beta_for_child = release_beta.clone();
    let beta = TaskSpec::new("beta", move |ctx| {
        let release_beta = release_beta_for_child.clone();
        let beta_tx = beta_tx.clone();
        async move {
            beta_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() == 1 {
                release_beta.notified().await;
                return Err(common::test_error("beta boom"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let handle_owner = Supervisor::ordered()
        .strategy(crate::supervisor::Strategy::OneForAll)
        .default_restart(Restart::on_failure().limit(1, Duration::from_secs(1)))
        .child(alpha)
        .child(beta)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut alpha_rx).await, 0);
    assert_eq!(common::recv_event(&mut beta_rx).await, 0);

    release_alpha.notify_one();

    assert_eq!(common::recv_event(&mut alpha_rx).await, 1);
    assert_eq!(common::recv_event(&mut beta_rx).await, 1);

    release_beta.notify_one();

    assert_eq!(common::recv_event(&mut alpha_rx).await, 2);
    assert_eq!(common::recv_event(&mut beta_rx).await, 2);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}
