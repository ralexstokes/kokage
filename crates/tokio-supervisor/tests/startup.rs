use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, RestartIntensity, RestartPolicy, StartMode, Strategy,
    SupervisorBuilder, SupervisorSpec,
};

#[tokio::test]
async fn sequential_start_waits_for_explicit_readiness() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());

    let first = ChildSpec::new("first", {
        let order = Arc::clone(&order);
        let release = Arc::clone(&release);
        move |ctx| {
            let order = Arc::clone(&order);
            let release = Arc::clone(&release);
            async move {
                order.lock().await.push("first");
                release.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let second = ChildSpec::new("second", {
        let order = Arc::clone(&order);
        move |ctx| {
            let order = Arc::clone(&order);
            async move {
                order.lock().await.push("second");
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();

    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(&*order.lock().await, &["first"]);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*order.lock().await, &["first", "second"]);
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn concurrent_start_does_not_wait_for_readiness() {
    let second_started = Arc::new(Notify::new());
    let first_release = Arc::new(Notify::new());

    let first = ChildSpec::new("first", {
        let first_release = Arc::clone(&first_release);
        move |ctx| {
            let first_release = Arc::clone(&first_release);
            async move {
                first_release.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let second = ChildSpec::new("second", {
        let second_started = Arc::clone(&second_started);
        move |ctx| {
            let second_started = Arc::clone(&second_started);
            async move {
                second_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();

    let handle = SupervisorBuilder::new()
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();

    tokio::time::timeout(Duration::from_secs(1), second_started.notified())
        .await
        .unwrap();
    first_release.notify_one();
    handle.wait_started().await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn one_for_all_restart_preserves_sequential_readiness_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(Notify::new());
    let release_restart = Arc::new(Notify::new());

    let first = ChildSpec::new("first", {
        let order = Arc::clone(&order);
        let fail = Arc::clone(&fail);
        let release_restart = Arc::clone(&release_restart);
        move |ctx| {
            let order = Arc::clone(&order);
            let fail = Arc::clone(&fail);
            let release_restart = Arc::clone(&release_restart);
            async move {
                order.lock().await.push(("first", ctx.generation()));
                if ctx.generation() > 0 {
                    release_restart.notified().await;
                }
                ctx.mark_ready();
                if ctx.generation() == 0 {
                    fail.notified().await;
                    Err(std::io::Error::other("restart").into())
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .wait_for_ready();
    let second = ChildSpec::new("second", {
        let order = Arc::clone(&order);
        move |ctx| {
            let order = Arc::clone(&order);
            async move {
                order.lock().await.push(("second", ctx.generation()));
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .start_mode(StartMode::Sequential)
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();
    handle.wait_started().await.unwrap();
    fail.notify_one();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if order.lock().await.contains(&("first", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!order.lock().await.contains(&("second", 1)));
    release_restart.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if order.lock().await.contains(&("second", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn startup_failure_leaves_later_sequential_children_unstarted() {
    let later_started = Arc::new(Notify::new());
    let failed = ChildSpec::new("failed", |_| async {
        Err(std::io::Error::other("init failed").into())
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let later = ChildSpec::new("later", {
        let later_started = Arc::clone(&later_started);
        move |ctx| {
            let later_started = Arc::clone(&later_started);
            async move {
                later_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();

    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(failed)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), later_started.notified())
            .await
            .is_err()
    );
    assert!(matches!(
        handle.wait_started().await,
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn sequential_start_resumes_after_pre_ready_restart() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let later_started = Arc::new(Notify::new());
    let flaky = ChildSpec::new("flaky", {
        let attempts = Arc::clone(&attempts);
        move |ctx| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    return Err(std::io::Error::other("retry init").into());
                }
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let later = ChildSpec::new("later", {
        let later_started = Arc::clone(&later_started);
        move |ctx| {
            let later_started = Arc::clone(&later_started);
            async move {
                later_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(flaky)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    tokio::time::timeout(Duration::from_secs(1), later_started.notified())
        .await
        .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn wait_started_accepts_an_immediate_child_that_already_completed() {
    let handle = SupervisorBuilder::new()
        .child(ChildSpec::new("oneshot", |_| async { Ok(()) }).restart(RestartPolicy::Never))
        .build()
        .unwrap()
        .spawn();
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn dynamic_gated_child_waits_for_readiness_in_sequential_mode() {
    let release = Arc::new(Notify::new());
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .build()
        .unwrap()
        .spawn();
    let child = ChildSpec::new("dynamic", {
        let release = Arc::clone(&release);
        move |ctx| {
            let release = Arc::clone(&release);
            async move {
                release.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let add = tokio::spawn({
        let handle = handle.clone();
        async move { handle.add_child(child).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!add.is_finished());
    release.notify_one();
    add.await.unwrap().unwrap();
    handle.wait_started().await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn nested_supervisor_gates_later_parent_siblings() {
    let release = Arc::new(Notify::new());
    let later_started = Arc::new(Notify::new());
    let nested = SupervisorBuilder::new()
        .child(
            ChildSpec::new("nested-child", {
                let release = Arc::clone(&release);
                move |ctx| {
                    let release = Arc::clone(&release);
                    async move {
                        release.notified().await;
                        ctx.mark_ready();
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .wait_for_ready(),
        )
        .build()
        .unwrap();
    let later = ChildSpec::new("later", {
        let later_started = Arc::clone(&later_started);
        move |ctx| {
            let later_started = Arc::clone(&later_started);
            async move {
                later_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .supervisor("nested", nested)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), later_started.notified())
            .await
            .is_err()
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_traffic_does_not_starve_sequential_readiness() {
    let noisy_attempts = Arc::new(AtomicUsize::new(0));
    let gated_started = Arc::new(Notify::new());
    let release_gated = Arc::new(Notify::new());
    let later_started = Arc::new(Notify::new());

    let mut root = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .event_channel_capacity(1_024);
    for index in 0..4 {
        let attempts = Arc::clone(&noisy_attempts);
        let nested = SupervisorBuilder::new()
            .restart_intensity(RestartIntensity::new(100_000, Duration::from_secs(60)))
            .child(
                ChildSpec::new("flapping", move |_ctx| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async move { Err(std::io::Error::other("emit another nested update").into()) }
                })
                .restart(RestartPolicy::OnFailure),
            )
            .build()
            .unwrap();
        root = root.supervisor(format!("noisy-{index}"), nested);
    }

    let gated = ChildSpec::new("gated", {
        let gated_started = Arc::clone(&gated_started);
        let release_gated = Arc::clone(&release_gated);
        move |ctx| {
            let gated_started = Arc::clone(&gated_started);
            let release_gated = Arc::clone(&release_gated);
            async move {
                gated_started.notify_one();
                release_gated.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let later = ChildSpec::new("later", {
        let later_started = Arc::clone(&later_started);
        move |ctx| {
            let later_started = Arc::clone(&later_started);
            async move {
                later_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();

    let handle = root.child(gated).child(later).build().unwrap().spawn();
    tokio::time::timeout(Duration::from_secs(2), gated_started.notified())
        .await
        .expect("parent should reach the readiness-gated child");
    tokio::time::timeout(Duration::from_secs(2), async {
        while noisy_attempts.load(Ordering::SeqCst) < 200 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("nested children should continuously emit lifecycle updates");

    release_gated.notify_one();
    tokio::time::timeout(Duration::from_millis(250), later_started.notified())
        .await
        .expect("queued readiness must preempt continuous nested traffic");

    handle.wait_started().await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn rest_for_one_restart_preserves_sequential_readiness_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(Notify::new());
    let release_restart = Arc::new(Notify::new());
    let anchor = ChildSpec::new("anchor", {
        let order = Arc::clone(&order);
        move |ctx| {
            let order = Arc::clone(&order);
            async move {
                order.lock().await.push(("anchor", ctx.generation()));
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let middle = ChildSpec::new("middle", {
        let order = Arc::clone(&order);
        let fail = Arc::clone(&fail);
        let release_restart = Arc::clone(&release_restart);
        move |ctx| {
            let order = Arc::clone(&order);
            let fail = Arc::clone(&fail);
            let release_restart = Arc::clone(&release_restart);
            async move {
                order.lock().await.push(("middle", ctx.generation()));
                if ctx.generation() > 0 {
                    release_restart.notified().await;
                }
                ctx.mark_ready();
                if ctx.generation() == 0 {
                    fail.notified().await;
                    Err(std::io::Error::other("restart suffix").into())
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    })
    .wait_for_ready();
    let last = ChildSpec::new("last", {
        let order = Arc::clone(&order);
        move |ctx| {
            let order = Arc::clone(&order);
            async move {
                order.lock().await.push(("last", ctx.generation()));
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::RestForOne)
        .start_mode(StartMode::Sequential)
        .child(anchor)
        .child(middle)
        .child(last)
        .build()
        .unwrap()
        .spawn();
    handle.wait_started().await.unwrap();
    fail.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if order.lock().await.contains(&("middle", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!order.lock().await.contains(&("last", 1)));
    assert!(!order.lock().await.contains(&("anchor", 1)));
    release_restart.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if order.lock().await.contains(&("last", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pre_ready_one_for_all_failure_does_not_duplicate_children() {
    let first_attempts = Arc::new(AtomicUsize::new(0));
    let second_runs = Arc::new(AtomicUsize::new(0));
    let first = ChildSpec::new("first", {
        let first_attempts = Arc::clone(&first_attempts);
        move |ctx| {
            let attempt = first_attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    return Err(std::io::Error::other("pre-ready failure").into());
                }
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let second = ChildSpec::new("second", {
        let second_runs = Arc::clone(&second_runs);
        move |ctx| {
            let second_runs = Arc::clone(&second_runs);
            async move {
                second_runs.fetch_add(1, Ordering::SeqCst);
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .start_mode(StartMode::Sequential)
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn nested_startup_abort_gracefully_stops_ready_siblings() {
    let sibling_stopped = Arc::new(Notify::new());
    let sibling = ChildSpec::new("sibling", {
        let sibling_stopped = Arc::clone(&sibling_stopped);
        move |ctx| {
            let sibling_stopped = Arc::clone(&sibling_stopped);
            async move {
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                sibling_stopped.notify_one();
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let failed = ChildSpec::new("failed", |_| async {
        Err(std::io::Error::other("nested init failed").into())
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let nested = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(sibling)
        .child(failed)
        .build()
        .unwrap();
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .supervisor(
            "nested",
            SupervisorSpec::new(nested).restart(RestartPolicy::Never),
        )
        .build()
        .unwrap()
        .spawn();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    tokio::time::timeout(Duration::from_secs(1), sibling_stopped.notified())
        .await
        .unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn removal_during_pre_ready_group_restart_does_not_stall_later_children() {
    let trigger = Arc::new(Notify::new());
    let pre_ready_failure = Arc::new(Notify::new());
    let sibling_runs = Arc::new(AtomicUsize::new(0));
    let failing = ChildSpec::new("failing", {
        let trigger = Arc::clone(&trigger);
        let pre_ready_failure = Arc::clone(&pre_ready_failure);
        move |ctx| {
            let trigger = Arc::clone(&trigger);
            let pre_ready_failure = Arc::clone(&pre_ready_failure);
            async move {
                if ctx.generation() == 0 {
                    ctx.mark_ready();
                    trigger.notified().await;
                } else {
                    pre_ready_failure.notify_one();
                }
                Err(std::io::Error::other("restart failure").into())
            }
        }
    })
    .restart_intensity(
        RestartIntensity::new(5, Duration::from_secs(10))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(50))),
    )
    .wait_for_ready();
    let sibling = ChildSpec::new("sibling", {
        let sibling_runs = Arc::clone(&sibling_runs);
        move |ctx| {
            let sibling_runs = Arc::clone(&sibling_runs);
            async move {
                sibling_runs.fetch_add(1, Ordering::SeqCst);
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .start_mode(StartMode::Sequential)
        .child(failing)
        .child(sibling)
        .build()
        .unwrap()
        .spawn();
    handle.wait_started().await.unwrap();
    trigger.notify_one();
    tokio::time::timeout(Duration::from_secs(1), pre_ready_failure.notified())
        .await
        .unwrap();
    handle.remove_child("failing").await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while sibling_runs.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sibling_runs.load(Ordering::SeqCst), 2);
    handle.wait_started().await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn removal_during_initial_readiness_wait_continues_later_children() {
    let failing_exited = Arc::new(Notify::new());
    let later_started = Arc::new(Notify::new());
    let failing = ChildSpec::new("failing", {
        let failing_exited = Arc::clone(&failing_exited);
        move |_ctx| {
            let failing_exited = Arc::clone(&failing_exited);
            async move {
                failing_exited.notify_one();
                Err(std::io::Error::other("init failed").into())
            }
        }
    })
    .restart_intensity(
        RestartIntensity::new(5, Duration::from_secs(10))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(50))),
    )
    .wait_for_ready();
    let later = ChildSpec::new("later", {
        let later_started = Arc::clone(&later_started);
        move |ctx| {
            let later_started = Arc::clone(&later_started);
            async move {
                later_started.notify_one();
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .child(failing)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    failing_exited.notified().await;
    handle.remove_child("failing").await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), later_started.notified())
        .await
        .unwrap();
    handle.wait_started().await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn fatal_error_during_dynamic_start_stops_the_supervisor() {
    let trigger_failure = Arc::new(Notify::new());
    let dynamic_started = Arc::new(Notify::new());
    let sibling = ChildSpec::new("sibling", {
        let trigger_failure = Arc::clone(&trigger_failure);
        move |ctx| {
            let trigger_failure = Arc::clone(&trigger_failure);
            async move {
                if ctx.generation() == 0 {
                    trigger_failure.notified().await;
                }
                Err(std::io::Error::other("fatal restart loop").into())
            }
        }
    });
    let handle = SupervisorBuilder::new()
        .start_mode(StartMode::Sequential)
        .restart_intensity(RestartIntensity::new(1, Duration::from_secs(10)))
        .child(sibling)
        .build()
        .unwrap()
        .spawn();
    let dynamic = ChildSpec::new("dynamic", {
        let dynamic_started = Arc::clone(&dynamic_started);
        move |ctx| {
            let dynamic_started = Arc::clone(&dynamic_started);
            async move {
                dynamic_started.notify_one();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let add = tokio::spawn({
        let handle = handle.clone();
        async move { handle.add_child(dynamic).await }
    });
    dynamic_started.notified().await;
    trigger_failure.notify_one();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), add)
            .await
            .unwrap()
            .unwrap(),
        Err(tokio_supervisor::ControlError::SupervisorStopping)
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), handle.wait())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::RestartIntensityExceeded)
    );
}

#[tokio::test]
async fn drained_pre_ready_never_child_reports_startup_aborted() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let never = ChildSpec::new("never", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let failing = ChildSpec::new("failing", {
        let attempts = Arc::clone(&attempts);
        move |ctx| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    return Err(std::io::Error::other("restart group").into());
                }
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(never)
        .child(failing)
        .build()
        .unwrap()
        .spawn();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    assert!(handle.snapshot().child("never").unwrap().startup_aborted);
    handle.shutdown_and_wait().await.unwrap();
}
