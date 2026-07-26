use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify};
use tokio_supervisor::{
    ChildSpec, RestartIntensity, RestartPolicy, Strategy, SupervisorBuilder, SupervisorSpec,
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
async fn startup_failure_is_skipped_before_later_sequential_children_start() {
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
        .child(failed)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(Duration::from_secs(1), later_started.notified())
        .await
        .expect("a terminal startup failure should be skipped");
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

    let mut root = SupervisorBuilder::new().event_channel_capacity(1_024);
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
        .child(sibling)
        .child(failed)
        .build()
        .unwrap();
    let handle = SupervisorBuilder::new()
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
async fn drained_pre_ready_never_child_reports_startup_aborted() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let trigger = Arc::new(Notify::new());
    let never_started = Arc::new(Notify::new());
    let never = ChildSpec::new("never", {
        let never_started = Arc::clone(&never_started);
        move |ctx| {
            let never_started = Arc::clone(&never_started);
            async move {
                never_started.notify_one();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let failing = ChildSpec::new("failing", {
        let attempts = Arc::clone(&attempts);
        let trigger = Arc::clone(&trigger);
        move |ctx| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let trigger = Arc::clone(&trigger);
            async move {
                ctx.mark_ready();
                if attempt == 0 {
                    trigger.notified().await;
                    return Err(std::io::Error::other("restart group").into());
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(failing)
        .child(never)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(Duration::from_secs(1), never_started.notified())
        .await
        .expect("never child should start and remain pre-ready");
    trigger.notify_one();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    assert!(handle.snapshot().child("never").unwrap().startup_aborted);
    handle.shutdown_and_wait().await.unwrap();
}
