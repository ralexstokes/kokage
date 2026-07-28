use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify, mpsc};
use tokio_supervisor::{ChildSpec, RestartConfig, RestartPolicy, Strategy, Supervisor};

mod common;

#[tokio::test]
async fn sequential_start_waits_for_explicit_readiness() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let first_started = Arc::new(Notify::new());

    let first = ChildSpec::task("first", {
        let order = Arc::clone(&order);
        let release = Arc::clone(&release);
        let first_started = Arc::clone(&first_started);
        move |ctx| {
            let order = Arc::clone(&order);
            let release = Arc::clone(&release);
            let first_started = Arc::clone(&first_started);
            async move {
                order.lock().await.push("first");
                first_started.notify_one();
                release.notified().await;
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let second = ChildSpec::task("second", {
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

    let handle = Supervisor::ordered()
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();

    tokio::time::timeout(common::EVENT_TIMEOUT, first_started.notified())
        .await
        .expect("first child should enter its readiness gate");
    assert_eq!(&*order.lock().await, &["first"]);
    release.notify_one();
    tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*order.lock().await, &["first", "second"]);
    common::shutdown_and_wait(&handle, "sequential readiness test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn one_for_all_restart_preserves_sequential_readiness_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(Notify::new());
    let release_restart = Arc::new(Notify::new());

    let first = ChildSpec::task("first", {
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
    let second = ChildSpec::task("second", {
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

    let handle = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();
    common::wait_started(&handle, "initial one-for-all startup")
        .await
        .unwrap();
    fail.notify_one();

    tokio::time::timeout(common::EVENT_TIMEOUT, async {
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
    tokio::time::timeout(common::EVENT_TIMEOUT, async {
        loop {
            if order.lock().await.contains(&("second", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    common::shutdown_and_wait(&handle, "one-for-all readiness test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn startup_failure_is_skipped_before_later_sequential_children_start() {
    let later_started = Arc::new(Notify::new());
    let failed = ChildSpec::task("failed", |_| async {
        Err(std::io::Error::other("init failed").into())
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let later = ChildSpec::task("later", {
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

    let handle = Supervisor::ordered()
        .child(failed)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, later_started.notified())
        .await
        .expect("a terminal startup failure should be skipped");
    assert!(matches!(
        common::wait_started(&handle, "startup-abort result").await,
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    common::shutdown_and_wait(&handle, "startup failure test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn sequential_start_resumes_after_pre_ready_restart() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let later_started = Arc::new(Notify::new());
    let flaky = ChildSpec::task("flaky", {
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
    let later = ChildSpec::task("later", {
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
    let handle = Supervisor::ordered()
        .child(flaky)
        .child(later)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    tokio::time::timeout(common::EVENT_TIMEOUT, later_started.notified())
        .await
        .unwrap();
    common::shutdown_and_wait(&handle, "pre-ready restart test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn wait_started_accepts_an_immediate_child_that_already_completed() {
    let completed = Arc::new(Notify::new());
    let handle = Supervisor::ordered()
        .child(
            ChildSpec::task("oneshot", {
                let completed = Arc::clone(&completed);
                move |_| {
                    let completed = Arc::clone(&completed);
                    async move {
                        completed.notify_one();
                        Ok(())
                    }
                }
            })
            .restart(RestartPolicy::Never),
        )
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, completed.notified())
        .await
        .expect("immediate child should complete before wait_started is called");
    tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    common::shutdown_and_wait(&handle, "immediate completion test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn nested_supervisor_gates_later_parent_siblings() {
    let release = Arc::new(Notify::new());
    let nested_started = Arc::new(Notify::new());
    let (later_started_tx, mut later_started_rx) = mpsc::unbounded_channel();
    let nested = Supervisor::ordered()
        .child(
            ChildSpec::task("nested-child", {
                let release = Arc::clone(&release);
                let nested_started = Arc::clone(&nested_started);
                move |ctx| {
                    let release = Arc::clone(&release);
                    let nested_started = Arc::clone(&nested_started);
                    async move {
                        nested_started.notify_one();
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
    let later = ChildSpec::task("later", {
        let later_started_tx = later_started_tx.clone();
        move |ctx| {
            let later_started_tx = later_started_tx.clone();
            async move {
                later_started_tx
                    .send(())
                    .expect("later start receiver alive");
                ctx.mark_ready();
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .wait_for_ready();
    let handle = Supervisor::ordered()
        .child(ChildSpec::supervisor("nested", nested))
        .child(later)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, nested_started.notified())
        .await
        .expect("nested child should enter its readiness gate");
    assert!(matches!(
        later_started_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    release.notify_one();
    tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(common::EVENT_TIMEOUT, later_started_rx.recv())
        .await
        .expect("later child should start after nested readiness")
        .expect("later start sender remains live");
    common::shutdown_and_wait(&handle, "nested readiness test shutdown")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_traffic_does_not_starve_sequential_readiness() {
    let noisy_attempts = Arc::new(AtomicUsize::new(0));
    let gated_started = Arc::new(Notify::new());
    let release_gated = Arc::new(Notify::new());
    let later_started = Arc::new(Notify::new());

    let mut root = Supervisor::ordered();
    for index in 0..4 {
        let attempts = Arc::clone(&noisy_attempts);
        let nested = Supervisor::ordered()
            .restart_intensity(RestartConfig::new(100_000, Duration::from_secs(60)))
            .child(
                ChildSpec::task("flapping", move |_ctx| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async move { Err(std::io::Error::other("emit another nested update").into()) }
                })
                .restart(RestartPolicy::OnFailure),
            )
            .build()
            .unwrap();
        root = root.child(ChildSpec::supervisor(format!("noisy-{index}"), nested));
    }

    let gated = ChildSpec::task("gated", {
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
    let later = ChildSpec::task("later", {
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
    tokio::time::timeout(common::EVENT_TIMEOUT, gated_started.notified())
        .await
        .expect("parent should reach the readiness-gated child");
    tokio::time::timeout(common::EVENT_TIMEOUT, async {
        while noisy_attempts.load(Ordering::SeqCst) < 200 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("nested children should continuously emit lifecycle updates");

    release_gated.notify_one();
    tokio::time::timeout(common::EVENT_TIMEOUT, later_started.notified())
        .await
        .expect("queued readiness must preempt continuous nested traffic");

    common::wait_started(&handle, "noisy nested readiness completion")
        .await
        .unwrap();
    common::shutdown_and_wait(&handle, "noisy nested readiness test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn rest_for_one_restart_preserves_sequential_readiness_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(Notify::new());
    let release_restart = Arc::new(Notify::new());
    let anchor = ChildSpec::task("anchor", {
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
    let middle = ChildSpec::task("middle", {
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
    let last = ChildSpec::task("last", {
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
    let handle = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(anchor)
        .child(middle)
        .child(last)
        .build()
        .unwrap()
        .spawn();
    common::wait_started(&handle, "initial rest-for-one startup")
        .await
        .unwrap();
    fail.notify_one();
    tokio::time::timeout(common::EVENT_TIMEOUT, async {
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
    tokio::time::timeout(common::EVENT_TIMEOUT, async {
        loop {
            if order.lock().await.contains(&("last", 1)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    common::shutdown_and_wait(&handle, "rest-for-one readiness test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn pre_ready_one_for_all_failure_does_not_duplicate_children() {
    let first_attempts = Arc::new(AtomicUsize::new(0));
    let second_runs = Arc::new(AtomicUsize::new(0));
    let first = ChildSpec::task("first", {
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
    let second = ChildSpec::task("second", {
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
    let handle = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(first)
        .child(second)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    common::shutdown_and_wait(&handle, "pre-ready one-for-all test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn nested_startup_abort_gracefully_stops_ready_siblings() {
    let sibling_stopped = Arc::new(Notify::new());
    let sibling = ChildSpec::task("sibling", {
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
    let failed = ChildSpec::task("failed", |_| async {
        Err(std::io::Error::other("nested init failed").into())
    })
    .restart(RestartPolicy::Never)
    .wait_for_ready();
    let nested = Supervisor::ordered()
        .child(sibling)
        .child(failed)
        .build()
        .unwrap();
    let handle = Supervisor::ordered()
        .child(ChildSpec::supervisor("nested", nested).restart(RestartPolicy::Never))
        .build()
        .unwrap()
        .spawn();
    assert!(matches!(
        tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    tokio::time::timeout(common::EVENT_TIMEOUT, sibling_stopped.notified())
        .await
        .unwrap();
    common::shutdown_and_wait(&handle, "nested startup-abort test shutdown")
        .await
        .unwrap();
}

#[tokio::test]
async fn drained_pre_ready_never_child_reports_startup_aborted() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let trigger = Arc::new(Notify::new());
    let never_started = Arc::new(Notify::new());
    let never = ChildSpec::task("never", {
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
    let failing = ChildSpec::task("failing", {
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
    let handle = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(failing)
        .child(never)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(common::EVENT_TIMEOUT, never_started.notified())
        .await
        .expect("never child should start and remain pre-ready");
    trigger.notify_one();
    assert!(matches!(
        tokio::time::timeout(common::EVENT_TIMEOUT, handle.wait_started())
            .await
            .unwrap(),
        Err(tokio_supervisor::SupervisorError::StartupAborted(_))
    ));
    assert!(
        handle
            .snapshot()
            .child("never")
            .unwrap()
            .state
            .startup_aborted()
    );
    common::shutdown_and_wait(&handle, "drained pre-ready child test shutdown")
        .await
        .unwrap();
}
