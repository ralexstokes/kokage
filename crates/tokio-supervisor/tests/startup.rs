use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Mutex, Notify, watch};
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
        .start_mode(StartMode::Sequential)
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
async fn dynamic_gated_child_add_resolves_before_readiness_in_sequential_mode() {
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
    let membership_epoch = tokio::time::timeout(Duration::from_secs(1), add)
        .await
        .expect("add should resolve on insertion")
        .expect("add task should join")
        .expect("dynamic child should be inserted");
    let child = handle
        .snapshot()
        .child("dynamic")
        .expect("inserted child should be visible")
        .clone();
    assert_eq!(child.membership_epoch, membership_epoch);
    assert!(!child.started);
    assert_eq!(child.state, tokio_supervisor::ChildStateView::Starting);

    let mut wait_started = Box::pin(handle.wait_started());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut wait_started)
            .await
            .is_err(),
        "wait_started should retain the stronger readiness contract"
    );
    release.notify_one();
    wait_started.await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn readiness_gated_child_can_await_add_before_marking_ready() {
    let (handle_tx, handle_rx) = watch::channel::<Option<tokio_supervisor::SupervisorHandle>>(None);
    let added_started = Arc::new(Notify::new());
    let first = ChildSpec::new("first", {
        let handle_rx = handle_rx.clone();
        let added_started = Arc::clone(&added_started);
        move |ctx| {
            let mut handle_rx = handle_rx.clone();
            let added_started = Arc::clone(&added_started);
            async move {
                let handle = {
                    let ready = handle_rx
                        .wait_for(Option::is_some)
                        .await
                        .expect("test handle sender remains open");
                    ready.as_ref().expect("handle was installed").clone()
                };
                handle
                    .add_child(ChildSpec::new("added-from-start", move |ctx| {
                        let added_started = Arc::clone(&added_started);
                        async move {
                            added_started.notify_one();
                            ctx.shutdown_token().cancelled().await;
                            Ok(())
                        }
                    }))
                    .await
                    .expect("add should resolve on insertion");
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
        .build()
        .expect("valid supervisor")
        .spawn();
    handle_tx
        .send(Some(handle.clone()))
        .expect("startup child retains the receiver");

    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .expect("startup should not deadlock on the control command")
        .expect("all children should report started");
    tokio::time::timeout(Duration::from_secs(1), added_started.notified())
        .await
        .expect("the queued child should start after its predecessor is ready");
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn add_mid_sequence_is_visible_before_it_spawns_and_starts_in_order() {
    let release_first = Arc::new(Notify::new());
    let late_started = Arc::new(Notify::new());
    let release_late = Arc::new(Notify::new());
    let first = ChildSpec::new("first", {
        let release_first = Arc::clone(&release_first);
        move |ctx| {
            let release_first = Arc::clone(&release_first);
            async move {
                release_first.notified().await;
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
        .build()
        .expect("valid supervisor")
        .spawn();

    let epoch = handle
        .add_child(
            ChildSpec::new("late", {
                let late_started = Arc::clone(&late_started);
                let release_late = Arc::clone(&release_late);
                move |ctx| {
                    let late_started = Arc::clone(&late_started);
                    let release_late = Arc::clone(&release_late);
                    async move {
                        late_started.notify_one();
                        release_late.notified().await;
                        ctx.mark_ready();
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .wait_for_ready(),
        )
        .await
        .expect("add should resolve while first is gated");
    let snapshot = handle.snapshot();
    let late = snapshot.child("late").expect("queued child is visible");
    assert_eq!(late.membership_epoch, epoch);
    assert_eq!(late.state, tokio_supervisor::ChildStateView::Starting);
    assert!(!late.started);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), late_started.notified())
            .await
            .is_err(),
        "late child must stay queued behind the first gate"
    );

    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), late_started.notified())
        .await
        .expect("late child should start once first is ready");
    let mut wait_started = Box::pin(handle.wait_started());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut wait_started)
            .await
            .is_err()
    );
    release_late.notify_one();
    wait_started.await.unwrap();
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn removing_a_start_queued_child_never_spawns_it() {
    let release_first = Arc::new(Notify::new());
    let queued_started = Arc::new(Notify::new());
    let first = ChildSpec::new("first", {
        let release_first = Arc::clone(&release_first);
        move |ctx| {
            let release_first = Arc::clone(&release_first);
            async move {
                release_first.notified().await;
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
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_child(ChildSpec::new("queued", {
            let queued_started = Arc::clone(&queued_started);
            move |ctx| {
                let queued_started = Arc::clone(&queued_started);
                async move {
                    queued_started.notify_one();
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }))
        .await
        .expect("queued membership should be inserted");

    handle
        .remove_child("queued")
        .await
        .expect("queued child removal should be immediate");
    release_first.notify_one();
    handle.wait_started().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), queued_started.notified())
            .await
            .is_err(),
        "removed queued child must never spawn"
    );
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn queued_nested_supervisor_accepts_control_before_its_loop_starts() {
    let release_first = Arc::new(Notify::new());
    let nested_child_started = Arc::new(Notify::new());
    let first = ChildSpec::new("first", {
        let release_first = Arc::clone(&release_first);
        move |ctx| {
            let release_first = Arc::clone(&release_first);
            async move {
                release_first.notified().await;
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
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_supervisor(
            "queued-nested",
            SupervisorBuilder::new().build().expect("empty supervisor"),
        )
        .await
        .expect("queued nested membership should be inserted");
    let nested = handle
        .supervisor("queued-nested")
        .expect("stable nested handle is registered at insertion");
    let add_task = tokio::spawn({
        let nested = nested.clone();
        let nested_child_started = Arc::clone(&nested_child_started);
        async move {
            nested
                .add_child(ChildSpec::new("nested-child", move |ctx| {
                    let nested_child_started = Arc::clone(&nested_child_started);
                    async move {
                        nested_child_started.notify_one();
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !add_task.is_finished(),
        "control should queue until the nested loop starts"
    );

    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), add_task)
        .await
        .expect("queued control should dispatch after nested startup")
        .expect("add task should join")
        .expect("nested child should be inserted");
    tokio::time::timeout(Duration::from_secs(1), nested_child_started.notified())
        .await
        .expect("nested child should start");
    assert!(nested.snapshot().child("nested-child").is_some());

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
    tokio::time::timeout(Duration::from_secs(1), add)
        .await
        .expect("add should already have resolved on insertion")
        .expect("add task should join")
        .expect("dynamic child should have been inserted");
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
