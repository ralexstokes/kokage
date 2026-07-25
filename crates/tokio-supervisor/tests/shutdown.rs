use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tokio::{
    sync::{Barrier, Notify, mpsc},
    time::{Duration, Instant, sleep, timeout},
};
use tokio_supervisor::{
    BackoffPolicy, ChildSpec, ControlError, DynamicSupervisorBuilder, ExitStatusView,
    LifecycleEventKind, RestartIntensity, RestartPolicy, ShutdownMode, ShutdownPolicy,
    SupervisorBuilder, SupervisorError, SupervisorEvent,
};

mod common;

#[tokio::test]
async fn external_shutdown_stops_all_children() {
    let exits = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let make_child = |id: &'static str, exits: Arc<AtomicUsize>| {
        let started_tx = started_tx.clone();
        ChildSpec::new(id, move |ctx| {
            let exits = exits.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                exits.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
    };

    let supervisor = SupervisorBuilder::new()
        .child(make_child("worker-a", exits.clone()))
        .child(make_child("worker-b", exits.clone()))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    common::recv_event(&mut started_rx).await;
    common::recv_event(&mut started_rx).await;
    handle.shutdown();

    handle.wait().await.expect("shutdown should succeed");
    assert_eq!(exits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn shutdown_is_idempotent_across_handle_clones() {
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let clone = handle.clone();

    handle.shutdown();
    clone.shutdown();
    handle.shutdown();

    handle.wait().await.expect("first waiter should resolve");
    clone.wait().await.expect("second waiter should resolve");
}

#[tokio::test]
async fn dropping_last_handle_clone_requests_graceful_shutdown() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();

    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", move |ctx| {
            let lifecycle_tx = lifecycle_tx.clone();
            async move {
                lifecycle_tx.send("started").expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                lifecycle_tx
                    .send("cancelled")
                    .expect("test receiver dropped");
                Ok(())
            }
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();
    let last_handle = handle.clone();

    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "started");

    drop(handle);
    common::assert_no_event(&mut lifecycle_rx).await;

    drop(last_handle);
    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "cancelled");

    loop {
        if common::recv_supervisor_event(&mut events).await == SupervisorEvent::SupervisorStopped {
            break;
        }
    }
}

#[tokio::test]
async fn dropping_last_handle_stops_a_supervisor_idling_at_zero_children() {
    let supervisor = SupervisorBuilder::new()
        .build()
        .expect("empty supervisor builds");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();

    drop(handle);

    loop {
        if common::recv_supervisor_event(&mut events).await == SupervisorEvent::SupervisorStopped {
            break;
        }
    }
}

#[tokio::test]
async fn cooperative_child_observes_cancellation_before_shutdown_finishes() {
    let saw_cancel = Arc::new(AtomicBool::new(false));

    let saw_cancel_for_child = saw_cancel.clone();
    let supervisor = SupervisorBuilder::new()
        .child(ChildSpec::new("worker", move |ctx| {
            let saw_cancel = saw_cancel_for_child.clone();
            async move {
                ctx.shutdown_token().cancelled().await;
                saw_cancel.store(true, Ordering::SeqCst);
                Ok(())
            }
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    handle.shutdown();

    handle.wait().await.expect("shutdown should succeed");
    assert!(saw_cancel.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stubborn_child_is_aborted_in_cooperative_then_abort_mode() {
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let live_flag = common::LiveFlag::new();

    let saw_cancel_for_child = saw_cancel.clone();
    let live_flag_for_child = live_flag.clone();
    let supervisor = SupervisorBuilder::new()
        .child(
            ChildSpec::new("stubborn", move |_ctx| {
                let saw_cancel = saw_cancel_for_child.clone();
                let live_flag = live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    loop {
                        sleep(Duration::from_millis(10)).await;
                        let _ = saw_cancel.load(Ordering::SeqCst);
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeThenAbort,
            )),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    handle.shutdown();

    handle.wait().await.expect("shutdown should succeed");
    assert!(!saw_cancel.load(Ordering::SeqCst));
    assert!(
        !live_flag.is_live(),
        "task should be dropped before wait resolves"
    );
}

#[tokio::test]
async fn cooperative_shutdown_times_out_with_stuck_child_name() {
    let live_flag = common::LiveFlag::new();

    let live_flag_for_child = live_flag.clone();
    let supervisor = SupervisorBuilder::new()
        .child(
            ChildSpec::new("stubborn", move |_ctx| {
                let live_flag = live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeStrict,
            )),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    handle.shutdown();

    let err = handle
        .wait()
        .await
        .expect_err("pure cooperative shutdown should time out");
    assert_eq!(
        err,
        SupervisorError::ShutdownTimedOut("stubborn".to_owned())
    );
    assert!(
        !live_flag.is_live(),
        "timed-out cooperative child should be aborted before wait resolves"
    );
}

#[tokio::test]
async fn mixed_shutdown_only_reports_pure_cooperative_children() {
    let cooperative_live_flag = common::LiveFlag::new();
    let aborting_live_flag = common::LiveFlag::new();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let started_tx_for_aborting = started_tx.clone();
    let aborting_live_flag_for_child = aborting_live_flag.clone();
    let started_tx_for_cooperative = started_tx.clone();
    let cooperative_live_flag_for_child = cooperative_live_flag.clone();
    let supervisor = SupervisorBuilder::new()
        .child(
            ChildSpec::new("cooperative-then-abort", move |_ctx| {
                let started_tx = started_tx_for_aborting.clone();
                let live_flag = aborting_live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    started_tx.send(()).expect("test receiver dropped");
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeThenAbort,
            )),
        )
        .child(
            ChildSpec::new("cooperative", move |_ctx| {
                let started_tx = started_tx_for_cooperative.clone();
                let live_flag = cooperative_live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    started_tx.send(()).expect("test receiver dropped");
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeStrict,
            )),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    common::recv_n(&mut started_rx, 2).await;

    handle.shutdown();

    let err = handle
        .wait()
        .await
        .expect_err("mixed shutdown should still report cooperative timeouts");
    assert_eq!(
        err,
        SupervisorError::ShutdownTimedOut("cooperative".to_owned())
    );
    assert!(
        !cooperative_live_flag.is_live(),
        "timed-out cooperative child should be aborted before wait resolves"
    );
    assert!(
        !aborting_live_flag.is_live(),
        "cooperative-then-abort child should also be aborted before wait resolves"
    );
}

#[tokio::test]
async fn dynamic_children_escalate_at_their_own_grace_deadlines() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (short_escalated_tx, mut short_escalated_rx) = mpsc::unbounded_channel();
    let short_started_tx = started_tx.clone();
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();

    handle
        .add_child(
            ChildSpec::new("short", move |ctx| {
                let started_tx = short_started_tx.clone();
                let short_escalated_tx = short_escalated_tx.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.abort_token().cancelled().await;
                    short_escalated_tx.send(()).expect("test receiver dropped");
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::cooperative_strict(Duration::from_millis(
                20,
            ))),
        )
        .await
        .expect("short child added");
    handle
        .add_child(
            ChildSpec::new("long", move |ctx| {
                let started_tx = started_tx.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::cooperative_then_abort(Duration::from_secs(
                5,
            ))),
        )
        .await
        .expect("long child added");
    common::recv_n(&mut started_rx, 2).await;

    handle.shutdown();
    timeout(Duration::from_millis(200), short_escalated_rx.recv())
        .await
        .expect("short child escalated at its own grace")
        .expect("short escalation sender remained live");
    assert_eq!(
        handle.wait().await,
        Err(SupervisorError::ShutdownTimedOut("short".to_owned()))
    );

    while let Some(event) = lifecycle.next().await {
        if event.child_id == "short"
            && let LifecycleEventKind::Exited { reason, .. } = event.kind
        {
            assert_eq!(reason, ExitStatusView::ShutdownTimedOut);
            return;
        }
    }
    panic!("short child timeout exit was not published");
}

#[tokio::test]
async fn cooperative_remove_child_times_out_with_stuck_child_name() {
    let live_flag = common::LiveFlag::new();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let live_flag_for_child = live_flag.clone();
    let handle = DynamicSupervisorBuilder::new()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_child(
            ChildSpec::new("stubborn", move |_ctx| {
                let started_tx = started_tx.clone();
                let live_flag = live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    started_tx.send(()).expect("test receiver dropped");
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeStrict,
            )),
        )
        .await
        .expect("stubborn child added");
    handle
        .add_child(ChildSpec::new("keeper", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("keeper added");
    common::recv_event(&mut started_rx).await;
    let mut lifecycle = handle.watch_lifecycle();

    let err = handle
        .remove_child("stubborn")
        .await
        .expect_err("pure cooperative child removal should time out");
    assert_eq!(err, ControlError::ShutdownTimedOut("stubborn".to_owned()));
    assert!(
        !live_flag.is_live(),
        "timed-out cooperative removal should abort the child before returning"
    );
    loop {
        let event = lifecycle.next().await.expect("keeper keeps scope live");
        if event.child_id == "stubborn"
            && let LifecycleEventKind::Exited { reason, .. } = event.kind
        {
            assert_eq!(reason, ExitStatusView::ShutdownTimedOut);
            break;
        }
    }

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn wait_only_resolves_after_child_lifetimes_end() {
    let live_flag = common::LiveFlag::new();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let live_flag_for_child = live_flag.clone();
    let supervisor = SupervisorBuilder::new()
        .child(
            ChildSpec::new("stubborn", move |_ctx| {
                let started_tx = started_tx.clone();
                let live_flag = live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    started_tx.send(()).expect("test receiver dropped");
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::Abort,
            )),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    common::recv_event(&mut started_rx).await;
    assert!(live_flag.is_live());

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
    assert!(
        !live_flag.is_live(),
        "child must be dropped before wait completes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_preempts_zero_delay_restart() {
    let supervisor = SupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(8, Duration::from_secs(1)))
        .child(
            ChildSpec::new("flaky", |_ctx| async move {
                Err(common::test_error("restart immediately"))
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();

    loop {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::ChildRestartScheduled { delay, .. } => {
                assert!(delay.is_zero(), "test requires zero-delay restart");
                handle.shutdown();
                break;
            }
            SupervisorEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to zero-delay restart");
            }
            _ => {}
        }
    }

    loop {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::SupervisorStopping => break,
            SupervisorEvent::ChildRestarted { .. } => {
                panic!("child restarted after shutdown was requested");
            }
            SupervisorEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to zero-delay restart");
            }
            _ => {}
        }
    }

    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn shutdown_preempts_delayed_restart_in_cooperative_mode() {
    let saw_cancel = Arc::new(AtomicBool::new(false));

    let saw_cancel_for_keeper = saw_cancel.clone();
    let supervisor = SupervisorBuilder::new()
        .restart_intensity(
            RestartIntensity::new(8, Duration::from_secs(1))
                .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(200))),
        )
        .child(
            ChildSpec::new("flaky", |_ctx| async move {
                Err(common::test_error("restart later"))
            })
            .restart(RestartPolicy::OnFailure),
        )
        .child(
            ChildSpec::new("keeper", move |ctx| {
                let saw_cancel = saw_cancel_for_keeper.clone();
                async move {
                    ctx.shutdown_token().cancelled().await;
                    saw_cancel.store(true, Ordering::SeqCst);
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::new(
                common::SHORT_GRACE,
                ShutdownMode::CooperativeStrict,
            )),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();

    loop {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::ChildRestartScheduled { id, delay, .. } if id == "flaky" => {
                assert!(
                    delay >= Duration::from_millis(200),
                    "test requires a non-zero delayed restart"
                );
                handle.shutdown();
                break;
            }
            SupervisorEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to delayed restart");
            }
            _ => {}
        }
    }

    loop {
        match common::recv_supervisor_event(&mut events).await {
            SupervisorEvent::SupervisorStopping => break,
            SupervisorEvent::ChildRestarted { id, .. } if id == "flaky" => {
                panic!("child restarted after shutdown was requested");
            }
            SupervisorEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to delayed restart");
            }
            _ => {}
        }
    }

    handle.wait().await.expect("shutdown should succeed");
    assert!(
        saw_cancel.load(Ordering::SeqCst),
        "cooperative child should observe shutdown cancellation"
    );
}

#[tokio::test]
async fn ordered_shutdown_waits_for_each_later_sibling_before_cancelling_the_previous_one() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let first_release = Arc::new(Notify::new());
    let second_release = Arc::new(Notify::new());
    let third_release = Arc::new(Notify::new());
    let child = |id: &'static str, release: Arc<Notify>| {
        let cancelled_tx = cancelled_tx.clone();
        ChildSpec::new(id, move |ctx| {
            let cancelled_tx = cancelled_tx.clone();
            let release = Arc::clone(&release);
            async move {
                ctx.shutdown_token().cancelled().await;
                cancelled_tx.send(id).expect("test receiver dropped");
                release.notified().await;
                Ok(())
            }
        })
    };
    let handle = SupervisorBuilder::new()
        .child(child("first", Arc::clone(&first_release)))
        .child(child("second", Arc::clone(&second_release)))
        .child(child("third", Arc::clone(&third_release)))
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    handle.wait_started().await.expect("children started");
    let mut lifecycle = handle.watch_lifecycle();

    let shutdown = tokio::spawn({
        let handle = handle.clone();
        async move { handle.shutdown_and_wait().await }
    });
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "third");
    assert!(
        timeout(common::QUIET_TIMEOUT, cancelled_rx.recv())
            .await
            .is_err(),
        "second must not be cancelled before third exits"
    );
    third_release.notify_one();
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "second");
    assert!(
        timeout(common::QUIET_TIMEOUT, cancelled_rx.recv())
            .await
            .is_err(),
        "first must not be cancelled before second exits"
    );
    second_release.notify_one();
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "first");
    first_release.notify_one();
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("ordered shutdown succeeds");

    let mut exited = Vec::new();
    while exited.len() < 3 {
        let event = lifecycle
            .next()
            .await
            .expect("staged lifecycle exits remain available");
        if matches!(event.kind, LifecycleEventKind::Exited { .. }) {
            exited.push(event.child_id);
        }
    }
    assert_eq!(exited, ["third", "second", "first"]);
}

#[tokio::test]
async fn ordered_grace_expiry_aborts_and_joins_only_the_cursor_child_before_advancing() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let stubborn_live = common::LiveFlag::new();
    let stubborn = ChildSpec::new("stubborn", {
        let stubborn_live = stubborn_live.clone();
        let cancelled_tx = cancelled_tx.clone();
        move |ctx| {
            let guard = stubborn_live.guard();
            let cancelled_tx = cancelled_tx.clone();
            async move {
                let _guard = guard;
                ctx.shutdown_token().cancelled().await;
                cancelled_tx
                    .send("stubborn")
                    .expect("test receiver dropped");
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::cooperative_then_abort(common::SHORT_GRACE));
    let dependency = ChildSpec::new("dependency", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx
                .send("dependency")
                .expect("test receiver dropped");
            Ok(())
        }
    });
    let handle = SupervisorBuilder::new()
        .child(dependency)
        .child(stubborn)
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    handle.wait_started().await.expect("children start");
    assert!(stubborn_live.is_live());

    let shutdown = tokio::spawn({
        let handle = handle.clone();
        async move { handle.shutdown_and_wait().await }
    });
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "stubborn");
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "dependency");
    assert!(
        !stubborn_live.is_live(),
        "the expired cursor child must be aborted and joined before advancing"
    );
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("then-abort expiry is a clean shutdown");
}

#[tokio::test]
async fn parent_child_grace_bounds_a_slow_nested_ordered_teardown() {
    let head_live = common::LiveFlag::new();
    let tail_live = common::LiveFlag::new();
    let (tail_cancelled_tx, mut tail_cancelled_rx) = mpsc::unbounded_channel();
    let nested_child = |id: &'static str, live: common::LiveFlag, report: bool| {
        let tail_cancelled_tx = tail_cancelled_tx.clone();
        ChildSpec::new(id, move |ctx| {
            let guard = live.guard();
            let tail_cancelled_tx = tail_cancelled_tx.clone();
            async move {
                let _guard = guard;
                ctx.shutdown_token().cancelled().await;
                if report {
                    tail_cancelled_tx.send(()).expect("test receiver dropped");
                }
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .shutdown(ShutdownPolicy::cooperative_then_abort(Duration::from_secs(
            5,
        )))
    };
    let nested = SupervisorBuilder::new()
        .child(nested_child("head", head_live.clone(), false))
        .child(nested_child("tail", tail_live.clone(), true))
        .build()
        .expect("nested ordered supervisor builds");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "nested",
            tokio_supervisor::SupervisorSpec::new(nested)
                .shutdown(ShutdownPolicy::cooperative_then_abort(common::SHORT_GRACE)),
        )
        .build()
        .expect("root builds")
        .spawn();
    handle.wait_started().await.expect("nested tree starts");

    timeout(common::EVENT_TIMEOUT, async {
        let shutdown = handle.shutdown_and_wait();
        tokio::pin!(shutdown);
        tokio::select! {
            result = &mut shutdown => result,
            event = tail_cancelled_rx.recv() => {
                event.expect("tail observes nested cancellation");
                shutdown.await
            }
        }
    })
    .await
    .expect("parent grace bounds the slow nested walk")
    .expect("parent then-abort shutdown succeeds");
    timeout(common::EVENT_TIMEOUT, async {
        while head_live.is_live() || tail_live.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborting the nested wrapper cascades to its remaining descendants");
}

#[tokio::test]
async fn parent_grace_expiry_hard_cascades_through_nested_supervisor_levels() {
    let leaf_live = common::LiveFlag::new();
    let leaf = ChildSpec::new("leaf", {
        let leaf_live = leaf_live.clone();
        move |ctx| {
            let guard = leaf_live.guard();
            async move {
                let _guard = guard;
                ctx.shutdown_token().cancelled().await;
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::cooperative_then_abort(Duration::from_secs(
        5,
    )));
    let inner = SupervisorBuilder::new()
        .child(leaf)
        .build()
        .expect("inner supervisor builds");
    let middle = SupervisorBuilder::new()
        .supervisor(
            "inner",
            tokio_supervisor::SupervisorSpec::new(inner).shutdown(
                ShutdownPolicy::cooperative_then_abort(Duration::from_secs(5)),
            ),
        )
        .build()
        .expect("middle supervisor builds");
    let handle = SupervisorBuilder::new()
        .supervisor(
            "middle",
            tokio_supervisor::SupervisorSpec::new(middle)
                .shutdown(ShutdownPolicy::cooperative_then_abort(common::SHORT_GRACE)),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    handle.wait_started().await.expect("nested tree starts");
    assert!(leaf_live.is_live());

    timeout(common::EVENT_TIMEOUT, handle.shutdown_and_wait())
        .await
        .expect("parent grace bounds recursive nested shutdown")
        .expect("parent fallback abort is a clean shutdown");
    timeout(common::EVENT_TIMEOUT, async {
        while leaf_live.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hard cascade reaches the deeply nested leaf");
}

#[tokio::test]
async fn ordered_shutdown_graces_sum_while_dynamic_child_clocks_run_concurrently() {
    const GRACE: Duration = Duration::from_millis(40);
    let stubborn = |id: &'static str| {
        ChildSpec::new(id, |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            std::future::pending::<()>().await;
            Ok(())
        })
        .shutdown(ShutdownPolicy::cooperative_then_abort(GRACE))
    };

    let ordered = SupervisorBuilder::new()
        .child(stubborn("first"))
        .child(stubborn("second"))
        .child(stubborn("third"))
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    ordered
        .wait_started()
        .await
        .expect("ordered children start");
    let ordered_started = Instant::now();
    ordered
        .shutdown_and_wait()
        .await
        .expect("ordered shutdown succeeds");
    let ordered_elapsed = ordered_started.elapsed();
    assert!(
        ordered_elapsed >= GRACE * 3,
        "ordered children each receive their own grace: {ordered_elapsed:?}"
    );

    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Barrier::new(4));
    let dynamic = DynamicSupervisorBuilder::new()
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    for id in ["first", "second", "third"] {
        let cancelled_tx = cancelled_tx.clone();
        let release = Arc::clone(&release);
        dynamic
            .add_child(
                ChildSpec::new(id, move |ctx| {
                    let cancelled_tx = cancelled_tx.clone();
                    let release = Arc::clone(&release);
                    async move {
                        ctx.shutdown_token().cancelled().await;
                        cancelled_tx.send(id).expect("test receiver dropped");
                        release.wait().await;
                        Ok(())
                    }
                })
                .shutdown(ShutdownPolicy::cooperative_then_abort(GRACE)),
            )
            .await
            .expect("dynamic member added");
    }
    dynamic.wait_started().await.expect("dynamic members start");
    let shutdown = tokio::spawn({
        let dynamic = dynamic.clone();
        async move { dynamic.shutdown_and_wait().await }
    });
    let mut cancelled = common::recv_n(&mut cancelled_rx, 3).await;
    cancelled.sort_unstable();
    assert_eq!(cancelled, ["first", "second", "third"]);
    release.wait().await;
    shutdown
        .await
        .expect("dynamic shutdown task joins")
        .expect("dynamic shutdown succeeds");
}
