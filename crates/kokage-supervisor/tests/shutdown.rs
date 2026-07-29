use std::sync::{
    Arc, Barrier as ThreadBarrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use kokage_supervisor::{
    BackoffPolicy, ChildLifecycleEvent, ChildLifecycleEventKind, ChildLifecycleWatch, ChildSpec,
    ControlError, ExitStatusView, RestartConfig, RestartPolicy, ShutdownPolicy, Supervisor,
    SupervisorError,
};
use tokio::{
    sync::{Barrier, Notify, mpsc},
    time::{Duration, Instant, sleep, timeout},
};

mod common;
use common::ObservedEvent;

async fn next_lifecycle_event(
    lifecycle: &mut ChildLifecycleWatch,
    phase: &str,
) -> Option<ChildLifecycleEvent> {
    timeout(common::EVENT_TIMEOUT, lifecycle.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
}

#[tokio::test]
async fn external_shutdown_stops_all_children() {
    let exits = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let make_child = |id: &'static str, exits: Arc<AtomicUsize>| {
        let started_tx = started_tx.clone();
        ChildSpec::task(id, move |ctx| {
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

    let supervisor = Supervisor::ordered()
        .child(make_child("worker-a", exits.clone()))
        .child(make_child("worker-b", exits.clone()))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    common::recv_event(&mut started_rx).await;
    common::recv_event(&mut started_rx).await;
    handle.shutdown();

    common::wait(&handle, "external shutdown")
        .await
        .expect("shutdown should succeed");
    assert_eq!(exits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn shutdown_is_idempotent_across_handle_clones() {
    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("worker", |ctx| async move {
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

    common::wait(&handle, "first cloned shutdown waiter")
        .await
        .expect("first waiter should resolve");
    common::wait(&clone, "second cloned shutdown waiter")
        .await
        .expect("second waiter should resolve");
}

#[tokio::test]
async fn dropping_every_handle_leaves_the_owned_supervisor_running() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();

    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("worker", move |ctx| {
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

    let running = supervisor.spawn();
    let handle = running.handle();
    let last_handle = handle.clone();

    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "started");

    drop(handle);
    common::assert_no_event(&mut lifecycle_rx).await;

    drop(last_handle);
    common::assert_no_event(&mut lifecycle_rx).await;

    running.shutdown();
    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "cancelled");

    running.wait().await.expect("supervisor stops cleanly");
}

#[tokio::test]
async fn dropping_the_running_supervisor_requests_graceful_shutdown() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let running = Supervisor::ordered()
        .child(ChildSpec::task("worker", move |ctx| {
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
        .spawn()
        .expect("valid supervisor");
    let handle = running.handle();

    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "started");
    drop(running);
    assert_eq!(common::recv_event(&mut lifecycle_rx).await, "cancelled");
    handle
        .wait()
        .await
        .expect("owner drop drains the supervisor");
}

#[tokio::test]
async fn fire_and_forget_spawn_shuts_down_immediately() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let builder = Supervisor::ordered().child(ChildSpec::task("worker", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver dropped");
            Ok(())
        }
    }));
    let handle = builder.handle();

    let _ = builder.spawn().expect("valid supervisor");
    common::recv_event(&mut cancelled_rx).await;
    handle
        .wait()
        .await
        .expect("temporary owner drains the supervisor");
}

#[tokio::test]
async fn dropping_owner_stops_a_supervisor_idling_at_zero_children() {
    let running = Supervisor::ordered()
        .spawn()
        .expect("empty supervisor builds");
    let handle = running.handle();
    let mut events = common::event_watch(&handle);

    drop(running);

    loop {
        if common::recv_supervisor_event(&mut events).await == ObservedEvent::SupervisorStopped {
            break;
        }
    }
}

#[tokio::test]
async fn cooperative_child_observes_cancellation_before_shutdown_finishes() {
    let saw_cancel = Arc::new(AtomicBool::new(false));

    let saw_cancel_for_child = saw_cancel.clone();
    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("worker", move |ctx| {
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

    common::wait(&handle, "cooperative child shutdown")
        .await
        .expect("shutdown should succeed");
    assert!(saw_cancel.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stubborn_child_is_aborted_and_reported_in_cooperative_mode() {
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let live_flag = common::LiveFlag::new();

    let saw_cancel_for_child = saw_cancel.clone();
    let live_flag_for_child = live_flag.clone();
    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("stubborn", move |_ctx| {
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
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    handle.shutdown();

    assert!(matches!(
        common::wait(&handle, "stubborn child shutdown").await,
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "stubborn"
    ));
    assert!(!saw_cancel.load(Ordering::SeqCst));
    assert!(
        !live_flag.is_live(),
        "task should be dropped before wait resolves"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_shutdown_does_not_wait_unboundedly_for_an_aborted_task_to_join() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (blocking_tx, mut blocking_rx) = mpsc::unbounded_channel();
    let release_blocking_poll = Arc::new(ThreadBarrier::new(2));
    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("non-yielding", {
                let release_blocking_poll = Arc::clone(&release_blocking_poll);
                move |ctx| {
                    let started_tx = started_tx.clone();
                    let blocking_tx = blocking_tx.clone();
                    let release_blocking_poll = Arc::clone(&release_blocking_poll);
                    async move {
                        started_tx.send(()).expect("test receiver dropped");
                        ctx.shutdown_token().cancelled().await;
                        blocking_tx.send(()).expect("test receiver dropped");
                        release_blocking_poll.wait();
                        Ok(())
                    }
                }
            })
            .shutdown(ShutdownPolicy::Cooperative {
                grace: Duration::from_millis(20),
            }),
        )
        .build()
        .expect("valid supervisor");
    let handle = supervisor.spawn();
    common::recv_event(&mut started_rx).await;

    handle.shutdown();
    common::recv_event(&mut blocking_rx).await;
    let wait_result = timeout(common::EVENT_TIMEOUT, handle.wait()).await;
    release_blocking_poll.wait();
    assert!(matches!(
        wait_result.expect("ordered shutdown should advance without joining the blocked poll"),
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "non-yielding"
    ));
}

#[tokio::test]
async fn cooperative_shutdown_times_out_with_stuck_child_name() {
    let live_flag = common::LiveFlag::new();

    let live_flag_for_child = live_flag.clone();
    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("stubborn", move |_ctx| {
                let live_flag = live_flag_for_child.clone();
                async move {
                    let _guard = live_flag.guard();
                    loop {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
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
async fn mixed_cooperative_shutdown_reports_every_timed_out_child() {
    let cooperative_live_flag = common::LiveFlag::new();
    let aborting_live_flag = common::LiveFlag::new();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let started_tx_for_aborting = started_tx.clone();
    let aborting_live_flag_for_child = aborting_live_flag.clone();
    let started_tx_for_cooperative = started_tx.clone();
    let cooperative_live_flag_for_child = cooperative_live_flag.clone();
    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("cooperative-peer", move |_ctx| {
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
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
        )
        .child(
            ChildSpec::task("cooperative", move |_ctx| {
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
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
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
        SupervisorError::ShutdownTimedOut("cooperative, cooperative-peer".to_owned())
    );
    assert!(
        !cooperative_live_flag.is_live(),
        "timed-out cooperative child should be aborted before wait resolves"
    );
    assert!(
        !aborting_live_flag.is_live(),
        "second cooperative child should also be aborted before wait resolves"
    );
}

#[tokio::test]
async fn a_wrapper_that_overruns_the_tidy_beat_is_hard_aborted() {
    const GRACE: Duration = Duration::from_millis(20);
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let live_flag = common::LiveFlag::new();
    let live = live_flag.clone();
    let handle = Supervisor::ordered()
        .child(
            ChildSpec::task("slow-wrapper", move |ctx| {
                let started_tx = started_tx.clone();
                let live = live.clone();
                async move {
                    let _guard = live.guard();
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.abort_token().cancelled().await;
                    std::future::pending::<()>().await;
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::Cooperative { grace: GRACE }),
        )
        .build()
        .expect("supervisor builds")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();
    common::recv_event(&mut started_rx).await;

    assert_eq!(
        timeout(common::EVENT_TIMEOUT, handle.shutdown_and_wait())
            .await
            .expect("hard-abort shutdown should remain bounded"),
        Err(SupervisorError::ShutdownTimedOut("slow-wrapper".to_owned()))
    );
    assert!(
        !live_flag.is_live(),
        "an overrunning wrapper is aborted rather than left running"
    );

    while let Some(event) = next_lifecycle_event(&mut lifecycle, "wrapper timeout exit").await {
        if let ChildLifecycleEventKind::Exited { reason, .. } = event.kind {
            assert_eq!(reason, ExitStatusView::Aborted { after_grace: true });
            return;
        }
    }
    panic!("hard-aborted wrapper did not publish a timeout exit");
}

#[tokio::test]
async fn dynamic_children_escalate_at_their_own_grace_deadlines() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (short_escalated_tx, mut short_escalated_rx) = mpsc::unbounded_channel();
    let short_started_tx = started_tx.clone();
    let handle = Supervisor::dynamic()
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    let mut lifecycle = handle.watch_lifecycle();

    handle
        .add_child(
            ChildSpec::task("short", move |ctx| {
                let started_tx = short_started_tx.clone();
                let short_escalated_tx = short_escalated_tx.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.abort_token().cancelled().await;
                    short_escalated_tx.send(()).expect("test receiver dropped");
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::Cooperative {
                grace: Duration::from_millis(20),
            }),
        )
        .await
        .expect("short child added");
    handle
        .add_child(
            ChildSpec::task("long", move |ctx| {
                let started_tx = started_tx.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::Cooperative {
                grace: Duration::from_secs(5),
            }),
        )
        .await
        .expect("long child added");
    common::recv_n(&mut started_rx, 2).await;

    handle.shutdown();
    common::recv_event(&mut short_escalated_rx).await;
    assert_eq!(
        timeout(common::EVENT_TIMEOUT, handle.wait())
            .await
            .expect("dynamic shutdown should finish after the short child times out"),
        Err(SupervisorError::ShutdownTimedOut("short".to_owned()))
    );

    while let Some(event) = next_lifecycle_event(&mut lifecycle, "dynamic child timeout exit").await
    {
        if event.child_id == "short"
            && let ChildLifecycleEventKind::Exited { reason, .. } = event.kind
        {
            assert_eq!(reason, ExitStatusView::Aborted { after_grace: true });
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
    let handle = Supervisor::dynamic()
        .build()
        .expect("valid supervisor")
        .spawn();
    handle
        .add_child(
            ChildSpec::task("stubborn", move |_ctx| {
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
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
        )
        .await
        .expect("stubborn child added");
    handle
        .add_child(ChildSpec::task("keeper", |ctx| async move {
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
    assert_eq!(
        err,
        ControlError::Failed(SupervisorError::ShutdownTimedOut("stubborn".to_owned()))
    );
    assert!(
        !live_flag.is_live(),
        "timed-out cooperative removal should abort the child before returning"
    );
    loop {
        let event = next_lifecycle_event(&mut lifecycle, "drop-triggered child exit")
            .await
            .expect("keeper keeps scope live");
        if event.child_id == "stubborn"
            && let ChildLifecycleEventKind::Exited { reason, .. } = event.kind
        {
            assert_eq!(reason, ExitStatusView::Aborted { after_grace: true });
            break;
        }
    }

    handle.shutdown();
    common::wait(&handle, "drop-triggered dynamic shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn wait_only_resolves_after_child_lifetimes_end() {
    let live_flag = common::LiveFlag::new();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let live_flag_for_child = live_flag.clone();
    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("stubborn", move |_ctx| {
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
            .shutdown(ShutdownPolicy::Abort),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    common::recv_event(&mut started_rx).await;
    assert!(live_flag.is_live());

    handle.shutdown();
    common::wait(&handle, "drop-triggered handle shutdown")
        .await
        .expect("shutdown should succeed");
    assert!(
        !live_flag.is_live(),
        "child must be dropped before wait completes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_preempts_zero_delay_restart() {
    let supervisor = Supervisor::ordered()
        .restart_config(RestartConfig::new(8, Duration::from_secs(1)))
        .child(
            ChildSpec::task("flaky", |_ctx| async move {
                Err(common::test_error("restart immediately"))
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = common::event_watch(&handle);

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestartScheduled { delay, .. } => {
                assert!(delay.is_zero(), "test requires zero-delay restart");
                handle.shutdown();
                break;
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to zero-delay restart");
            }
            _ => {}
        }
    }

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::SupervisorStopping => break,
            ObservedEvent::ChildRestarted { .. } => {
                panic!("child restarted after shutdown was requested");
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to zero-delay restart");
            }
            _ => {}
        }
    }

    common::wait(&handle, "zero-delay restart shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn shutdown_preempts_delayed_restart_in_cooperative_mode() {
    let saw_cancel = Arc::new(AtomicBool::new(false));

    let saw_cancel_for_keeper = saw_cancel.clone();
    let supervisor = Supervisor::ordered()
        .restart_config(common::restart_config(
            8,
            Duration::from_secs(1),
            BackoffPolicy::Fixed(Duration::from_millis(200)),
        ))
        .child(
            ChildSpec::task("flaky", |_ctx| async move {
                Err(common::test_error("restart later"))
            })
            .restart(RestartPolicy::OnFailure),
        )
        .child(
            ChildSpec::task("keeper", move |ctx| {
                let saw_cancel = saw_cancel_for_keeper.clone();
                async move {
                    ctx.shutdown_token().cancelled().await;
                    saw_cancel.store(true, Ordering::SeqCst);
                    Ok(())
                }
            })
            .shutdown(ShutdownPolicy::Cooperative {
                grace: common::SHORT_GRACE,
            }),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = common::event_watch(&handle);

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestartScheduled { id, delay, .. } if id == "flaky" => {
                assert!(
                    delay >= Duration::from_millis(200),
                    "test requires a non-zero delayed restart"
                );
                handle.shutdown();
                break;
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to delayed restart");
            }
            _ => {}
        }
    }

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::SupervisorStopping => break,
            ObservedEvent::ChildRestarted { id, .. } if id == "flaky" => {
                panic!("child restarted after shutdown was requested");
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("shutdown lost to delayed restart");
            }
            _ => {}
        }
    }

    common::wait(&handle, "delayed restart shutdown")
        .await
        .expect("shutdown should succeed");
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
        ChildSpec::task(id, move |ctx| {
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
    let handle = Supervisor::ordered()
        .child(child("first", Arc::clone(&first_release)))
        .child(child("second", Arc::clone(&second_release)))
        .child(child("third", Arc::clone(&third_release)))
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    common::wait_started(&handle, "ordered shutdown children startup")
        .await
        .expect("children started");
    let mut lifecycle = handle.watch_lifecycle();

    let shutdown = tokio::spawn({
        let handle = handle.clone();
        async move { common::shutdown_and_wait(&handle, "ordered staged shutdown").await }
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
        if matches!(event.kind, ChildLifecycleEventKind::Exited { .. }) {
            exited.push(event.child_id);
        }
    }
    assert_eq!(exited, ["third", "second", "first"]);
}

#[tokio::test]
async fn ordered_grace_expiry_aborts_and_joins_only_the_cursor_child_before_advancing() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let stubborn_live = common::LiveFlag::new();
    let stubborn = ChildSpec::task("stubborn", {
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
    .shutdown(ShutdownPolicy::Cooperative {
        grace: common::SHORT_GRACE,
    });
    let dependency = ChildSpec::task("dependency", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx
                .send("dependency")
                .expect("test receiver dropped");
            Ok(())
        }
    });
    let handle = Supervisor::ordered()
        .child(dependency)
        .child(stubborn)
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    common::wait_started(&handle, "dynamic deadline children startup")
        .await
        .expect("children start");
    assert!(stubborn_live.is_live());

    let shutdown = tokio::spawn({
        let handle = handle.clone();
        async move { common::shutdown_and_wait(&handle, "dynamic deadline shutdown").await }
    });
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "stubborn");
    assert_eq!(common::recv_event(&mut cancelled_rx).await, "dependency");
    assert!(
        !stubborn_live.is_live(),
        "the expired cursor child must be aborted and joined before advancing"
    );
    assert!(matches!(
        shutdown.await.expect("shutdown task joins"),
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "stubborn"
    ));
}

#[tokio::test]
async fn parent_child_grace_bounds_a_slow_nested_ordered_teardown() {
    let head_live = common::LiveFlag::new();
    let tail_live = common::LiveFlag::new();
    let (tail_cancelled_tx, mut tail_cancelled_rx) = mpsc::unbounded_channel();
    let nested_child = |id: &'static str, live: common::LiveFlag, report: bool| {
        let tail_cancelled_tx = tail_cancelled_tx.clone();
        ChildSpec::task(id, move |ctx| {
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
        .shutdown(ShutdownPolicy::Cooperative {
            grace: Duration::from_secs(5),
        })
    };
    let nested = Supervisor::ordered()
        .child(nested_child("head", head_live.clone(), false))
        .child(nested_child("tail", tail_live.clone(), true))
        .build()
        .expect("nested ordered supervisor builds");
    let handle = Supervisor::ordered()
        .child(
            kokage_supervisor::ChildSpec::supervisor("nested", nested).shutdown(
                ShutdownPolicy::Cooperative {
                    grace: common::SHORT_GRACE,
                },
            ),
        )
        .build()
        .expect("root builds")
        .spawn();
    common::wait_started(&handle, "slow nested tree startup")
        .await
        .expect("nested tree starts");

    let shutdown = timeout(common::EVENT_TIMEOUT, async {
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
    .expect("parent grace bounds the slow nested walk");
    assert!(matches!(
        shutdown,
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "nested"
    ));
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
    let leaf = ChildSpec::task("leaf", {
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
    .shutdown(ShutdownPolicy::Cooperative {
        grace: Duration::from_secs(5),
    });
    let inner = Supervisor::ordered()
        .child(leaf)
        .build()
        .expect("inner supervisor builds");
    let middle = Supervisor::ordered()
        .child(
            kokage_supervisor::ChildSpec::supervisor("inner", inner).shutdown(
                ShutdownPolicy::Cooperative {
                    grace: Duration::from_secs(5),
                },
            ),
        )
        .build()
        .expect("middle supervisor builds");
    let handle = Supervisor::ordered()
        .child(
            kokage_supervisor::ChildSpec::supervisor("middle", middle).shutdown(
                ShutdownPolicy::Cooperative {
                    grace: common::SHORT_GRACE,
                },
            ),
        )
        .build()
        .expect("root supervisor builds")
        .spawn();
    common::wait_started(&handle, "deep nested tree startup")
        .await
        .expect("nested tree starts");
    assert!(leaf_live.is_live());

    let shutdown = timeout(common::EVENT_TIMEOUT, handle.shutdown_and_wait())
        .await
        .expect("parent grace bounds recursive nested shutdown");
    assert!(matches!(
        shutdown,
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "middle"
    ));
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
        ChildSpec::task(id, |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            std::future::pending::<()>().await;
            Ok(())
        })
        .shutdown(ShutdownPolicy::Cooperative { grace: GRACE })
    };

    let ordered = Supervisor::ordered()
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
    assert!(matches!(
        ordered.shutdown_and_wait().await,
        Err(SupervisorError::ShutdownTimedOut(ids)) if ids == "third, second, first"
    ));
    let ordered_elapsed = ordered_started.elapsed();
    assert!(
        ordered_elapsed >= GRACE * 3,
        "ordered children each receive their own grace: {ordered_elapsed:?}"
    );

    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Barrier::new(4));
    let dynamic = Supervisor::dynamic()
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    for id in ["first", "second", "third"] {
        let cancelled_tx = cancelled_tx.clone();
        let release = Arc::clone(&release);
        dynamic
            .add_child(
                ChildSpec::task(id, move |ctx| {
                    let cancelled_tx = cancelled_tx.clone();
                    let release = Arc::clone(&release);
                    async move {
                        ctx.shutdown_token().cancelled().await;
                        cancelled_tx.send(id).expect("test receiver dropped");
                        release.wait().await;
                        Ok(())
                    }
                })
                .shutdown(ShutdownPolicy::Cooperative { grace: GRACE }),
            )
            .await
            .expect("dynamic member added");
    }
    common::wait_started(&dynamic, "dynamic shutdown members startup")
        .await
        .expect("dynamic members start");
    let shutdown = tokio::spawn({
        let dynamic = dynamic.clone();
        async move { common::shutdown_and_wait(&dynamic, "concurrent dynamic shutdown").await }
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
