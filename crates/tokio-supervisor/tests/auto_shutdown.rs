use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc, oneshot},
    time::timeout,
};
use tokio_supervisor::{
    AutoShutdown, ChildSpec, ControlError, DynamicSupervisorBuilder, ExitStatusView,
    RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy, SupervisorBuilder, SupervisorError,
    SupervisorEvent, SupervisorSpec,
};

mod common;

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn any_significant_clean_exit_stops_siblings_and_supervisor() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let supervisor = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AnySignificant)
        .child(ChildSpec::new("trigger", |_| async { Ok(()) }).significant())
        .child(ChildSpec::new("sibling", move |ctx| {
            let cancelled_tx = cancelled_tx.clone();
            async move {
                ctx.shutdown_token().cancelled().await;
                cancelled_tx.send(()).expect("test receiver dropped");
                Ok(())
            }
        }))
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();
    handle.wait().await.expect("auto-shutdown should be clean");
    common::recv_event(&mut cancelled_rx).await;

    let mut sequence = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            SupervisorEvent::ChildExited { id, .. } if id == "trigger" => {
                sequence.push("exited");
            }
            SupervisorEvent::AutoShutdownTriggered { id, mode, .. }
                if id == "trigger" && mode == AutoShutdown::AnySignificant =>
            {
                sequence.push("auto_shutdown");
            }
            SupervisorEvent::SupervisorStopping => sequence.push("stopping"),
            SupervisorEvent::SupervisorStopped => sequence.push("stopped"),
            _ => {}
        }
    }
    assert_eq!(sequence, ["exited", "auto_shutdown", "stopping", "stopped"]);
    timeout(common::QUIET_TIMEOUT, events.recv())
        .await
        .expect_err("SupervisorStopped must be the final supervisor event");
}

#[tokio::test]
async fn all_significant_waits_for_last_clean_exit() {
    let (first_tx, first_rx) = oneshot::channel::<()>();
    let first_rx = Arc::new(std::sync::Mutex::new(Some(first_rx)));
    let (second_tx, second_rx) = oneshot::channel::<()>();
    let second_rx = Arc::new(std::sync::Mutex::new(Some(second_rx)));

    let supervisor = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AllSignificant)
        .child(
            ChildSpec::new("first", move |_| {
                let rx = first_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(RestartPolicy::Never)
            .significant(),
        )
        .child(
            ChildSpec::new("second", move |_| {
                let rx = second_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(RestartPolicy::Never)
            .significant(),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    first_tx.send(()).expect("first child dropped");
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "one significant child must not be enough"
    );
    second_tx.send(()).expect("second child dropped");
    handle
        .wait()
        .await
        .expect("last significant exit should stop cleanly");
}

#[tokio::test]
async fn significant_failure_restarts_before_clean_exit_triggers_shutdown() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_child = attempts.clone();
    let supervisor = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AnySignificant)
        .child(
            ChildSpec::new("trigger", move |_| {
                let attempt = attempts_for_child.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err("first attempt fails".into())
                    } else {
                        Ok(())
                    }
                }
            })
            .significant(),
        )
        .build()
        .expect("valid supervisor");

    supervisor
        .spawn()
        .wait()
        .await
        .expect("second clean exit should stop supervisor");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn nested_auto_shutdown_is_a_clean_child_exit_to_parent() {
    let inner = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AnySignificant)
        .child(ChildSpec::new("done", |_| async { Ok(()) }).significant())
        .build()
        .expect("valid inner supervisor");
    let parent = SupervisorBuilder::new()
        .supervisor(
            "job",
            SupervisorSpec::new(inner).restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid parent supervisor");

    let handle = parent.spawn();
    let mut events = handle.subscribe();
    let event = loop {
        let event = common::recv_supervisor_event(&mut events).await;
        if matches!(
            &event,
            SupervisorEvent::ChildExited {
                id,
                status: ExitStatusView::Completed,
                ..
            } if id == "job"
        ) {
            break event;
        }
    };
    assert!(matches!(event, SupervisorEvent::ChildExited { .. }));
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "parent should continue after clean child exit"
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("parent shutdown succeeds");
}

#[tokio::test]
async fn significant_nested_supervisor_triggers_parent_auto_shutdown() {
    let inner = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AnySignificant)
        .child(ChildSpec::new("done", |_| async { Ok(()) }).significant())
        .build()
        .expect("valid inner supervisor");
    let parent = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AnySignificant)
        .supervisor("job", SupervisorSpec::new(inner).significant())
        .build()
        .expect("valid parent supervisor");

    parent
        .spawn()
        .wait()
        .await
        .expect("significant nested completion should stop parent");
}

#[tokio::test]
async fn dynamic_significant_children_report_an_accurate_invalid_config() {
    let handle = DynamicSupervisorBuilder::new()
        .restart(RestartPolicy::Always)
        .build()
        .expect("valid dynamic supervisor")
        .spawn();
    let err = handle
        .add_child(ChildSpec::new("invalid", |_| async { Ok(()) }).significant())
        .await
        .expect_err("dynamic scopes reject significant children");
    assert_eq!(
        err,
        ControlError::InvalidConfig("dynamic scopes do not support significant children")
    );
    let nested = SupervisorBuilder::new()
        .build()
        .expect("valid nested supervisor");
    let err = handle
        .add_supervisor("nested", SupervisorSpec::new(nested).significant())
        .await
        .expect_err("dynamic scopes reject significant nested supervisors");
    assert_eq!(
        err,
        ControlError::InvalidConfig("dynamic scopes do not support significant children")
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("dynamic supervisor shuts down");
}

#[tokio::test]
async fn failed_never_child_does_not_satisfy_all_significant() {
    let supervisor = SupervisorBuilder::new()
        .auto_shutdown(AutoShutdown::AllSignificant)
        .child(
            ChildSpec::new("failed", |_| async { Err(common::test_error("failed")) })
                .restart(RestartPolicy::Never)
                .significant(),
        )
        .child(
            ChildSpec::new("completed", |_| async { Ok(()) })
                .restart(RestartPolicy::Never)
                .significant(),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a failed Never child must not count as a clean completion"
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("explicit shutdown succeeds");
}

#[tokio::test]
async fn all_significant_ignores_stale_completion_after_group_restart() {
    let fail_group = Arc::new(Notify::new());
    let finish_a = Arc::new(Notify::new());
    let finish_b = Arc::new(Notify::new());
    let (restarted_tx, mut restarted_rx) = mpsc::unbounded_channel();

    let a = ChildSpec::new("a", {
        let finish_a = finish_a.clone();
        let restarted_tx = restarted_tx.clone();
        move |ctx| {
            let finish_a = finish_a.clone();
            let restarted_tx = restarted_tx.clone();
            async move {
                if ctx.generation() == 0 {
                    return Ok(());
                }
                restarted_tx.send("a").expect("test receiver dropped");
                finish_a.notified().await;
                Ok(())
            }
        }
    })
    .significant();
    let b = ChildSpec::new("b", {
        let finish_b = finish_b.clone();
        let restarted_tx = restarted_tx.clone();
        move |ctx| {
            let finish_b = finish_b.clone();
            let restarted_tx = restarted_tx.clone();
            async move {
                if ctx.generation() == 0 {
                    ctx.shutdown_token().cancelled().await;
                    return Ok(());
                }
                restarted_tx.send("b").expect("test receiver dropped");
                finish_b.notified().await;
                Ok(())
            }
        }
    })
    .significant();
    let failing = ChildSpec::new("failing", {
        let fail_group = fail_group.clone();
        move |ctx| {
            let fail_group = fail_group.clone();
            async move {
                if ctx.generation() == 0 {
                    fail_group.notified().await;
                    return Err(common::test_error("restart group"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    });

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .auto_shutdown(AutoShutdown::AllSignificant)
        .child(a)
        .child(b)
        .child(failing)
        .build()
        .expect("valid supervisor")
        .spawn();

    fail_group.notify_one();
    let mut restarted = common::recv_n(&mut restarted_rx, 2).await;
    restarted.sort_unstable();
    assert_eq!(restarted, ["a", "b"]);

    finish_b.notify_one();
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a running significant child with a stale completed exit must not count"
    );
    finish_a.notify_one();
    handle
        .wait()
        .await
        .expect("both current generations completed cleanly");
}

#[tokio::test]
async fn natural_significant_completion_during_group_drain_still_triggers() {
    let finish_significant = Arc::new(Notify::new());
    let significant = ChildSpec::new("significant", {
        let finish_significant = finish_significant.clone();
        move |_| {
            let finish_significant = finish_significant.clone();
            async move {
                finish_significant.notified().await;
                Ok(())
            }
        }
    })
    .restart(RestartPolicy::Never)
    .significant();
    let failing = ChildSpec::new("failing", move |ctx| {
        let finish_significant = finish_significant.clone();
        async move {
            if ctx.generation() == 0 {
                finish_significant.notify_one();
                return Err(common::test_error("restart group"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .auto_shutdown(AutoShutdown::AnySignificant)
        .child(significant)
        .child(failing)
        .build()
        .expect("valid supervisor")
        .spawn();

    timeout(common::EVENT_TIMEOUT, handle.wait())
        .await
        .expect("natural completion must not be lost to the group drain")
        .expect("auto-shutdown should be clean");
}

#[tokio::test]
async fn group_cancellation_does_not_count_as_all_significant_completion() {
    let finish_natural = Arc::new(Notify::new());
    let finish_restarted = Arc::new(Notify::new());
    let finish_restarted_for_child = finish_restarted.clone();
    let (restarted_tx, mut restarted_rx) = mpsc::unbounded_channel();

    let natural = ChildSpec::new("natural", {
        let finish_natural = finish_natural.clone();
        move |_| {
            let finish_natural = finish_natural.clone();
            async move {
                finish_natural.notified().await;
                Ok(())
            }
        }
    })
    .restart(RestartPolicy::Never)
    .significant();
    let restarted = ChildSpec::new("restarted", move |ctx| {
        let finish_restarted = finish_restarted_for_child.clone();
        let restarted_tx = restarted_tx.clone();
        async move {
            if ctx.generation() == 0 {
                ctx.shutdown_token().cancelled().await;
                return Ok(());
            }
            restarted_tx.send(()).expect("test receiver dropped");
            finish_restarted.notified().await;
            Ok(())
        }
    })
    .significant();
    let failing = ChildSpec::new("failing", move |ctx| {
        let finish_natural = finish_natural.clone();
        async move {
            if ctx.generation() == 0 {
                finish_natural.notify_one();
                return Err(common::test_error("restart group"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .auto_shutdown(AutoShutdown::AllSignificant)
        .child(natural)
        .child(restarted)
        .child(failing)
        .build()
        .expect("valid supervisor")
        .spawn();

    common::recv_event(&mut restarted_rx).await;
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a cancellation-driven clean exit must not satisfy AllSignificant"
    );
    finish_restarted.notify_one();
    handle
        .wait()
        .await
        .expect("the restarted significant child completed naturally");
}

#[tokio::test]
async fn natural_always_completion_during_group_drain_spawns_once() {
    let finish_always = Arc::new(Notify::new());
    let (restarted_tx, mut restarted_rx) = mpsc::unbounded_channel();
    let always = ChildSpec::new("always", {
        let finish_always = finish_always.clone();
        move |ctx| {
            let finish_always = finish_always.clone();
            let restarted_tx = restarted_tx.clone();
            async move {
                if ctx.generation() == 0 {
                    finish_always.notified().await;
                    return Ok(());
                }
                restarted_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(RestartPolicy::Always);
    let failing = ChildSpec::new("failing", move |ctx| {
        let finish_always = finish_always.clone();
        async move {
            if ctx.generation() == 0 {
                finish_always.notify_one();
                return Err(common::test_error("restart group"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });

    let handle = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(always)
        .child(failing)
        .build()
        .expect("valid supervisor")
        .spawn();

    assert_eq!(common::recv_event(&mut restarted_rx).await, 1);
    common::assert_no_event(&mut restarted_rx).await;
    handle
        .shutdown_and_wait()
        .await
        .expect("single restarted generation should shut down cleanly");
}

#[tokio::test]
async fn fatal_restart_during_abort_removal_stops_supervisor() {
    let fail = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let removable = ChildSpec::new("removable", {
        let fail = fail.clone();
        let started_tx = started_tx.clone();
        move |_| {
            let guard = NotifyOnDrop(fail.clone());
            let started_tx = started_tx.clone();
            async move {
                let _guard = guard;
                started_tx.send(()).expect("test receiver dropped");
                pending::<()>().await;
                Ok(())
            }
        }
    })
    .shutdown(ShutdownPolicy::abort());
    let failing = ChildSpec::new("failing", move |_| {
        let fail = fail.clone();
        let started_tx = started_tx.clone();
        async move {
            started_tx.send(()).expect("test receiver dropped");
            fail.notified().await;
            Err(common::test_error("fatal restart"))
        }
    });

    let handle = DynamicSupervisorBuilder::new()
        .restart_intensity(RestartIntensity::new(0, Duration::from_secs(1)))
        .build()
        .expect("valid supervisor")
        .spawn();
    handle.add_child(removable).await.expect("removable added");
    handle.add_child(failing).await.expect("failing added");
    common::recv_n(&mut started_rx, 2).await;

    let _ = handle.remove_child("removable").await;
    let result = timeout(common::EVENT_TIMEOUT, handle.wait())
        .await
        .expect("fatal restart observed during removal must stop supervisor");
    assert_eq!(result, Err(SupervisorError::RestartIntensityExceeded));
}
