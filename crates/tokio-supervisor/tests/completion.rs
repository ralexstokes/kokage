//! Stopping a scope once its finite work is done.
//!
//! These cover `SupervisorHandle::wait_completed` and
//! `SupervisorHandle::shutdown_on_completion`, which replaced the supervisor's
//! `AutoShutdown` configuration and the `significant` child flag.

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
    ChildSpec, CompletionOutcome, DynamicSupervisorBuilder, ExitStatusView, RestartIntensity,
    RestartPolicy, ShutdownPolicy, Strategy, SupervisorBuilder, SupervisorError, SupervisorEvent,
    SupervisorSpec,
};

mod common;

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn a_completed_child_stops_siblings_and_supervisor() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let builder = SupervisorBuilder::new()
        .child(ChildSpec::new("trigger", |_| async { Ok(()) }).restart(RestartPolicy::Never))
        .child(ChildSpec::new("sibling", move |ctx| {
            let cancelled_tx = cancelled_tx.clone();
            async move {
                ctx.shutdown_token().cancelled().await;
                cancelled_tx.send(()).expect("test receiver dropped");
                Ok(())
            }
        }));
    let _finished = builder.handle().shutdown_on_completion(["trigger"]);
    let supervisor = builder.build().expect("valid supervisor");

    let handle = supervisor.spawn();
    let mut events = handle.subscribe();
    handle.wait().await.expect("completion should stop cleanly");
    common::recv_event(&mut cancelled_rx).await;

    let mut sequence = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            SupervisorEvent::ChildExited { id, .. } if id == "trigger" => {
                sequence.push("exited");
            }
            SupervisorEvent::SupervisorStopping => sequence.push("stopping"),
            SupervisorEvent::SupervisorStopped => sequence.push("stopped"),
            _ => {}
        }
    }
    assert_eq!(sequence, ["exited", "stopping", "stopped"]);
    let after_stopped = timeout(common::QUIET_TIMEOUT, events.recv()).await;
    assert!(
        matches!(
            after_stopped,
            Err(_) | Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
        ),
        "SupervisorStopped must be the final supervisor event: {after_stopped:?}"
    );
}

#[tokio::test]
async fn a_completion_set_waits_for_its_last_child() {
    let (first_tx, first_rx) = oneshot::channel::<()>();
    let first_rx = Arc::new(std::sync::Mutex::new(Some(first_rx)));
    let (second_tx, second_rx) = oneshot::channel::<()>();
    let second_rx = Arc::new(std::sync::Mutex::new(Some(second_rx)));

    let builder = SupervisorBuilder::new()
        .child(
            ChildSpec::new("first", move |_| {
                let rx = first_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(RestartPolicy::Never),
        )
        .child(
            ChildSpec::new("second", move |_| {
                let rx = second_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(RestartPolicy::Never),
        );
    let _finished = builder.handle().shutdown_on_completion(["first", "second"]);
    let supervisor = builder.build().expect("valid supervisor");

    let handle = supervisor.spawn();
    first_tx.send(()).expect("first child dropped");
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "one completed child must not be enough"
    );
    second_tx.send(()).expect("second child dropped");
    handle
        .wait()
        .await
        .expect("the last completion should stop cleanly");
}

#[tokio::test]
async fn a_failure_restarts_before_a_clean_exit_completes() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_child = attempts.clone();
    let builder = SupervisorBuilder::new().child(ChildSpec::new("trigger", move |_| {
        let attempt = attempts_for_child.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Err("first attempt fails".into())
            } else {
                Ok(())
            }
        }
    }));
    let _finished = builder.handle().shutdown_on_completion(["trigger"]);

    builder
        .build()
        .expect("valid supervisor")
        .spawn()
        .wait()
        .await
        .expect("second clean exit should stop supervisor");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn wait_completed_reports_a_supervisor_that_stopped_first() {
    let builder = SupervisorBuilder::new().child(ChildSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let spawned = builder.build().expect("valid supervisor").spawn();

    let waiter = tokio::spawn(async move { handle.wait_completed(["worker"]).await });
    spawned
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");

    let outcome = timeout(common::EVENT_TIMEOUT, waiter)
        .await
        .expect("the wait must resolve once the identity is terminal")
        .expect("waiter task panicked");
    assert_eq!(outcome, CompletionOutcome::Closed);
}

#[tokio::test]
async fn an_empty_completion_set_is_already_satisfied() {
    let builder = SupervisorBuilder::new().child(ChildSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let spawned = builder.build().expect("valid supervisor").spawn();

    let outcome = timeout(
        common::EVENT_TIMEOUT,
        handle.wait_completed(Vec::<String>::new()),
    )
    .await
    .expect("an empty set must not block");
    assert_eq!(outcome, CompletionOutcome::Completed);
    spawned
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn a_nested_scope_completion_is_a_clean_child_exit_to_parent() {
    let inner_builder =
        SupervisorBuilder::new().child(ChildSpec::new("done", |_| async { Ok(()) }));
    let _finished = inner_builder.handle().shutdown_on_completion(["done"]);
    let inner = inner_builder.build().expect("valid inner supervisor");

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
async fn a_completed_nested_scope_can_complete_its_parent() {
    let inner_builder =
        SupervisorBuilder::new().child(ChildSpec::new("done", |_| async { Ok(()) }));
    let _inner_finished = inner_builder.handle().shutdown_on_completion(["done"]);
    let inner = inner_builder.build().expect("valid inner supervisor");

    let parent_builder = SupervisorBuilder::new().supervisor("job", SupervisorSpec::new(inner));
    let _parent_finished = parent_builder.handle().shutdown_on_completion(["job"]);

    parent_builder
        .build()
        .expect("valid parent supervisor")
        .spawn()
        .wait()
        .await
        .expect("nested completion should stop the parent");
}

#[tokio::test]
async fn a_dynamic_scope_can_await_completion() {
    let builder = DynamicSupervisorBuilder::new().restart(RestartPolicy::Never);
    // Armed before the children exist: an id that is not yet a member stays
    // pending rather than counting as already gone.
    let _finished = builder.handle().shutdown_on_completion(["first", "second"]);
    let spawned = builder.build().expect("valid dynamic supervisor").spawn();

    let gate = Arc::new(Notify::new());
    spawned
        .add_child(ChildSpec::new("first", |_| async { Ok(()) }))
        .await
        .expect("first added");
    spawned
        .add_child(ChildSpec::new("second", {
            let gate = gate.clone();
            move |_| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    Ok(())
                }
            }
        }))
        .await
        .expect("second added");

    assert!(
        timeout(common::QUIET_TIMEOUT, spawned.wait())
            .await
            .is_err(),
        "the dynamic scope must wait for its second child"
    );
    gate.notify_one();
    spawned
        .wait()
        .await
        .expect("both dynamic children completed");
}

#[tokio::test]
async fn a_failed_never_child_never_completes() {
    let builder = SupervisorBuilder::new()
        .child(
            ChildSpec::new("failed", |_| async { Err(common::test_error("failed")) })
                .restart(RestartPolicy::Never),
        )
        .child(ChildSpec::new("completed", |_| async { Ok(()) }).restart(RestartPolicy::Never));
    let _finished = builder
        .handle()
        .shutdown_on_completion(["failed", "completed"]);
    let supervisor = builder.build().expect("valid supervisor");

    let handle = supervisor.spawn();
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a failed Never child must not count as finished work"
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("explicit shutdown succeeds");
}

#[tokio::test]
async fn a_group_restart_invalidates_a_stale_completion() {
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
    });
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
    });
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

    let builder = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(a)
        .child(b)
        .child(failing);
    let _finished = builder.handle().shutdown_on_completion(["a", "b"]);
    let handle = builder.build().expect("valid supervisor").spawn();

    fail_group.notify_one();
    let mut restarted = common::recv_n(&mut restarted_rx, 2).await;
    restarted.sort_unstable();
    assert_eq!(restarted, ["a", "b"]);

    finish_b.notify_one();
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a restarted child's earlier completion must not still count"
    );
    finish_a.notify_one();
    handle
        .wait()
        .await
        .expect("both current generations completed cleanly");
}

#[tokio::test]
async fn a_group_cancelled_clean_exit_does_not_complete() {
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
    .restart(RestartPolicy::Never);
    // Returns `Ok(())` because the supervisor cancelled it, not because its
    // work finished. That must not satisfy the completion set.
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
    });
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

    let builder = SupervisorBuilder::new()
        .strategy(Strategy::OneForAll)
        .child(natural)
        .child(restarted)
        .child(failing);
    let _finished = builder
        .handle()
        .shutdown_on_completion(["natural", "restarted"]);
    let handle = builder.build().expect("valid supervisor").spawn();

    common::recv_event(&mut restarted_rx).await;
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a cancellation-driven clean exit must not complete the set"
    );
    finish_restarted.notify_one();
    handle
        .wait()
        .await
        .expect("the restarted child then completed on its own");
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
async fn a_dropped_guard_leaves_the_supervisor_running() {
    let builder = SupervisorBuilder::new()
        .child(ChildSpec::new("trigger", |_| async { Ok(()) }).restart(RestartPolicy::Never))
        .child(ChildSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    let guard = builder.handle().shutdown_on_completion(["trigger"]);
    drop(guard);
    let handle = builder.build().expect("valid supervisor").spawn();

    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a cancelled completion watch must not stop the supervisor"
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("explicit shutdown succeeds");
}

#[tokio::test]
async fn a_retained_guard_does_not_keep_a_root_alive() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let builder = SupervisorBuilder::new().child(ChildSpec::new("worker", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver dropped");
            Ok(())
        }
    }));
    // The watch task holds no lifecycle lease, so dropping every public handle
    // must still request shutdown even while the guard is retained.
    let _finished = builder.handle().shutdown_on_completion(["worker"]);
    drop(builder.build().expect("valid supervisor").spawn());

    common::recv_event(&mut cancelled_rx).await;
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
