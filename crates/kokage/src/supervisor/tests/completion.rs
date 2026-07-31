//! Stopping a scope once its finite work is done.
//!
//! These cover direct completion waits and completion-triggered shutdown.

use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{
    ChildSpec, ChildStateView, CompletionError, Restart, Shutdown, Strategy, Supervisor,
    SupervisorError, TaskSpec,
};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    time::timeout,
};

use super::common;
use common::{ExitStatusView, ObservedEvent};

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn a_completed_child_stops_siblings_and_supervisor() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let builder = Supervisor::ordered()
        .child(TaskSpec::new("trigger", |_| async { Ok(()) }).restart(Restart::never()))
        .child(TaskSpec::new("sibling", move |ctx| {
            let cancelled_tx = cancelled_tx.clone();
            async move {
                ctx.shutdown_token().cancelled().await;
                cancelled_tx.send(()).expect("test receiver dropped");
                Ok(())
            }
        }));
    let completion = builder
        .handle()
        .shutdown_when_children_complete(["trigger"])
        .expect("completion condition is valid");
    assert!(
        !completion.is_finished(),
        "an armed completion has not finished before the supervisor runs"
    );
    let supervisor = builder.build().expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);
    handle.wait().await.expect("completion should stop cleanly");
    common::recv_event(&mut cancelled_rx).await;
    timeout(common::EVENT_TIMEOUT, completion.finished())
        .await
        .expect("completion guard reports natural completion");
    assert!(
        !completion.is_cancelled(),
        "natural completion is distinct from cancellation"
    );

    let mut sequence = Vec::new();
    while let Ok(event) = events.recv().await {
        match event {
            ObservedEvent::ChildExited { id, .. } if id == "trigger" => {
                sequence.push("exited");
            }
            ObservedEvent::SupervisorStopping => sequence.push("stopping"),
            ObservedEvent::SupervisorStopped => sequence.push("stopped"),
            _ => {}
        }
    }
    assert_eq!(sequence, ["exited", "stopping", "stopped"]);
}

#[tokio::test]
async fn a_completion_set_waits_for_its_last_child() {
    let (first_tx, first_rx) = oneshot::channel::<()>();
    let first_rx = Arc::new(std::sync::Mutex::new(Some(first_rx)));
    let (second_tx, second_rx) = oneshot::channel::<()>();
    let second_rx = Arc::new(std::sync::Mutex::new(Some(second_rx)));

    let builder = Supervisor::ordered()
        .child(
            TaskSpec::new("first", move |_| {
                let rx = first_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(Restart::never()),
        )
        .child(
            TaskSpec::new("second", move |_| {
                let rx = second_rx.lock().expect("lock poisoned").take().unwrap();
                async move {
                    rx.await.expect("gate dropped");
                    Ok(())
                }
            })
            .restart(Restart::never()),
        );
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["first", "second"])
        .expect("completion condition is valid");
    let supervisor = builder.build().expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
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
    let builder = Supervisor::ordered().child(TaskSpec::new("trigger", move |_| {
        let attempt = attempts_for_child.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Err("first attempt fails".into())
            } else {
                Ok(())
            }
        }
    }));
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["trigger"])
        .expect("completion condition is valid");

    let running = builder.build().expect("valid supervisor").spawn();
    running
        .handle()
        .wait()
        .await
        .expect("second clean exit should stop supervisor");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn wait_completed_reports_a_supervisor_that_stopped_first() {
    let builder = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let spawned_owner = builder.build().expect("valid supervisor").spawn();
    let spawned = spawned_owner.handle();

    let waiter = tokio::spawn(async move { handle.wait_for_children(["worker"]).await });
    spawned
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");

    let outcome = timeout(common::EVENT_TIMEOUT, waiter)
        .await
        .expect("the wait must resolve once the identity is terminal")
        .expect("waiter task panicked");
    assert_eq!(outcome, Err(CompletionError::ScopeClosed));
}

#[tokio::test]
async fn an_empty_completion_set_is_already_satisfied() {
    let builder = Supervisor::ordered().child(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let handle = builder.handle();
    let spawned_owner = builder.build().expect("valid supervisor").spawn();
    let spawned = spawned_owner.handle();

    let outcome = timeout(
        common::EVENT_TIMEOUT,
        handle.wait_for_children(Vec::<String>::new()),
    )
    .await
    .expect("an empty set must not block");
    assert_eq!(outcome, Ok(()));
    spawned
        .shutdown_and_wait()
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn wait_completed_realigns_from_a_clean_pre_ready_exit() {
    let handle_owner = Supervisor::ordered()
        .child(
            TaskSpec::new("worker", |_| async { Ok(()) })
                .restart(Restart::never())
                .wait_for_ready(),
        )
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert!(matches!(
        timeout(common::EVENT_TIMEOUT, handle.wait_started())
            .await
            .expect("startup result arrives"),
        Err(SupervisorError::StartupAborted(_))
    ));
    assert!(matches!(
        handle
            .snapshot()
            .child("worker")
            .expect("worker remains in the snapshot")
            .state,
        ChildStateView::StartupAborted { .. }
    ));

    let outcome = timeout(common::EVENT_TIMEOUT, handle.wait_for_children(["worker"]))
        .await
        .expect("snapshot realignment recognizes the completed exit");
    assert_eq!(outcome, Ok(()));

    let _finished = handle
        .shutdown_when_children_complete(["worker"])
        .expect("completion condition is valid");
    timeout(common::EVENT_TIMEOUT, handle.wait())
        .await
        .expect("the completion guard also realigns and requests shutdown")
        .expect("completion-driven shutdown succeeds");
}

#[tokio::test]
async fn dynamic_completion_realigns_after_real_lifecycle_overflow() {
    let running = Supervisor::dynamic()
        .build()
        .expect("supervisor builds")
        .spawn();
    let handle = running.handle();
    let dynamic = handle.dynamic().expect("dynamic capability is present");
    let mut wait = Box::pin(handle.wait_for_future_children(["target"]));
    assert!(
        timeout(common::QUIET_TIMEOUT, &mut wait).await.is_err(),
        "the wait is armed before future membership appears"
    );

    dynamic
        .add_child(TaskSpec::new("target", |_| async { Ok(()) }).restart(Restart::never()))
        .await
        .expect("target is added");
    for index in 0..70 {
        let id = format!("churn-{index}");
        dynamic
            .add_child(TaskSpec::new(id.clone(), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }))
            .await
            .expect("churn child is added");
        dynamic
            .remove_child(id)
            .await
            .expect("churn child is removed");
    }

    assert_eq!(
        timeout(common::EVENT_TIMEOUT, wait)
            .await
            .expect("overflow realignment completes"),
        Ok(())
    );
    running.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_nested_scope_completion_is_a_clean_child_exit_to_parent() {
    let inner_builder = Supervisor::ordered().child(TaskSpec::new("done", |_| async { Ok(()) }));
    let _finished = inner_builder
        .handle()
        .shutdown_when_children_complete(["done"])
        .expect("completion condition is valid");
    let inner = inner_builder.build().expect("valid inner supervisor");

    let parent = Supervisor::ordered()
        .child_spec(ChildSpec::supervisor("job", inner).restart(Restart::on_failure()))
        .build()
        .expect("valid parent supervisor");

    let handle_owner = parent.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);
    let event = loop {
        let event = common::recv_supervisor_event(&mut events).await;
        if matches!(
            &event,
            ObservedEvent::ChildExited {
                id,
                status: ExitStatusView::Completed,
                ..
            } if id == "job"
        ) {
            break event;
        }
    };
    assert!(matches!(event, ObservedEvent::ChildExited { .. }));
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
    let inner_builder = Supervisor::ordered().child(TaskSpec::new("done", |_| async { Ok(()) }));
    let _inner_finished = inner_builder
        .handle()
        .shutdown_when_children_complete(["done"])
        .expect("completion condition is valid");
    let inner = inner_builder.build().expect("valid inner supervisor");

    let parent_builder = Supervisor::ordered().child_spec(ChildSpec::supervisor("job", inner));
    let _parent_finished = parent_builder
        .handle()
        .shutdown_when_children_complete(["job"])
        .expect("completion condition is valid");

    let running = parent_builder
        .build()
        .expect("valid parent supervisor")
        .spawn();
    running
        .handle()
        .wait()
        .await
        .expect("nested completion should stop the parent");
}

#[tokio::test]
async fn a_dynamic_scope_can_await_completion() {
    let builder = Supervisor::dynamic().default_restart(Restart::never());
    // Armed before the children exist: an id that is not yet a member stays
    // pending rather than counting as already gone.
    let finished = builder
        .handle()
        .shutdown_when_future_children_complete(["first", "second"])
        .expect("completion condition is valid");
    assert!(
        !finished.is_finished(),
        "an armed dynamic completion has not finished before its members exist"
    );
    let spawned_owner = builder.build().expect("valid dynamic supervisor").spawn();
    let spawned = spawned_owner.handle();

    let gate = Arc::new(Notify::new());
    spawned
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("first", |_| async { Ok(()) }))
        .await
        .expect("first added");
    spawned
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("second", {
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
    let builder = Supervisor::ordered()
        .child(
            TaskSpec::new("failed", |_| async { Err(common::test_error("failed")) })
                .restart(Restart::never()),
        )
        .child(TaskSpec::new("completed", |_| async { Ok(()) }).restart(Restart::never()));
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["failed", "completed"])
        .expect("completion condition is valid");
    let supervisor = builder.build().expect("valid supervisor");

    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
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

    let a = TaskSpec::new("a", {
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
    let b = TaskSpec::new("b", {
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
    let failing = TaskSpec::new("failing", {
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

    let builder = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(a)
        .child(b)
        .child(failing);
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["a", "b"])
        .expect("completion condition is valid");
    let handle_owner = builder.build().expect("valid supervisor").spawn();
    let handle = handle_owner.handle();

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

    let natural = TaskSpec::new("natural", {
        let finish_natural = finish_natural.clone();
        move |_| {
            let finish_natural = finish_natural.clone();
            async move {
                finish_natural.notified().await;
                Ok(())
            }
        }
    })
    .restart(Restart::never());
    // Returns `Ok(())` because the supervisor cancelled it, not because its
    // work finished. That must not satisfy the completion set.
    let restarted = TaskSpec::new("restarted", move |ctx| {
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
    let failing = TaskSpec::new("failing", move |ctx| {
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

    let builder = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(natural)
        .child(restarted)
        .child(failing);
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["natural", "restarted"])
        .expect("completion condition is valid");
    let handle_owner = builder.build().expect("valid supervisor").spawn();
    let handle = handle_owner.handle();

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
    let always = TaskSpec::new("always", {
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
    .restart(Restart::always());
    let failing = TaskSpec::new("failing", move |ctx| {
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

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(always)
        .child(failing)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut restarted_rx).await, 1);
    common::assert_no_event(&mut restarted_rx).await;
    handle
        .shutdown_and_wait()
        .await
        .expect("single restarted generation should shut down cleanly");
}

#[tokio::test]
async fn a_clean_exit_with_always_policy_never_satisfies_completion() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let child_attempts = Arc::clone(&attempts);
    let builder = Supervisor::ordered().child(
        TaskSpec::new("service", move |ctx| {
            let attempts = Arc::clone(&child_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(());
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })
        .restart(Restart::always()),
    );
    let handle = builder.handle();
    let owner = builder.build().expect("valid supervisor").spawn();

    timeout(common::EVENT_TIMEOUT, async {
        while attempts.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("always child restarts");
    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait_for_children(["service"]),)
            .await
            .is_err(),
        "a service that will always restart is not finite completed work"
    );

    owner.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn a_dropped_guard_leaves_the_supervisor_running() {
    let builder = Supervisor::ordered()
        .child(TaskSpec::new("trigger", |_| async { Ok(()) }).restart(Restart::never()))
        .child(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    let guard = builder
        .handle()
        .shutdown_when_children_complete(["trigger"])
        .expect("completion condition is valid");
    drop(guard);
    let handle_owner = builder.build().expect("valid supervisor").spawn();
    let handle = handle_owner.handle();

    assert!(
        timeout(common::QUIET_TIMEOUT, handle.wait()).await.is_err(),
        "a cancelled completion operation must not stop the supervisor"
    );
    handle
        .shutdown_and_wait()
        .await
        .expect("explicit shutdown succeeds");
}

#[tokio::test]
async fn a_detached_guard_preserves_completion_shutdown() {
    let builder = Supervisor::ordered()
        .child(TaskSpec::new("trigger", |_| async { Ok(()) }).restart(Restart::never()))
        .child(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    builder
        .handle()
        .shutdown_when_children_complete(["trigger"])
        .expect("completion condition is valid")
        .detach();
    let owner = builder.build().expect("valid supervisor").spawn();

    timeout(common::EVENT_TIMEOUT, owner.handle().wait())
        .await
        .expect("detached completion guard still requests shutdown")
        .expect("completion shutdown succeeds");
}

#[tokio::test]
async fn a_retained_guard_does_not_keep_a_root_alive() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let builder = Supervisor::ordered().child(TaskSpec::new("worker", move |ctx| {
        let cancelled_tx = cancelled_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            cancelled_tx.send(()).expect("test receiver dropped");
            Ok(())
        }
    }));
    // The watch task holds no lifecycle ownership, so dropping the explicit
    // root owner still requests shutdown even while the guard is retained.
    let _finished = builder
        .handle()
        .shutdown_when_children_complete(["worker"])
        .expect("completion condition is valid");
    drop(builder.build().expect("valid supervisor").spawn());

    common::recv_event(&mut cancelled_rx).await;
}

#[tokio::test]
async fn fatal_restart_during_abort_removal_stops_supervisor() {
    let fail = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let removable = TaskSpec::new("removable", {
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
    .shutdown(Shutdown::abort());
    let failing = TaskSpec::new("failing", move |_| {
        let fail = fail.clone();
        let started_tx = started_tx.clone();
        async move {
            started_tx.send(()).expect("test receiver dropped");
            fail.notified().await;
            Err(common::test_error("fatal restart"))
        }
    });

    let handle_owner = Supervisor::dynamic()
        .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1)))
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();
    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(removable)
        .await
        .expect("removable added");
    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(failing)
        .await
        .expect("failing added");
    common::recv_n(&mut started_rx, 2).await;

    let _ = handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("removable")
        .await;
    let result = timeout(common::EVENT_TIMEOUT, handle.wait())
        .await
        .expect("fatal restart observed during removal must stop supervisor");
    assert_eq!(result, Err(SupervisorError::RestartIntensityExceeded));
}
