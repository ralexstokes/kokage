use std::{ops::Deref, sync::Arc, time::Duration};

use crate::supervisor::{
    BuildError, ControlError, DynamicSupervisorBuilder, Restart, RunningSupervisor, ScopeKind,
    Shutdown, Supervisor, SupervisorError, SupervisorHandle, TaskSpec,
};
use tokio::{
    sync::{Notify, mpsc},
    time::{sleep, timeout},
};

use super::common;
use common::{ExitStatusView, ObservedEvent};

async fn spawn_dynamic(
    builder: DynamicSupervisorBuilder,
    children: impl IntoIterator<Item = TaskSpec>,
) -> SpawnedSupervisor {
    let owner = builder.build().expect("valid dynamic supervisor").spawn();
    let handle = owner.handle();
    for child in children {
        handle
            .dynamic()
            .expect("dynamic supervisor")
            .add_child(child)
            .await
            .expect("initial dynamic child should be accepted");
    }
    SpawnedSupervisor {
        _owner: owner,
        handle,
    }
}

/// Test fixture that retains the linear owner while exercising a cloned
/// handle. Production callers make this transition explicitly with
/// `RunningSupervisor::handle`.
struct SpawnedSupervisor {
    _owner: RunningSupervisor,
    handle: SupervisorHandle,
}

impl Deref for SpawnedSupervisor {
    type Target = SupervisorHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

#[test]
fn empty_supervisors_are_valid() {
    Supervisor::ordered()
        .build()
        .expect("empty ordered supervisors are valid");
    Supervisor::dynamic()
        .build()
        .expect("empty dynamic supervisors are valid");
}

#[tokio::test]
async fn ordered_handles_have_no_membership_capability() {
    let running = Supervisor::ordered()
        .child(TaskSpec::new("declared", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("ordered supervisor builds")
        .spawn();
    let handle = running.handle();

    assert_eq!(handle.snapshot().kind, ScopeKind::Ordered);
    assert!(handle.dynamic().is_none());

    common::shutdown_and_wait(&handle, "dynamic add/remove test shutdown")
        .await
        .expect("clean shutdown");
}

#[tokio::test]
async fn empty_supervisor_starts_empty_and_accepts_children() {
    let supervisor = Supervisor::dynamic()
        .build()
        .expect("empty supervisor builds");
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    assert!(handle.snapshot().children.is_empty());
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("dynamic", |_ctx| async move { Ok(()) }).restart(Restart::never()))
        .await
        .expect("empty supervisor accepts a child");

    let mut saw_started = false;
    let mut saw_exited = false;
    while !saw_exited {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildStarted { id, .. } if id == "dynamic" => {
                saw_started = true;
            }
            ObservedEvent::ChildExited { id, status, .. } if id == "dynamic" => {
                assert!(saw_started, "child should start before exiting");
                assert_eq!(status, ExitStatusView::Completed);
                saw_exited = true;
            }
            ObservedEvent::SupervisorStopped => {
                panic!("empty supervisor stopped instead of idling");
            }
            _ => {}
        }
    }

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn dynamic_builder_defaults_apply_without_overriding_explicit_child_policy() {
    let (inherited_tx, mut inherited_rx) = mpsc::unbounded_channel();
    let inherited = TaskSpec::new("inherited", move |ctx| {
        let inherited_tx = inherited_tx.clone();
        async move {
            inherited_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() == 0 {
                Ok(())
            } else {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    });
    let (explicit_tx, mut explicit_rx) = mpsc::unbounded_channel();
    let explicit = TaskSpec::new("explicit", move |ctx| {
        let explicit_tx = explicit_tx.clone();
        async move {
            explicit_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            Ok(())
        }
    })
    .restart(Restart::never());
    let handle_owner = Supervisor::dynamic()
        .default_restart(Restart::always())
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    let handle = handle_owner.handle();

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(inherited)
        .await
        .expect("inherited child added");
    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(explicit)
        .await
        .expect("explicit child added");
    assert_eq!(common::recv_n(&mut inherited_rx, 2).await, [0, 1]);
    assert_eq!(common::recv_event(&mut explicit_rx).await, 0);
    common::assert_no_event(&mut explicit_rx).await;

    common::shutdown_and_wait(&handle, "dynamic policy test shutdown")
        .await
        .expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_child_can_remove_itself_after_a_non_restarted_exit() {
    let supervisor = Supervisor::dynamic()
        .build()
        .expect("empty supervisor builds");
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(
            TaskSpec::new("temporary", |_ctx| async move { Ok(()) })
                .restart(Restart::never().remove_when_done()),
        )
        .await
        .expect("temporary child added");

    let mut exited = false;
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, .. } if id == "temporary" => exited = true,
            ObservedEvent::ChildRemoved { id, .. } if id == "temporary" => {
                assert!(exited, "exit is published before membership removal");
                break;
            }
            _ => {}
        }
    }
    assert!(handle.snapshot().child("temporary").is_none());

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("temporary", |_ctx| async move { Ok(()) }))
        .await
        .expect("auto-removed child id is reusable");
    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn temporary_dynamic_child_auto_removes_when_skipped_by_group_restart() {
    let trigger = Arc::new(Notify::new());
    let supervisor = Supervisor::ordered()
        .strategy(crate::supervisor::Strategy::OneForAll)
        .child(
            TaskSpec::new("temporary", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .restart(Restart::never().remove_when_done()),
        )
        .child(
            TaskSpec::new("trigger", {
                let trigger = trigger.clone();
                move |_ctx| {
                    let trigger = trigger.clone();
                    async move {
                        trigger.notified().await;
                        Err(common::test_error("restart group"))
                    }
                }
            })
            .shutdown(Shutdown::abort()),
        )
        .build()
        .expect("ordered group supervisor builds");
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    trigger.notify_one();
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRemoved { id, .. } if id == "temporary" => break,
            _ => {}
        }
    }
    assert!(handle.snapshot().child("temporary").is_none());

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn opted_in_non_never_exit_before_group_restart_forfeits_revival() {
    let finish_temporary = Arc::new(Notify::new());
    let fail_trigger = Arc::new(Notify::new());
    let (temporary_starts_tx, mut temporary_starts_rx) = mpsc::unbounded_channel();
    let supervisor = Supervisor::ordered()
        .strategy(crate::supervisor::Strategy::OneForAll)
        .child(
            TaskSpec::new("temporary", {
                let finish_temporary = finish_temporary.clone();
                move |ctx| {
                    let finish_temporary = finish_temporary.clone();
                    let temporary_starts_tx = temporary_starts_tx.clone();
                    async move {
                        temporary_starts_tx
                            .send(ctx.generation())
                            .expect("test receiver dropped");
                        finish_temporary.notified().await;
                        Ok(())
                    }
                }
            })
            .restart(Restart::on_failure().remove_when_done()),
        )
        .child(TaskSpec::new("trigger", {
            let fail_trigger = fail_trigger.clone();
            move |ctx| {
                let fail_trigger = fail_trigger.clone();
                async move {
                    if ctx.generation() == 0 {
                        fail_trigger.notified().await;
                        return Err(common::test_error("restart group"));
                    }
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }))
        .build()
        .expect("ordered group supervisor builds");
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);

    assert_eq!(common::recv_event(&mut temporary_starts_rx).await, 0);
    finish_temporary.notify_one();
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRemoved { id, .. } if id == "temporary" => break,
            _ => {}
        }
    }

    fail_trigger.notify_one();
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestarted {
                id,
                new_generation: 1,
                ..
            } if id == "trigger" => break,
            _ => {}
        }
    }
    common::assert_no_event(&mut temporary_starts_rx).await;
    assert!(handle.snapshot().child("temporary").is_none());

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn opted_in_non_never_exit_during_group_drain_is_respawned() {
    let fail_trigger = Arc::new(Notify::new());
    let (temporary_starts_tx, mut temporary_starts_rx) = mpsc::unbounded_channel();
    let supervisor = Supervisor::ordered()
        .strategy(crate::supervisor::Strategy::OneForAll)
        .child(
            TaskSpec::new("temporary", move |ctx| {
                let temporary_starts_tx = temporary_starts_tx.clone();
                async move {
                    temporary_starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(Restart::on_failure().remove_when_done()),
        )
        .child(
            TaskSpec::new("trigger", {
                let fail_trigger = fail_trigger.clone();
                move |ctx| {
                    let fail_trigger = fail_trigger.clone();
                    async move {
                        if ctx.generation() == 0 {
                            fail_trigger.notified().await;
                            return Err(common::test_error("restart group"));
                        }
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .restart(Restart::on_failure()),
        )
        .build()
        .expect("ordered group supervisor builds");
    let handle_owner = supervisor.spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut temporary_starts_rx).await, 0);
    fail_trigger.notify_one();
    assert_eq!(common::recv_event(&mut temporary_starts_rx).await, 1);
    assert!(
        handle
            .snapshot()
            .child("temporary")
            .is_some_and(|child| child.generation == 1),
        "completion during a group drain remains part of the restart cycle"
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn remove_last_child_and_readd_same_id() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let initial_starts_tx = starts_tx.clone();

    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("dynamic", move |ctx| {
            let starts_tx = initial_starts_tx.clone();
            async move {
                starts_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        })],
    )
    .await;
    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    let initial_lineage = handle
        .snapshot()
        .child("dynamic")
        .expect("initial child visible")
        .lineage;

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("dynamic")
        .await
        .expect("last child removal should be allowed");
    assert!(handle.snapshot().children.is_empty());

    let mut events = common::event_watch(&handle);
    let replacement_lineage = handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("dynamic", move |_ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx.send(0).expect("test receiver dropped");
                Ok(())
            }
        }))
        .await
        .expect("removed child id should be reusable");
    assert!(
        replacement_lineage > initial_lineage,
        "re-adding an id must return a distinct lineage"
    );
    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    let replacement = handle.snapshot();
    let replacement = replacement
        .child("dynamic")
        .expect("replacement child visible");
    assert_eq!(replacement.generation, 0);
    assert_eq!(replacement.lineage, replacement_lineage);
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, status, .. } if id == "dynamic" => {
                assert_eq!(status, ExitStatusView::Completed);
                break;
            }
            ObservedEvent::SupervisorStopped => {
                panic!("supervisor stopped after re-added child exited");
            }
            _ => {}
        }
    }

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn transient_success_idles_until_shutdown() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let release_for_child = release.clone();

    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("transient", move |_ctx| {
            let started_tx = started_tx.clone();
            let release = release_for_child.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                release.notified().await;
                Ok(())
            }
        })],
    )
    .await;
    let mut events = common::event_watch(&handle);
    common::recv_event(&mut started_rx).await;
    release.notify_one();

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, status, .. } if id == "transient" => {
                assert_eq!(status, ExitStatusView::Completed);
                break;
            }
            ObservedEvent::SupervisorStopped => {
                panic!("supervisor stopped on transient completion");
            }
            _ => {}
        }
    }

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("probe", |_ctx| async move { Ok(()) }))
        .await
        .expect("supervisor should still accept children after transient completion");

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn terminal_failure_remains_visible_while_idle() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let release_for_child = release.clone();

    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("fails", move |_ctx| {
            let started_tx = started_tx.clone();
            let release = release_for_child.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                release.notified().await;
                Err(common::test_error("terminal failure"))
            }
        })
        .restart(Restart::never())],
    )
    .await;
    let mut events = common::event_watch(&handle);
    common::recv_event(&mut started_rx).await;
    release.notify_one();

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildExited { id, status, .. } if id == "fails" => {
                assert!(matches!(status, ExitStatusView::Failed(_)));
                break;
            }
            ObservedEvent::SupervisorStopped => {
                panic!("supervisor stopped on terminal failure");
            }
            _ => {}
        }
    }

    assert!(
        handle
            .snapshot()
            .child("fails")
            .expect("failed child remains visible")
            .state
            .last_exit()
            .and_then(|exit| exit.failure_message())
            .is_some_and(|message| message.contains("terminal failure"))
    );

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("probe", |_ctx| async move { Ok(()) }))
        .await
        .expect("supervisor should still accept children after terminal failure");

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn add_child_starts_it_immediately() {
    let (dynamic_tx, mut dynamic_rx) = mpsc::unbounded_channel();

    let handle = spawn_dynamic(Supervisor::dynamic(), []).await;

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("dynamic", move |ctx| {
            let dynamic_tx = dynamic_tx.clone();
            async move {
                dynamic_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .await
        .expect("dynamic child should be accepted");

    assert_eq!(common::recv_event(&mut dynamic_rx).await, 0);

    handle.shutdown();
    common::wait(&handle, "dynamic child startup test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn remove_child_stops_it_without_restarting() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [
            TaskSpec::new("removable", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    Err(common::test_error("do not restart on remove"))
                }
            }),
            TaskSpec::new("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    let mut events = common::event_watch(&handle);

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("removable")
        .await
        .expect("child removal should succeed");

    let mut saw_removed = false;
    while !saw_removed {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRemoved { id, .. } if id == "removable" => {
                saw_removed = true;
            }
            _ => {}
        }
    }

    while starts_rx.try_recv().is_ok() {}
    common::assert_no_event(&mut starts_rx).await;

    handle.shutdown();
    common::wait(&handle, "dynamic restart test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn duplicate_add_and_unknown_remove_are_rejected() {
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("seed", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })],
    )
    .await;

    let duplicate = handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("seed", |_ctx| async move { Ok(()) }))
        .await
        .expect_err("duplicate id should be rejected");
    assert_eq!(
        duplicate,
        ControlError::Rejected(BuildError::DuplicateChildId("seed".to_owned()))
    );

    let missing = handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("missing")
        .await
        .expect_err("unknown child id should be rejected");
    assert_eq!(missing, ControlError::UnknownChildId("missing".to_owned()));

    handle.shutdown();
    common::wait(&handle, "unknown child test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn removing_the_last_active_child_leaves_an_idle_supervisor() {
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("only", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })],
    )
    .await;

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("only")
        .await
        .expect("last child removal should succeed");
    assert!(handle.snapshot().children.is_empty());

    handle.shutdown();
    common::wait(&handle, "empty dynamic supervisor test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn concurrent_removal_requests_fail_fast_while_the_first_is_pending() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let release_for_child = release.clone();
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [
            TaskSpec::new("removable", move |ctx| {
                let started_tx = started_tx.clone();
                let cancelled_tx = cancelled_tx.clone();
                let release = release_for_child.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    cancelled_tx.send(()).expect("test receiver dropped");
                    release.notified().await;
                    Ok(())
                }
            })
            .restart(Restart::on_failure()),
            TaskSpec::new("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move {
        remove_handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable")
            .await
    });

    common::recv_event(&mut cancelled_rx).await;

    let second_remove_handle = handle.clone();
    let second_remove_task = tokio::spawn(async move {
        second_remove_handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable")
            .await
    });

    let err = timeout(common::EVENT_TIMEOUT, second_remove_task)
        .await
        .expect("second removal should be dispatched while the first is pending")
        .expect("second remove task should join")
        .expect_err("same-id removal should fail while removal is pending");
    assert_eq!(
        err,
        ControlError::ChildRemovalInProgress("removable".to_owned())
    );

    release.notify_one();
    remove_task
        .await
        .expect("remove task should join")
        .expect("first removal should succeed");

    handle.shutdown();
    common::wait(&handle, "concurrent removal test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn shutdown_completes_a_pending_removal_and_preserves_its_timeout() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let fast_shutdown = Shutdown::drain_for(common::SHORT_GRACE);

    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [
            TaskSpec::new("removable", move |ctx| {
                let started_tx = started_tx.clone();
                let cancelled_tx = cancelled_tx.clone();
                async move {
                    started_tx.send(()).expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    cancelled_tx.send(()).expect("test receiver dropped");
                    std::future::pending::<()>().await;
                    Ok(())
                }
            })
            .restart(Restart::on_failure())
            .shutdown(fast_shutdown),
            TaskSpec::new("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(fast_shutdown),
        ],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move {
        remove_handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable")
            .await
    });

    common::recv_event(&mut cancelled_rx).await;
    handle.shutdown();

    let removal = remove_task.await.expect("remove task should join");
    assert!(matches!(
        removal,
        Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(id))) if id == "removable"
    ));

    assert!(matches!(
        common::wait(&handle, "pending removal shutdown").await,
        Err(SupervisorError::ShutdownTimedOut(id)) if id == "removable"
    ));
}

#[tokio::test]
async fn fatal_exit_resolves_an_accepted_pending_removal() {
    let removable_started = Arc::new(Notify::new());
    let removable_cancelled = Arc::new(Notify::new());
    let failing_started = Arc::new(Notify::new());
    let fail_now = Arc::new(Notify::new());

    let handle = spawn_dynamic(
        Supervisor::dynamic()
            .default_restart(Restart::on_failure().limit(0, Duration::from_secs(1))),
        [
            TaskSpec::new("removable", {
                let removable_started = Arc::clone(&removable_started);
                let removable_cancelled = Arc::clone(&removable_cancelled);
                move |ctx| {
                    let removable_started = Arc::clone(&removable_started);
                    let removable_cancelled = Arc::clone(&removable_cancelled);
                    async move {
                        removable_started.notify_one();
                        ctx.shutdown_token().cancelled().await;
                        removable_cancelled.notify_one();
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                }
            })
            .shutdown(Shutdown::drain_for(Duration::from_secs(5))),
            TaskSpec::new("failing", {
                let failing_started = Arc::clone(&failing_started);
                let fail_now = Arc::clone(&fail_now);
                move |_| {
                    let failing_started = Arc::clone(&failing_started);
                    let fail_now = Arc::clone(&fail_now);
                    async move {
                        failing_started.notify_one();
                        fail_now.notified().await;
                        Err(common::test_error("fatal restart"))
                    }
                }
            }),
        ],
    )
    .await;

    removable_started.notified().await;
    failing_started.notified().await;
    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move {
        remove_handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable")
            .await
    });
    removable_cancelled.notified().await;
    fail_now.notify_one();

    let remove_result = timeout(common::EVENT_TIMEOUT, remove_task)
        .await
        .expect("accepted removal reply must not dangle")
        .expect("remove task should join");
    assert_eq!(remove_result, Err(ControlError::Unavailable));
    assert_eq!(
        common::wait(&handle, "fatal-exit shutdown result").await,
        Err(SupervisorError::RestartIntensityExceeded)
    );
}

#[tokio::test]
async fn distinct_add_proceeds_while_a_cooperative_removal_drains() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (bounce_tx, mut bounce_rx) = mpsc::channel(1);
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("retiring", move |ctx| {
            let started_tx = started_tx.clone();
            let bounce_tx = bounce_tx.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                for _ in 0..3 {
                    bounce_tx.send(()).await.expect("writer mailbox dropped");
                }
                Ok(())
            }
        })
        .shutdown(Shutdown::drain_for(Duration::from_secs(1)))],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move {
        remove_handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("retiring")
            .await
    });
    common::recv_bounded_event(&mut bounce_rx).await;

    let same_id_error = handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("retiring", |_| async { Ok(()) }))
        .await
        .expect_err("same-id add must not queue behind removal");
    assert_eq!(
        same_id_error,
        ControlError::ChildRemovalInProgress("retiring".to_owned())
    );

    timeout(
        common::EVENT_TIMEOUT,
        handle
            .dynamic()
            .expect("dynamic supervisor")
            .add_child(TaskSpec::supervisor(
                "replacement",
                Supervisor::dynamic().build().expect("empty supervisor"),
            )),
    )
    .await
    .expect("distinct-id add should not queue behind the drain")
    .expect("replacement should be inserted");

    common::recv_bounded_event(&mut bounce_rx).await;
    common::recv_bounded_event(&mut bounce_rx).await;
    timeout(common::EVENT_TIMEOUT, remove_task)
        .await
        .expect("removal should finish before its grace expires")
        .expect("remove task should join")
        .expect("cooperative removal should succeed");

    common::shutdown_and_wait(&handle, "cooperative drain test shutdown")
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn distinct_removals_drain_independently() {
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel();
    let release_first = Arc::new(Notify::new());
    let release_second = Arc::new(Notify::new());
    let make_child = |id: &'static str, release: Arc<Notify>| {
        let cancelled_tx = cancelled_tx.clone();
        TaskSpec::new(id, move |ctx| {
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
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [
            make_child("first", Arc::clone(&release_first)),
            make_child("second", Arc::clone(&release_second)),
        ],
    )
    .await;
    common::wait_started(&handle, "independent removal children startup")
        .await
        .expect("children start");

    let first_remove = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .dynamic()
                .expect("dynamic supervisor")
                .remove_child("first")
                .await
        }
    });
    let second_remove = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .dynamic()
                .expect("dynamic supervisor")
                .remove_child("second")
                .await
        }
    });
    let mut cancelled = vec![
        common::recv_event(&mut cancelled_rx).await,
        common::recv_event(&mut cancelled_rx).await,
    ];
    cancelled.sort_unstable();
    assert_eq!(cancelled, vec!["first", "second"]);

    release_second.notify_one();
    timeout(common::EVENT_TIMEOUT, second_remove)
        .await
        .expect("second removal should not wait for first")
        .expect("second remove task should join")
        .expect("second removal succeeds");
    assert!(!first_remove.is_finished());
    release_first.notify_one();
    first_remove
        .await
        .expect("first remove task should join")
        .expect("first removal succeeds");

    common::shutdown_and_wait(&handle, "independent removals test shutdown")
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn cooperative_removal_reports_timeout_after_the_abort_join() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("stubborn", move |ctx| {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .shutdown(Shutdown::drain_for(common::SHORT_GRACE))],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let removal = timeout(
        Duration::from_secs(1),
        handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("stubborn"),
    )
    .await
    .expect("remove should finish after aborting the child");
    assert!(matches!(
        removal,
        Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(id))) if id == "stubborn"
    ));
    assert!(handle.snapshot().child("stubborn").is_none());

    common::shutdown_and_wait(&handle, "cooperative removal test shutdown")
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn control_plane_remains_available_after_all_children_exit() {
    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [TaskSpec::new("done", |_ctx| async move { Ok(()) }).restart(Restart::never())],
    )
    .await;

    let mut snapshots = handle.subscribe_snapshots();
    snapshots
        .wait_for(|snapshot| {
            snapshot
                .child("done")
                .is_some_and(|child| child.state.is_terminal())
        })
        .await
        .expect("completion snapshot remains available");

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("late", |_ctx| async move { Ok(()) }))
        .await
        .expect("control plane should remain available while idle");
    assert!(handle.snapshot().child("late").is_some());

    handle.shutdown();
    common::wait(&handle, "long-backoff removal test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn remove_child_completes_promptly_during_restart_backoff() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let handle = spawn_dynamic(
        Supervisor::dynamic().default_restart(common::restart_with_backoff(
            4,
            std::time::Duration::from_secs(60),
            crate::supervisor::Backoff::fixed(std::time::Duration::from_secs(30)),
        )),
        [
            TaskSpec::new("removable", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("restart me later"))
                }
            }),
            TaskSpec::new("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    let mut snapshots = handle.subscribe_snapshots();
    common::wait_for_snapshot(&mut snapshots, |snapshot| {
        snapshot
            .child("removable")
            .and_then(|child| child.next_restart_in)
            .is_some_and(|delay| delay > common::EVENT_TIMEOUT)
    })
    .await;

    timeout(
        common::EVENT_TIMEOUT,
        handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable"),
    )
    .await
    .expect("remove_child should not wait for the restart backoff")
    .expect("child removal should succeed during backoff");
    assert!(handle.snapshot().child("removable").is_none());
    while starts_rx.try_recv().is_ok() {}
    common::assert_no_event(&mut starts_rx).await;

    handle.shutdown();
    common::wait(&handle, "restart-backoff removal test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test(flavor = "current_thread")]
async fn remove_child_preempts_zero_delay_restart() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();

    let handle = spawn_dynamic(
        Supervisor::dynamic().default_restart(
            crate::supervisor::Restart::on_failure().limit(8, std::time::Duration::from_secs(1)),
        ),
        [TaskSpec::new("removable", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                Err(common::test_error("restart immediately"))
            }
        })],
    )
    .await;
    let mut events = common::event_watch(&handle);

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);
    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestartScheduled { id, delay, .. } if id == "removable" => {
                assert!(delay.is_zero(), "test requires zero-delay restart");
                break;
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("remove command lost to zero-delay restarts");
            }
            _ => {}
        }
    }

    timeout(
        common::EVENT_TIMEOUT,
        handle
            .dynamic()
            .expect("dynamic supervisor")
            .remove_child("removable"),
    )
    .await
    .expect("remove_child should beat the zero-delay restart")
    .expect("child removal should succeed");

    assert!(handle.snapshot().child("removable").is_none());
    while starts_rx.try_recv().is_ok() {}
    common::assert_no_event(&mut starts_rx).await;

    handle.shutdown();
    common::wait(&handle, "zero-delay removal test shutdown")
        .await
        .expect("shutdown should succeed");
}

#[tokio::test(flavor = "current_thread")]
async fn queued_command_batch_preempts_zero_delay_restart() {
    let handle_owner = Supervisor::dynamic()
        .default_restart(
            crate::supervisor::Restart::on_failure().limit(8, std::time::Duration::from_secs(1)),
        )
        .build()
        .expect("valid dynamic supervisor")
        .spawn();
    let handle = handle_owner.handle();
    let mut events = common::event_watch(&handle);
    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("removable", |_ctx| async move {
            Err(common::test_error("restart immediately"))
        }))
        .await
        .expect("initial dynamic child should be accepted");

    loop {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRestartScheduled { id, delay, .. } if id == "removable" => {
                assert!(delay.is_zero(), "test requires zero-delay restart");
                break;
            }
            ObservedEvent::RestartIntensityExceeded => {
                panic!("commands lost to zero-delay restarts");
            }
            _ => {}
        }
    }

    let replacement = TaskSpec::new("replacement", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let dynamic = handle.dynamic().expect("dynamic supervisor");
    let remove_dynamic = dynamic.clone();
    let (add_result, remove_result) = tokio::join!(
        biased;
        dynamic.add_child(replacement),
        remove_dynamic.remove_child("removable"),
    );
    add_result.expect("first queued command should add the replacement");
    remove_result.expect("second queued command should cancel the pending restart");

    let mut saw_removal = false;
    let mut saw_replacement_start = false;
    while !saw_removal || !saw_replacement_start {
        match common::recv_supervisor_event(&mut events).await {
            ObservedEvent::ChildRemoved { id } if id == "removable" => saw_removal = true,
            ObservedEvent::ChildStarted { id, .. } if id == "replacement" => {
                saw_replacement_start = true;
            }
            ObservedEvent::ChildRestarted { id, .. } if id == "removable" => {
                panic!("restart interleaved before the full queued command batch");
            }
            _ => {}
        }
    }

    assert!(handle.snapshot().child("removable").is_none());
    assert!(handle.snapshot().child("replacement").is_some());

    common::shutdown_and_wait(&handle, "queued command batch test shutdown")
        .await
        .expect("shutdown succeeds");
}

#[tokio::test]
async fn removed_child_does_not_restart_recycled_slot_after_backoff() {
    let (removable_tx, mut removable_rx) = mpsc::unbounded_channel();
    let (replacement_tx, mut replacement_rx) = mpsc::unbounded_channel();
    let backoff = std::time::Duration::from_millis(80);

    let handle = spawn_dynamic(
        Supervisor::dynamic().default_restart(common::restart_with_backoff(
            4,
            std::time::Duration::from_secs(1),
            crate::supervisor::Backoff::fixed(backoff),
        )),
        [
            TaskSpec::new("removable", move |ctx| {
                let removable_tx = removable_tx.clone();
                async move {
                    removable_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("restart me later"))
                }
            }),
            TaskSpec::new("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    let mut events = common::event_watch(&handle);

    assert_eq!(common::recv_event(&mut removable_rx).await, 0);

    loop {
        if matches!(
            common::recv_supervisor_event(&mut events).await,
            ObservedEvent::ChildRestartScheduled { id, .. } if id == "removable"
        ) {
            break;
        }
    }

    handle
        .dynamic()
        .expect("dynamic supervisor")
        .remove_child("removable")
        .await
        .expect("child removal should succeed during backoff");
    handle
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(TaskSpec::new("replacement", move |ctx| {
            let replacement_tx = replacement_tx.clone();
            async move {
                replacement_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .await
        .expect("replacement child should be accepted");

    assert_eq!(common::recv_event(&mut replacement_rx).await, 0);
    sleep(backoff + common::QUIET_TIMEOUT).await;
    common::assert_no_event(&mut replacement_rx).await;

    handle.shutdown();
    common::wait(&handle, "recycled slot test shutdown")
        .await
        .expect("shutdown should succeed");
}
