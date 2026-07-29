use std::{sync::Arc, time::Duration};

use kokage_supervisor::{
    ChildSpec, ControlError, DynamicSupervisorBuilder, ExitStatusView, RestartConfig,
    RestartPolicy, ScopeKind, ShutdownPolicy, Supervisor, SupervisorBuildError, SupervisorError,
    SupervisorHandle,
};
use tokio::{
    sync::{Notify, mpsc},
    time::{sleep, timeout},
};

mod common;
use common::ObservedEvent;

async fn spawn_dynamic(
    builder: DynamicSupervisorBuilder,
    children: impl IntoIterator<Item = ChildSpec>,
) -> SupervisorHandle {
    let handle = builder.build().expect("valid dynamic supervisor").spawn();
    for child in children {
        handle
            .add_child(child)
            .await
            .expect("initial dynamic child should be accepted");
    }
    handle
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
async fn ordered_membership_operations_report_the_scope_kind() {
    let handle = Supervisor::ordered()
        .child(ChildSpec::task("declared", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .build()
        .expect("ordered supervisor builds")
        .spawn();

    let add_child = handle
        .add_child(ChildSpec::task("runtime", |_| async { Ok(()) }))
        .await
        .expect_err("ordered add_child is rejected");
    assert_eq!(
        add_child,
        ControlError::UnsupportedByScopeKind {
            operation: "add_child",
            kind: ScopeKind::Ordered,
        }
    );

    let nested = Supervisor::ordered()
        .build()
        .expect("nested supervisor builds");
    let add_nested = handle
        .add_child(ChildSpec::supervisor("runtime-scope", nested))
        .await
        .expect_err("ordered add_child is rejected for nested supervisors too");
    assert_eq!(
        add_nested,
        ControlError::UnsupportedByScopeKind {
            operation: "add_child",
            kind: ScopeKind::Ordered,
        }
    );

    let remove_child = handle
        .remove_child("declared")
        .await
        .expect_err("ordered remove_child is rejected");
    assert_eq!(
        remove_child,
        ControlError::UnsupportedByScopeKind {
            operation: "remove_child",
            kind: ScopeKind::Ordered,
        }
    );

    common::shutdown_and_wait(&handle, "dynamic add/remove test shutdown")
        .await
        .expect("clean shutdown");
}

#[tokio::test]
async fn empty_supervisor_starts_empty_and_accepts_children() {
    let supervisor = Supervisor::dynamic()
        .build()
        .expect("empty supervisor builds");
    let handle = supervisor.spawn();
    let mut events = common::event_watch(&handle);

    assert!(handle.snapshot().children.is_empty());
    assert_eq!(handle.snapshot().kind, ScopeKind::Dynamic);

    handle
        .add_child(
            ChildSpec::task("dynamic", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never),
        )
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
    let inherited = ChildSpec::task("inherited", move |ctx| {
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
    let explicit = ChildSpec::task("explicit", move |ctx| {
        let explicit_tx = explicit_tx.clone();
        async move {
            explicit_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            Ok(())
        }
    })
    .restart(RestartPolicy::Never);
    let handle = Supervisor::dynamic()
        .restart(RestartPolicy::Always)
        .build()
        .expect("dynamic supervisor builds")
        .spawn();

    handle
        .add_child(inherited)
        .await
        .expect("inherited child added");
    handle
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
    let handle = supervisor.spawn();
    let mut events = common::event_watch(&handle);

    handle
        .add_child(
            ChildSpec::task("temporary", |_ctx| async move { Ok(()) })
                .restart(RestartPolicy::Never)
                .remove_on_exit(true),
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
        .add_child(ChildSpec::task("temporary", |_ctx| async move { Ok(()) }))
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
        .strategy(kokage_supervisor::Strategy::OneForAll)
        .child(
            ChildSpec::task("temporary", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .restart(RestartPolicy::Never)
            .remove_on_exit(true),
        )
        .child(
            ChildSpec::task("trigger", {
                let trigger = trigger.clone();
                move |_ctx| {
                    let trigger = trigger.clone();
                    async move {
                        trigger.notified().await;
                        Err(common::test_error("restart group"))
                    }
                }
            })
            .shutdown(ShutdownPolicy::abort()),
        )
        .build()
        .expect("ordered group supervisor builds");
    let handle = supervisor.spawn();
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
        .strategy(kokage_supervisor::Strategy::OneForAll)
        .child(
            ChildSpec::task("temporary", {
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
            .restart(RestartPolicy::OnFailure)
            .remove_on_exit(true),
        )
        .child(
            ChildSpec::task("trigger", {
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
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("ordered group supervisor builds");
    let handle = supervisor.spawn();
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
        .strategy(kokage_supervisor::Strategy::OneForAll)
        .child(
            ChildSpec::task("temporary", move |ctx| {
                let temporary_starts_tx = temporary_starts_tx.clone();
                async move {
                    temporary_starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure)
            .remove_on_exit(true),
        )
        .child(
            ChildSpec::task("trigger", {
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
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("ordered group supervisor builds");
    let handle = supervisor.spawn();

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
        [ChildSpec::task("dynamic", move |ctx| {
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
        .remove_child("dynamic")
        .await
        .expect("last child removal should be allowed");
    assert!(handle.snapshot().children.is_empty());

    let mut events = common::event_watch(&handle);
    let replacement_lineage = handle
        .add_child(ChildSpec::task("dynamic", move |_ctx| {
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
        [ChildSpec::task("transient", move |_ctx| {
            let started_tx = started_tx.clone();
            let release = release_for_child.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                release.notified().await;
                Ok(())
            }
        })
        .restart(RestartPolicy::OnFailure)],
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
        .add_child(ChildSpec::task("probe", |_ctx| async move { Ok(()) }))
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
        [ChildSpec::task("fails", move |_ctx| {
            let started_tx = started_tx.clone();
            let release = release_for_child.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                release.notified().await;
                Err(common::test_error("terminal failure"))
            }
        })
        .restart(RestartPolicy::Never)],
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

    assert!(matches!(
        handle
            .snapshot()
            .child("fails")
            .expect("failed child remains visible")
            .last_exit(),
        Some(ExitStatusView::Failed(message)) if message.contains("terminal failure")
    ));

    handle
        .add_child(ChildSpec::task("probe", |_ctx| async move { Ok(()) }))
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
        .add_child(ChildSpec::task("dynamic", move |ctx| {
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
            ChildSpec::task("removable", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    ctx.shutdown_token().cancelled().await;
                    Err(common::test_error("do not restart on remove"))
                }
            })
            .restart(RestartPolicy::OnFailure),
            ChildSpec::task("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    let mut events = common::event_watch(&handle);

    assert_eq!(common::recv_event(&mut starts_rx).await, 0);

    handle
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
        [ChildSpec::task("seed", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })],
    )
    .await;

    let duplicate = handle
        .add_child(ChildSpec::task("seed", |_ctx| async move { Ok(()) }))
        .await
        .expect_err("duplicate id should be rejected");
    assert_eq!(
        duplicate,
        ControlError::Rejected(SupervisorBuildError::DuplicateChildId("seed".to_owned()))
    );

    let missing = handle
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
        [ChildSpec::task("only", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })],
    )
    .await;

    handle
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
            ChildSpec::task("removable", move |ctx| {
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
            .restart(RestartPolicy::OnFailure),
            ChildSpec::task("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }),
        ],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removable").await });

    common::recv_event(&mut cancelled_rx).await;

    let second_remove_handle = handle.clone();
    let second_remove_task =
        tokio::spawn(async move { second_remove_handle.remove_child("removable").await });

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
    let fast_shutdown = ShutdownPolicy::cooperative(common::SHORT_GRACE);

    let handle = spawn_dynamic(
        Supervisor::dynamic(),
        [
            ChildSpec::task("removable", move |ctx| {
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
            .restart(RestartPolicy::OnFailure)
            .shutdown(fast_shutdown),
            ChildSpec::task("keeper", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .shutdown(fast_shutdown),
        ],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removable").await });

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
        Supervisor::dynamic().restart_intensity(RestartConfig::new(0, Duration::from_secs(1))),
        [
            ChildSpec::task("removable", {
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
            .shutdown(ShutdownPolicy::cooperative(Duration::from_secs(5))),
            ChildSpec::task("failing", {
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
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("removable").await });
    removable_cancelled.notified().await;
    fail_now.notify_one();

    let remove_result = timeout(common::EVENT_TIMEOUT, remove_task)
        .await
        .expect("accepted removal reply must not dangle")
        .expect("remove task should join");
    assert_eq!(remove_result, Err(ControlError::SupervisorStopping));
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
        [ChildSpec::task("retiring", move |ctx| {
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
        .shutdown(ShutdownPolicy::cooperative(Duration::from_secs(1)))],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let remove_handle = handle.clone();
    let remove_task = tokio::spawn(async move { remove_handle.remove_child("retiring").await });
    common::recv_bounded_event(&mut bounce_rx).await;

    let same_id_error = handle
        .add_child(ChildSpec::task("retiring", |_| async { Ok(()) }))
        .await
        .expect_err("same-id add must not queue behind removal");
    assert_eq!(
        same_id_error,
        ControlError::ChildRemovalInProgress("retiring".to_owned())
    );

    timeout(
        common::EVENT_TIMEOUT,
        handle.add_child(ChildSpec::supervisor(
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
        async move { handle.remove_child("first").await }
    });
    let second_remove = tokio::spawn({
        let handle = handle.clone();
        async move { handle.remove_child("second").await }
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
        [ChildSpec::task("stubborn", move |ctx| {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .shutdown(ShutdownPolicy::cooperative(common::SHORT_GRACE))],
    )
    .await;
    common::recv_event(&mut started_rx).await;

    let removal = timeout(Duration::from_secs(1), handle.remove_child("stubborn"))
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
        [ChildSpec::task("done", |_ctx| async move { Ok(()) }).restart(RestartPolicy::Never)],
    )
    .await;

    let mut snapshots = handle.subscribe_snapshots();
    snapshots
        .wait_for(|snapshot| {
            snapshot
                .child("done")
                .is_some_and(|child| child.state.is_stopped())
        })
        .await
        .expect("completion snapshot remains available");

    handle
        .add_child(ChildSpec::task("late", |_ctx| async move { Ok(()) }))
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
        Supervisor::dynamic().restart_intensity(
            kokage_supervisor::RestartConfig::new(4, std::time::Duration::from_secs(60))
                .with_backoff(kokage_supervisor::BackoffPolicy::Fixed(
                    std::time::Duration::from_secs(30),
                )),
        ),
        [
            ChildSpec::task("removable", move |ctx| {
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("restart me later"))
                }
            })
            .restart(RestartPolicy::OnFailure),
            ChildSpec::task("keeper", |ctx| async move {
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

    timeout(common::EVENT_TIMEOUT, handle.remove_child("removable"))
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
        Supervisor::dynamic().restart_intensity(kokage_supervisor::RestartConfig::new(
            8,
            std::time::Duration::from_secs(1),
        )),
        [ChildSpec::task("removable", move |ctx| {
            let starts_tx = starts_tx.clone();
            async move {
                starts_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                Err(common::test_error("restart immediately"))
            }
        })
        .restart(RestartPolicy::OnFailure)],
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

    timeout(common::EVENT_TIMEOUT, handle.remove_child("removable"))
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
    let handle = Supervisor::dynamic()
        .restart_intensity(kokage_supervisor::RestartConfig::new(
            8,
            std::time::Duration::from_secs(1),
        ))
        .build()
        .expect("valid dynamic supervisor")
        .spawn();
    let mut events = common::event_watch(&handle);
    handle
        .add_child(
            ChildSpec::task("removable", |_ctx| async move {
                Err(common::test_error("restart immediately"))
            })
            .restart(RestartPolicy::OnFailure),
        )
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

    let replacement = ChildSpec::task("replacement", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let (add_result, remove_result) = tokio::join!(
        biased;
        handle.add_child(replacement),
        handle.remove_child("removable"),
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
        Supervisor::dynamic().restart_intensity(
            kokage_supervisor::RestartConfig::new(4, std::time::Duration::from_secs(1))
                .with_backoff(kokage_supervisor::BackoffPolicy::Fixed(backoff)),
        ),
        [
            ChildSpec::task("removable", move |ctx| {
                let removable_tx = removable_tx.clone();
                async move {
                    removable_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    Err(common::test_error("restart me later"))
                }
            })
            .restart(RestartPolicy::OnFailure),
            ChildSpec::task("keeper", |ctx| async move {
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
        .remove_child("removable")
        .await
        .expect("child removal should succeed during backoff");
    handle
        .add_child(ChildSpec::task("replacement", move |ctx| {
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
use kokage_tokio::TokioSupervisorExt as _;
