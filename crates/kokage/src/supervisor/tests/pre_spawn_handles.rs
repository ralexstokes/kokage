use std::time::Duration;

use crate::supervisor::{
    ChildSpec, ControlError, LifecycleEventKind, LifecycleWatch, Restart, Strategy, Supervisor,
    SupervisorError, TaskSpec,
};
use tokio::{sync::mpsc, time::timeout};

use super::common;

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

fn waiting_child(id: &str) -> TaskSpec {
    TaskSpec::new(id, |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
}

async fn wait_for_lifecycle_end(watch: &mut LifecycleWatch, message: &str) {
    timeout(EVENT_TIMEOUT, async {
        while watch.next().await.is_some() {}
    })
    .await
    .expect(message);
}

#[tokio::test]
async fn retained_builder_handle_preserves_kind_then_binds_to_the_spawned_root() {
    let builder = Supervisor::ordered().child(waiting_child("worker"));
    let handle = builder.handle();
    assert!(handle.dynamic().is_none());
    let declared = handle.snapshot();
    let worker = declared.child("worker").expect("worker is declared");
    assert!(matches!(
        worker.state,
        crate::supervisor::ChildStateView::Starting { .. }
    ));

    let supervisor = builder.build().expect("builder is valid");
    let spawned_owner = supervisor.spawn();
    let spawned = spawned_owner.handle();
    handle.wait_started().await.expect("retained handle binds");
    assert!(spawned.snapshot().child("worker").is_some());

    handle
        .shutdown_and_wait()
        .await
        .expect("root stops cleanly");
}

#[tokio::test]
async fn fluent_reconfiguration_updates_snapshot_without_pre_spawn_lifecycle_events() {
    let builder = Supervisor::ordered();
    let handle = builder.handle();
    let mut lifecycle = handle.watch_lifecycle();
    let builder = builder
        .strategy(Strategy::RestForOne)
        .child(waiting_child("first"))
        .child(waiting_child("second"));

    let snapshot = handle.snapshot();
    assert_eq!(snapshot.strategy, Strategy::RestForOne);
    assert_eq!(
        snapshot
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(
        timeout(Duration::from_millis(25), lifecycle.next())
            .await
            .is_err(),
        "declaration changes are snapshots, not lifecycle transitions"
    );

    drop(builder);
    wait_for_lifecycle_end(&mut lifecycle, "dropped builder closes lifecycle").await;
}

#[tokio::test]
async fn watch_before_spawn_observes_first_added_and_started_after_declared_baseline() {
    let builder = Supervisor::ordered().child(waiting_child("worker"));
    let handle = builder.handle();
    let mut lifecycle = handle.watch_lifecycle().direct_children();
    let baseline = handle.snapshot();
    let declared = baseline.child("worker").expect("worker is declared");
    let spawned_owner = builder.build().expect("builder is valid").spawn();
    let spawned = spawned_owner.handle();

    let added = timeout(EVENT_TIMEOUT, async {
        loop {
            let event = lifecycle.next().await.expect("watch remains open");
            if matches!(event.kind, LifecycleEventKind::ChildAdded) {
                break event;
            }
        }
    })
    .await
    .expect("Added arrives");
    let started = timeout(EVENT_TIMEOUT, async {
        loop {
            let event = lifecycle.next().await.expect("watch remains open");
            if matches!(event.kind, LifecycleEventKind::ChildStarted { .. }) {
                break event;
            }
        }
    })
    .await
    .expect("Started arrives");
    assert!(matches!(&added.kind, LifecycleEventKind::ChildAdded));
    assert!(matches!(
        started.kind,
        LifecycleEventKind::ChildStarted { generation: 0, .. }
    ));
    assert_eq!(added.seq(), Some(baseline.lifecycle_seq + 1));
    assert_eq!(
        added
            .child
            .as_ref()
            .expect("child transition carries identity")
            .lineage,
        declared.lineage
    );
    assert_eq!(started.seq(), added.seq().map(|seq| seq + 1));

    spawned
        .shutdown_and_wait()
        .await
        .expect("root stops cleanly");
}

#[tokio::test]
async fn dropped_builder_and_failed_build_terminalize_every_stream() {
    for abandonment in ["builder", "failed-build", "built-supervisor"] {
        let builder = Supervisor::ordered().child(waiting_child("worker"));
        let handle = builder.handle();
        let mut snapshots = handle.subscribe_snapshots();
        let mut events = common::event_watch(&handle);
        let mut lifecycle = handle.watch_lifecycle();

        match abandonment {
            "builder" => drop(builder),
            "failed-build" => {
                let error = builder
                    .default_restart(Restart::on_failure().limit(1, Duration::ZERO))
                    .build()
                    .expect_err("invalid build fails");
                assert!(error.to_string().contains("window"));
            }
            "built-supervisor" => {
                let supervisor = builder.build().expect("supervisor builds");
                drop(supervisor);
            }
            _ => unreachable!(),
        }

        assert!(handle.dynamic().is_none());
        assert!(matches!(
            timeout(EVENT_TIMEOUT, handle.wait_started())
                .await
                .expect("abandoned identity resolves readiness"),
            Err(SupervisorError::StartupAborted(_))
        ));
        assert!(snapshots.changed().await.is_err());
        assert!(events.recv().await.is_err());
        wait_for_lifecycle_end(&mut lifecycle, "lifecycle closes permanently").await;
    }
}

#[tokio::test]
async fn rejected_add_terminalizes_the_inserted_scopes_reserved_handle() {
    let parent_owner = Supervisor::dynamic()
        .build()
        .expect("dynamic parent builds")
        .spawn();
    let parent = parent_owner.handle();
    parent
        .dynamic()
        .expect("dynamic supervisor")
        .add_child(waiting_child("nested"))
        .await
        .expect("occupy nested id");
    let nested_builder = Supervisor::ordered().child(waiting_child("worker"));
    let nested_handle = nested_builder.handle();
    let mut snapshots = nested_handle.subscribe_snapshots();
    let mut lifecycle = nested_handle.watch_lifecycle();
    let nested = nested_builder.build().expect("nested scope builds");

    assert!(matches!(
        parent
            .dynamic()
            .expect("dynamic supervisor")
            .add_child_spec(ChildSpec::supervisor("nested", nested))
            .await,
        Err(ControlError::Rejected(
            crate::supervisor::BuildError::DuplicateChildId(id)
        )) if id == "nested"
    ));
    assert!(snapshots.changed().await.is_err());
    wait_for_lifecycle_end(&mut lifecycle, "rejected nested scope closes").await;
    assert!(nested_handle.dynamic().is_none());

    parent
        .shutdown_and_wait()
        .await
        .expect("parent stops cleanly");
}

#[tokio::test]
async fn dropping_the_last_retained_nested_handle_does_not_stop_the_inserted_scope() {
    let parent_owner = Supervisor::dynamic()
        .build()
        .expect("dynamic parent builds")
        .spawn();
    let parent = parent_owner.handle();
    parent.wait_started().await.expect("dynamic parent starts");
    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let nested_builder = Supervisor::ordered().child(TaskSpec::new("worker", move |ctx| {
        let stopped_tx = stopped_tx.clone();
        async move {
            ctx.shutdown_token().cancelled().await;
            let _ = stopped_tx.send(());
            Ok(())
        }
    }));
    let retained = nested_builder.handle();
    let nested = nested_builder.build().expect("nested scope builds");
    parent
        .dynamic()
        .expect("dynamic supervisor")
        .add_child_spec(ChildSpec::supervisor("nested", nested))
        .await
        .expect("nested scope inserts");
    retained.wait_started().await.expect("nested scope starts");

    drop(retained);
    assert!(
        timeout(Duration::from_millis(50), stopped_rx.recv())
            .await
            .is_err(),
        "a nested stable handle does not own the supervisor lifecycle"
    );
    assert!(
        parent
            .supervisor("nested")
            .expect("nested scope remains attached")
            .snapshot()
            .child("worker")
            .is_some_and(|worker| worker.state.is_running())
    );

    parent
        .shutdown_and_wait()
        .await
        .expect("parent stops cleanly");
    assert_eq!(
        timeout(EVENT_TIMEOUT, stopped_rx.recv())
            .await
            .expect("parent shutdown reaches nested worker"),
        Some(())
    );
}

#[tokio::test]
async fn wait_on_a_reserved_handle_waits_for_the_scope_to_run_and_stop() {
    let builder = Supervisor::ordered().child(waiting_child("worker"));
    let handle = builder.handle();

    // Nothing has bound yet, so `wait` is waiting for a scope that has not
    // started rather than reporting an unavailable incarnation.
    let mut waiting = Box::pin(handle.wait());
    assert!(
        timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err(),
        "wait must not resolve before the reserved identity binds"
    );

    let spawned_owner = builder.build().expect("builder is valid").spawn();
    let spawned = spawned_owner.handle();
    handle
        .wait_started()
        .await
        .expect("reserved identity binds");
    spawned.shutdown();

    timeout(EVENT_TIMEOUT, waiting)
        .await
        .expect("wait observes the spawned root stopping")
        .expect("root stops cleanly");
}

#[tokio::test]
async fn wait_on_an_abandoned_reserved_handle_reports_terminality() {
    let builder = Supervisor::ordered().child(waiting_child("worker"));
    let handle = builder.handle();

    let mut waiting = Box::pin(handle.wait());
    assert!(
        timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err(),
        "wait must not resolve before the reserved identity binds"
    );

    drop(builder);

    let error = timeout(EVENT_TIMEOUT, waiting)
        .await
        .expect("wait observes terminalization")
        .expect_err("an abandoned identity never runs");
    assert!(matches!(error, SupervisorError::Internal(_)));
}

#[tokio::test]
async fn recursive_lifecycle_watch_created_before_build_reaches_the_spawned_scope() {
    let builder = Supervisor::ordered().child(waiting_child("worker"));
    let handle = builder.handle();
    let mut events = common::event_watch(&handle);

    let spawned_owner = builder.build().expect("builder is valid").spawn();
    let spawned = spawned_owner.handle();
    handle.wait_started().await.expect("scope starts");

    timeout(EVENT_TIMEOUT, events.recv())
        .await
        .expect("a pre-build watch keeps receiving after bind")
        .expect("lifecycle stream stays open across bind");

    spawned
        .shutdown_and_wait()
        .await
        .expect("root stops cleanly");
}
