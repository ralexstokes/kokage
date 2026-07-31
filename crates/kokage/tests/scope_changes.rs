use std::time::Duration;

use kokage::{
    DynamicTree, ScopeChange,
    observe::{ChildEventKind, LifecycleEventKind},
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn changes_begin_with_state_then_deliver_new_transitions() {
    let running_tree = DynamicTree::new().spawn().expect("dynamic tree builds");
    let scope = running_tree.scope();
    let mut changes = scope.changes();

    let ScopeChange::Reset(initial) = changes.next().await.expect("initial reset") else {
        panic!("a change stream must begin with a reset");
    };
    assert!(initial.children.is_empty());

    let task = scope
        .add_task("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .await
        .expect("task is inserted");
    task.wait_started().await.expect("task starts");

    timeout(WAIT, async {
        loop {
            let ScopeChange::Event { event, snapshot } =
                changes.next().await.expect("stream remains open")
            else {
                continue;
            };
            if matches!(
                event.kind,
                LifecycleEventKind::Child(ref child)
                    if child.child_id == "worker"
                        && matches!(child.kind, ChildEventKind::Started { .. })
            ) {
                assert!(
                    event
                        .seq()
                        .is_some_and(|sequence| sequence > initial.lifecycle_seq)
                );
                assert!(snapshot.child("worker").is_some());
                break;
            }
        }
    })
    .await
    .expect("started transition arrives");

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn changes_replace_lag_with_a_fresh_reset() {
    let running_tree = DynamicTree::new().spawn().expect("dynamic tree builds");
    let scope = running_tree.scope();
    let mut changes = scope.changes();
    assert!(matches!(changes.next().await, Some(ScopeChange::Reset(_))));

    // Each insertion emits at least Added and Started. Seventy children exceed
    // the direct lifecycle queue's current 128-event capacity without relying
    // on completion or removal timing.
    for index in 0..70 {
        let task = scope
            .add_task(format!("worker-{index}"), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .await
            .expect("task is inserted");
        task.wait_started().await.expect("task starts");
    }

    let reset = timeout(WAIT, async {
        loop {
            if let Some(ScopeChange::Reset(snapshot)) = changes.next().await {
                break snapshot;
            }
        }
    })
    .await
    .expect("lag is recovered with a reset");
    assert_eq!(reset.children.len(), 70);
    assert_eq!(reset, scope.snapshot());

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn changes_keep_supervisor_transitions_on_the_correct_side_of_reset() {
    let tree = DynamicTree::new();
    let scope = tree.scope();
    let mut changes = scope.changes();
    let ScopeChange::Reset(_) = changes.next().await.expect("pre-spawn reset") else {
        panic!("a change stream must begin with a reset");
    };

    let running_tree = tree.spawn().expect("dynamic tree builds");
    assert!(matches!(
        timeout(WAIT, changes.next()).await.expect("startup event"),
        Some(ScopeChange::Event { event, .. })
            if matches!(event.kind, LifecycleEventKind::SupervisorStarted)
    ));

    scope.request_shutdown();
    let mut stopping = false;
    let mut stopped = false;
    timeout(WAIT, async {
        while let Some(change) = changes.next().await {
            let ScopeChange::Event { event, .. } = change else {
                panic!("a non-lagging stream must not reset again");
            };
            match event.kind {
                LifecycleEventKind::SupervisorStopping => stopping = true,
                LifecycleEventKind::SupervisorStopped => {
                    stopped = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("shutdown transitions arrive");
    assert!(stopping);
    assert!(stopped);
    running_tree.wait().await.expect("tree stops");

    let tree = DynamicTree::new();
    let scope = tree.scope();
    let mut startup = scope.lifecycle_events();
    let running_tree = tree.spawn().expect("second tree builds");
    timeout(WAIT, async {
        while let Some(event) = startup.next().await {
            if matches!(event.kind, LifecycleEventKind::SupervisorStarted) {
                break;
            }
        }
    })
    .await
    .expect("second tree reports startup");
    let mut changes = scope.changes();
    let ScopeChange::Reset(reset) = changes.next().await.expect("running reset") else {
        panic!("a change stream must begin with a reset");
    };
    assert_eq!(reset, scope.snapshot());
    assert!(
        timeout(Duration::from_millis(25), changes.next())
            .await
            .is_err(),
        "startup represented by the reset must not be replayed"
    );
    running_tree.shutdown().await.expect("second tree stops");
}
