use std::time::Duration;

use kokage::{DynamicTree, ScopeChange, observe::LifecycleEventKind};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn changes_begin_with_state_then_deliver_new_transitions() {
    let runtime = DynamicTree::new().spawn().expect("dynamic tree builds");
    let scope = runtime.scope();
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
            let ScopeChange::Event(event) = changes.next().await.expect("stream remains open")
            else {
                continue;
            };
            if matches!(
                event.kind,
                LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == "worker"
            ) {
                assert!(
                    event
                        .seq()
                        .is_some_and(|sequence| sequence > initial.lifecycle_seq)
                );
                break;
            }
        }
    })
    .await
    .expect("started transition arrives");

    runtime.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn changes_replace_lag_with_a_fresh_reset() {
    let runtime = DynamicTree::new().spawn().expect("dynamic tree builds");
    let scope = runtime.scope();
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

    runtime.shutdown().await.expect("tree stops");
}
