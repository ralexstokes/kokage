use std::time::Duration;

use kokage_supervisor::{ChildSpec, ControlError, ScopeKind, Supervisor};
use tokio::{sync::mpsc, time::timeout};

#[tokio::test]
async fn raw_child_context_exposes_its_scope_and_preserves_kind_gating() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let child = ChildSpec::task("leader", move |ctx| {
        let result_tx = result_tx.clone();
        async move {
            let result = ctx
                .supervisor()
                .add_child(ChildSpec::task("late", |_| async { Ok(()) }))
                .await;
            result_tx.send(result).expect("test receiver remains open");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });
    let handle = Supervisor::ordered()
        .child(child)
        .build()
        .expect("ordered supervisor builds")
        .spawn();

    let result = timeout(Duration::from_secs(2), result_rx.recv())
        .await
        .expect("child reports")
        .expect("report channel remains open");
    assert!(matches!(
        result,
        Err(ControlError::UnsupportedByScopeKind {
            operation: "add_child",
            kind: ScopeKind::Ordered,
        })
    ));
    handle
        .shutdown_and_wait()
        .await
        .expect("root stops cleanly");
}

#[tokio::test]
async fn raw_child_can_await_a_supported_operation_on_its_own_scope() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let handle = Supervisor::dynamic()
        .build()
        .expect("dynamic supervisor builds")
        .spawn();
    handle
        .add_child(ChildSpec::task("leader", move |ctx| {
            let result_tx = result_tx.clone();
            async move {
                let result = ctx
                    .supervisor()
                    .add_child(ChildSpec::task("sibling", |ctx| async move {
                        ctx.shutdown_token().cancelled().await;
                        Ok(())
                    }))
                    .await;
                result_tx.send(result).expect("test receiver remains open");
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }))
        .await
        .expect("leader added");

    timeout(Duration::from_secs(2), result_rx.recv())
        .await
        .expect("leader reports")
        .expect("report channel remains open")
        .expect("awaited self-scope add succeeds");
    assert!(handle.snapshot().child("sibling").is_some());
    handle
        .shutdown_and_wait()
        .await
        .expect("root stops cleanly");
}
