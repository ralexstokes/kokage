use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kokage_supervisor::{
    ChildSpec, ExitStatusView, RestartPolicy, ShutdownPolicy, Strategy, Supervisor,
};
use tokio::sync::{Notify, mpsc};

mod common;

#[tokio::test]
async fn transient_child_panic_causes_restart() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("panic-worker", move |ctx| {
                let attempts = attempts.clone();
                let starts_tx = starts_tx.clone();
                async move {
                    starts_tx
                        .send(ctx.generation())
                        .expect("test receiver dropped");
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        panic!("boom");
                    }

                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut starts_rx, 2).await, vec![0, 1]);
    assert!(matches!(
        handle
            .snapshot()
            .child("panic-worker")
            .expect("panic worker remains visible")
            .state
            .last_exit()
            .map(|exit| &exit.status),
        Some(ExitStatusView::Panicked)
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn abort_mode_group_peer_reports_aborted_exit_status() {
    let trigger_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (peer_starts_tx, mut peer_starts_rx) = mpsc::unbounded_channel();

    let peer = ChildSpec::task("abort-peer", move |ctx| {
        let peer_starts_tx = peer_starts_tx.clone();
        async move {
            peer_starts_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            std::future::pending::<()>().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always)
    .shutdown(ShutdownPolicy::Abort);

    let trigger_failure_for_child = Arc::clone(&trigger_failure);
    let trigger = ChildSpec::task("trigger", move |ctx| {
        let trigger_failure = Arc::clone(&trigger_failure_for_child);
        let trigger_attempts = Arc::clone(&trigger_attempts);
        async move {
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                trigger_failure.notified().await;
                return Err(common::test_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let handle = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(peer)
        .child(trigger)
        .build()
        .expect("valid supervisor")
        .spawn();
    let mut snapshots = handle.subscribe_snapshots();

    assert_eq!(common::recv_event(&mut peer_starts_rx).await, 0);
    trigger_failure.notify_one();
    assert_eq!(common::recv_event(&mut peer_starts_rx).await, 1);
    let peer = common::wait_for_child_running(&mut snapshots, "abort-peer", 1).await;
    assert!(matches!(
        peer.state.last_exit().map(|exit| &exit.status),
        Some(ExitStatusView::Aborted { after_grace: false })
    ));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn transient_factory_panic_causes_restart() {
    let (starts_tx, mut starts_rx) = mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let supervisor = Supervisor::ordered()
        .child(
            ChildSpec::task("panic-worker", move |ctx| {
                starts_tx
                    .send(ctx.generation())
                    .expect("test receiver dropped");
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("factory boom");
                }

                async move {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            })
            .restart(RestartPolicy::OnFailure),
        )
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut starts_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn one_for_all_panic_restarts_the_whole_group() {
    let (panic_tx, mut panic_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let panic_child = ChildSpec::task("panic-worker", move |ctx| {
        let attempts = attempts.clone();
        let panic_tx = panic_tx.clone();
        async move {
            panic_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom");
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::task("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(panic_child)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut panic_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn one_for_all_factory_panic_restarts_the_whole_group() {
    let (panic_tx, mut panic_rx) = mpsc::unbounded_channel();
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let panic_child = ChildSpec::task("panic-worker", move |ctx| {
        panic_tx
            .send(ctx.generation())
            .expect("test receiver dropped");
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("factory boom");
        }

        async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::OnFailure);

    let peer = ChildSpec::task("peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::Always);

    let supervisor = Supervisor::ordered()
        .strategy(Strategy::OneForAll)
        .child(panic_child)
        .child(peer)
        .build()
        .expect("valid supervisor");

    let handle = supervisor.spawn();

    assert_eq!(common::recv_n(&mut panic_rx, 2).await, vec![0, 1]);
    assert_eq!(common::recv_n(&mut peer_rx, 2).await, vec![0, 1]);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}
