use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{ChildSpec, Restart, Shutdown, Strategy, Supervisor};
use tokio::sync::{Notify, mpsc};

use super::common;

#[tokio::test]
async fn middle_failure_restarts_only_the_downstream_suffix_in_order() {
    let release_failure = Arc::new(Notify::new());
    let middle_attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let upstream = reporting_child("upstream", started_tx.clone());

    let release_failure_for_child = release_failure.clone();
    let middle_started_tx = started_tx.clone();
    let middle = ChildSpec::task("middle", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let middle_attempts = middle_attempts.clone();
        let started_tx = middle_started_tx.clone();
        async move {
            started_tx
                .send(("middle", ctx.generation()))
                .expect("test receiver dropped");
            if middle_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart downstream suffix"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(Restart::on_failure());

    let downstream = reporting_child("downstream", started_tx);
    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(upstream)
        .child(middle)
        .child(downstream)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(
        common::recv_n(&mut started_rx, 3).await,
        vec![("upstream", 0), ("middle", 0), ("downstream", 0)]
    );
    release_failure.notify_one();
    assert_eq!(
        common::recv_n(&mut started_rx, 2).await,
        vec![("middle", 1), ("downstream", 1)]
    );
    common::assert_no_event(&mut started_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn last_child_failure_restarts_only_itself() {
    let release_failure = Arc::new(Notify::new());
    let last_attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let first = reporting_child("first", started_tx.clone());
    let middle = reporting_child("middle", started_tx.clone());
    let release_failure_for_child = release_failure.clone();
    let last = ChildSpec::task("last", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let last_attempts = last_attempts.clone();
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send(("last", ctx.generation()))
                .expect("test receiver dropped");
            if last_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart last child"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(Restart::on_failure());

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(first)
        .child(middle)
        .child(last)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(
        common::recv_n(&mut started_rx, 3).await,
        vec![("first", 0), ("middle", 0), ("last", 0)]
    );
    release_failure.notify_one();
    assert_eq!(common::recv_event(&mut started_rx).await, ("last", 1));
    common::assert_no_event(&mut started_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn rest_for_one_escalates_a_stubborn_cooperative_suffix_and_restarts() {
    let release_failure = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let release_failure_for_child = release_failure.clone();
    let trigger = ChildSpec::task("trigger", move |ctx| {
        let release_failure = release_failure_for_child.clone();
        let trigger_attempts = trigger_attempts.clone();
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send(("trigger", ctx.generation()))
                .expect("test receiver dropped");
            if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                release_failure.notified().await;
                return Err(common::test_error("restart suffix"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(Restart::on_failure());

    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
    let peer = ChildSpec::task("stubborn-peer", move |ctx| {
        let peer_tx = peer_tx.clone();
        async move {
            peer_tx
                .send(ctx.generation())
                .expect("test receiver dropped");
            if ctx.generation() == 0 {
                std::future::pending::<()>().await;
            } else {
                ctx.shutdown_token().cancelled().await;
            }
            Ok(())
        }
    })
    .restart(Restart::always())
    .shutdown(Shutdown::drain_for(common::SHORT_GRACE));

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(trigger)
        .child(peer)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(common::recv_event(&mut started_rx).await, ("trigger", 0));
    assert_eq!(common::recv_event(&mut peer_rx).await, 0);
    release_failure.notify_one();
    assert_eq!(common::recv_event(&mut started_rx).await, ("trigger", 1));
    assert_eq!(common::recv_event(&mut peer_rx).await, 1);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn upstream_failure_during_suffix_drain_is_dispatched_after_the_restart() {
    let fail_upstream = Arc::new(Notify::new());
    let fail_middle = Arc::new(Notify::new());
    let slow_cancelled = Arc::new(Notify::new());
    let release_slow = Arc::new(Notify::new());
    let upstream_attempts = Arc::new(AtomicUsize::new(0));
    let middle_attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let fail_upstream_for_child = fail_upstream.clone();
    let upstream = ChildSpec::task("upstream", {
        let started_tx = started_tx.clone();
        move |ctx| {
            let fail_upstream = fail_upstream_for_child.clone();
            let upstream_attempts = upstream_attempts.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx
                    .send(("upstream", ctx.generation()))
                    .expect("test receiver dropped");
                if upstream_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    fail_upstream.notified().await;
                    return Err(common::test_error("upstream failed during suffix drain"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(Restart::on_failure());

    let fail_middle_for_child = fail_middle.clone();
    let middle = ChildSpec::task("middle", {
        let started_tx = started_tx.clone();
        move |ctx| {
            let fail_middle = fail_middle_for_child.clone();
            let middle_attempts = middle_attempts.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx
                    .send(("middle", ctx.generation()))
                    .expect("test receiver dropped");
                if middle_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    fail_middle.notified().await;
                    return Err(common::test_error("restart suffix"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(Restart::on_failure());

    let slow = ChildSpec::task("slow", {
        let started_tx = started_tx.clone();
        let slow_cancelled = slow_cancelled.clone();
        let release_slow = release_slow.clone();
        move |ctx| {
            let started_tx = started_tx.clone();
            let slow_cancelled = slow_cancelled.clone();
            let release_slow = release_slow.clone();
            async move {
                started_tx
                    .send(("slow", ctx.generation()))
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                if ctx.generation() == 0 {
                    slow_cancelled.notify_one();
                    release_slow.notified().await;
                }
                Ok(())
            }
        }
    })
    .restart(Restart::always())
    .shutdown(Shutdown::drain_for(Duration::from_secs(1)));

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(upstream)
        .child(middle)
        .child(slow)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(
        common::recv_n(&mut started_rx, 3).await,
        vec![("upstream", 0), ("middle", 0), ("slow", 0)]
    );
    fail_middle.notify_one();
    slow_cancelled.notified().await;
    fail_upstream.notify_one();
    tokio::task::yield_now().await;
    release_slow.notify_one();

    assert_eq!(
        common::recv_n(&mut started_rx, 5).await,
        vec![
            ("middle", 1),
            ("slow", 1),
            ("upstream", 1),
            ("middle", 2),
            ("slow", 2),
        ]
    );

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn never_child_in_suffix_is_drained_but_not_restarted() {
    let fail_trigger = Arc::new(Notify::new());
    let trigger_attempts = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (never_drained_tx, mut never_drained_rx) = mpsc::unbounded_channel();

    let fail_trigger_for_child = fail_trigger.clone();
    let trigger = ChildSpec::task("trigger", {
        let started_tx = started_tx.clone();
        move |ctx| {
            let fail_trigger = fail_trigger_for_child.clone();
            let trigger_attempts = trigger_attempts.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx
                    .send(("trigger", ctx.generation()))
                    .expect("test receiver dropped");
                if trigger_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    fail_trigger.notified().await;
                    return Err(common::test_error("restart suffix"));
                }
                ctx.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })
    .restart(Restart::on_failure());

    let never = ChildSpec::task("never", {
        let started_tx = started_tx.clone();
        move |ctx| {
            let started_tx = started_tx.clone();
            let never_drained_tx = never_drained_tx.clone();
            async move {
                started_tx
                    .send(("never", ctx.generation()))
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                never_drained_tx.send(()).expect("test receiver dropped");
                Ok(())
            }
        }
    })
    .restart(Restart::never());
    let eligible = reporting_child("eligible", started_tx);

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(trigger)
        .child(never)
        .child(eligible)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(
        common::recv_n(&mut started_rx, 3).await,
        vec![("trigger", 0), ("never", 0), ("eligible", 0)]
    );
    fail_trigger.notify_one();
    common::recv_event(&mut never_drained_rx).await;
    assert_eq!(
        common::recv_n(&mut started_rx, 2).await,
        vec![("trigger", 1), ("eligible", 1)]
    );
    common::assert_no_event(&mut started_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

fn reporting_child(
    id: &'static str,
    started_tx: mpsc::UnboundedSender<(&'static str, u64)>,
) -> ChildSpec {
    ChildSpec::task(id, move |ctx| {
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send((id, ctx.generation()))
                .expect("test receiver dropped");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(Restart::always())
}

/// Two upstream children fail while the suffix drain is held open, so both
/// exits are deferred in the same drain window. Dispatching the first
/// deferred exit restarts a suffix that contains the second deferred child;
/// its already-consumed join must not stall the nested drain, and its own
/// deferred exit must be recognized as stale afterwards.
#[tokio::test]
async fn two_upstream_failures_during_suffix_drain_all_recover() {
    let fail_a = Arc::new(Notify::new());
    let fail_b = Arc::new(Notify::new());
    let fail_trigger = Arc::new(Notify::new());
    let slow_cancelled = Arc::new(Notify::new());
    let release_slow = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let a = failing_once_child("a", fail_a.clone(), started_tx.clone());
    let b = failing_once_child("b", fail_b.clone(), started_tx.clone());
    let trigger = failing_once_child("trigger", fail_trigger.clone(), started_tx.clone());

    let slow = ChildSpec::task("slow", {
        let started_tx = started_tx.clone();
        let slow_cancelled = slow_cancelled.clone();
        let release_slow = release_slow.clone();
        move |ctx| {
            let started_tx = started_tx.clone();
            let slow_cancelled = slow_cancelled.clone();
            let release_slow = release_slow.clone();
            async move {
                started_tx
                    .send(("slow", ctx.generation()))
                    .expect("test receiver dropped");
                ctx.shutdown_token().cancelled().await;
                if ctx.generation() == 0 {
                    slow_cancelled.notify_one();
                    release_slow.notified().await;
                }
                Ok(())
            }
        }
    })
    .restart(Restart::always())
    .shutdown(Shutdown::drain_for(Duration::from_secs(1)));

    let handle_owner = Supervisor::ordered()
        .strategy(Strategy::RestForOne)
        .child(a)
        .child(b)
        .child(trigger)
        .child(slow)
        .build()
        .expect("valid supervisor")
        .spawn();
    let handle = handle_owner.handle();

    assert_eq!(
        common::recv_n(&mut started_rx, 4).await,
        vec![("a", 0), ("b", 0), ("trigger", 0), ("slow", 0)]
    );

    // Fail the trigger; the suffix [trigger, slow] starts draining and the
    // slow child holds the drain open in its grace loop.
    fail_trigger.notify_one();
    slow_cancelled.notified().await;

    // While the drain is held open, both upstream children fail. Their joins
    // are consumed by the drain loop and deferred.
    fail_a.notify_one();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    fail_b.notify_one();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    release_slow.notify_one();

    // Suffix restart from the original failure.
    assert_eq!(
        common::recv_n(&mut started_rx, 2).await,
        vec![("trigger", 1), ("slow", 1)]
    );
    // Dispatching a's deferred exit restarts the whole group (a is first).
    // That suffix restart already covers b, so b's own deferred exit must be
    // recognized as stale, not dispatched against the fresh generation.
    assert_eq!(
        common::recv_n(&mut started_rx, 4).await,
        vec![("a", 1), ("b", 1), ("trigger", 2), ("slow", 2)]
    );
    common::assert_no_event(&mut started_rx).await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

fn failing_once_child(
    id: &'static str,
    fail: Arc<Notify>,
    started_tx: mpsc::UnboundedSender<(&'static str, u64)>,
) -> ChildSpec {
    let attempts = Arc::new(AtomicUsize::new(0));
    ChildSpec::task(id, move |ctx| {
        let fail = fail.clone();
        let attempts = attempts.clone();
        let started_tx = started_tx.clone();
        async move {
            started_tx
                .send((id, ctx.generation()))
                .expect("test receiver dropped");
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                fail.notified().await;
                return Err(common::test_error("deferred-exit probe failure"));
            }
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(Restart::on_failure())
    .shutdown(Shutdown::drain_for(Duration::from_millis(200)))
}
