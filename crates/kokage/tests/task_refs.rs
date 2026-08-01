use std::time::Duration;

use kokage::{
    BuildError, ControlError, DynamicTree, OneShotTaskSpec, RestartPolicy, Shutdown, Strategy,
    TaskError, TaskSpec, Tree,
    observe::{ChildStateView, ExitStatus},
};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn declared_ref_observes_fast_completion() {
    let mut tree = Tree::new();
    let task = tree.add_task("job", |_| async { Ok(()) });
    let running_tree = tree.spawn().expect("tree builds");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("task completion is retained")
        .expect("task remains observable");
    assert_eq!(exit, ExitStatus::Completed { cancelled: false });

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn one_shot_ref_retains_removed_task_exit() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let task = running_tree
        .scope()
        .spawn_once("job", |_| async { Ok(()) })
        .await
        .expect("task is inserted");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("removed task completion is retained")
        .expect("task exit is available");
    assert!(exit.is_completed());
    assert!(task.snapshot().is_none());

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn spawn_once_accepts_a_consuming_factory() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let (observed_tx, observed_rx) = oneshot::channel();
    let payload = String::from("consumed exactly once");
    let task = running_tree
        .scope()
        .spawn_once("consuming-job", move |_| async move {
            observed_tx.send(payload).expect("receiver remains live");
            Ok(())
        })
        .await
        .expect("one-shot task is inserted");

    assert_eq!(
        observed_rx.await.expect("task reports payload"),
        "consumed exactly once"
    );
    assert!(
        task.wait()
            .await
            .expect("task remains observable")
            .is_completed()
    );
    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn configured_one_shot_retains_a_consuming_factory() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let scope = running_tree.scope();
    let (observed_tx, observed_rx) = oneshot::channel();
    let payload = String::from("configured and consumed");
    let task = scope
        .spawn_once_spec(
            OneShotTaskSpec::new("configured-job", move |ctx| async move {
                ctx.mark_ready();
                observed_tx.send(payload).expect("receiver remains live");
                Ok(())
            })
            .shutdown(Shutdown::abort())
            .manual_readiness(WAIT)
            .retain_when_done(),
        )
        .await
        .expect("configured one-shot task is inserted");

    assert_eq!(
        observed_rx.await.expect("task reports payload"),
        "configured and consumed"
    );
    assert!(
        task.wait()
            .await
            .expect("task remains observable")
            .is_completed()
    );
    let snapshot = task.snapshot().expect("terminal membership is retained");
    assert_eq!(snapshot.restart_policy, RestartPolicy::never());
    assert!(!snapshot.remove_when_done);

    assert!(matches!(
        scope.spawn_once("configured-job", |_| async { Ok(()) }).await,
        Err(ControlError::Rejected(BuildError::DuplicateChildId(id)))
            if id == "configured-job"
    ));
    scope
        .remove(&task)
        .await
        .expect("retained terminal membership is removed explicitly");
    assert!(task.snapshot().is_none());

    let replacement = scope
        .spawn_once("configured-job", |_| async { Ok(()) })
        .await
        .expect("the id can be reused after explicit removal");
    assert!(
        replacement
            .wait()
            .await
            .expect("replacement remains observable")
            .is_completed()
    );

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test(start_paused = true)]
async fn one_shot_manual_readiness_timeout_is_failed_and_removed() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let task = running_tree
        .scope()
        .spawn_once_spec(
            OneShotTaskSpec::new("bounded-job", |_| async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .manual_readiness(Duration::from_millis(10)),
        )
        .await
        .expect("one-shot task is inserted");

    assert_eq!(
        task.wait_started().await,
        Err(TaskError::StoppedBeforeReady {
            task_id: "bounded-job".to_owned(),
        })
    );
    let exit = task
        .wait()
        .await
        .expect("terminal timeout remains observable");
    assert!(
        exit.failure_message()
            .is_some_and(|message| message.contains("did not report readiness"))
    );
    assert!(task.snapshot().is_none());

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn one_shot_ref_retains_exit_after_sibling_churn() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let scope = running_tree.scope();
    let task = scope
        .spawn_once("job", |_| async { Ok(()) })
        .await
        .expect("task is inserted");

    timeout(WAIT, async {
        while task.snapshot().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task is removed before sibling churn");

    for index in 0..40 {
        let sibling = scope
            .spawn_once(format!("sibling-{index}"), |_| async { Ok(()) })
            .await
            .expect("sibling is inserted");
        assert!(
            sibling
                .wait()
                .await
                .expect("sibling remains observable")
                .is_completed()
        );
    }

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("completion survives unrelated lifecycle churn")
        .expect("task remains observable");
    assert!(exit.is_completed());

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn old_ref_does_not_follow_same_id_replacement() {
    let running_tree = DynamicTree::new().spawn().expect("tree builds");
    let scope = running_tree.scope();
    let first = scope
        .spawn_once("job", |_| async { Ok(()) })
        .await
        .expect("first task is inserted");
    let first_exit = first.wait().await.expect("first task completes");
    assert!(first_exit.is_completed());

    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    let second = scope
        .add_task("job", move |_| {
            let release_rx = release_rx
                .lock()
                .expect("release mutex is not poisoned")
                .take()
                .expect("task runs once");
            async move {
                let _ = release_rx.await;
                Ok(())
            }
        })
        .await
        .expect("replacement task is inserted");

    assert!(first.snapshot().is_none());
    assert!(matches!(
        second.snapshot().map(|child| child.state),
        Some(ChildStateView::Starting { .. } | ChildStateView::Running { .. })
    ));
    release_tx.send(()).expect("replacement is still running");
    assert!(
        second
            .wait()
            .await
            .expect("replacement completes")
            .is_completed()
    );

    running_tree.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn task_ref_waits_through_a_restart() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tree = Tree::new();
    let task = tree.add_task_spec(
        TaskSpec::new("job", {
            let attempts = std::sync::Arc::clone(&attempts);
            move |_| {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(std::io::Error::other("retry").into())
                    } else {
                        Ok(())
                    }
                }
            }
        })
        .restart(RestartPolicy::on_failure()),
    );
    let running_tree = tree.spawn().expect("tree builds");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("restart completes")
        .expect("task remains observable");
    assert!(exit.is_completed());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    running_tree.shutdown().await.expect("tree stops");
}

async fn assert_task_ref_waits_through_group_restart(strategy: Strategy) {
    let fail = std::sync::Arc::new(Notify::new());
    let release_blocker = std::sync::Arc::new(Notify::new());
    let blocker_cancelled = std::sync::Arc::new(Notify::new());
    let (peer_started_tx, mut peer_started_rx) = mpsc::unbounded_channel();

    let mut tree = Tree::new().strategy(strategy);
    tree.add_task("trigger", {
        let fail = std::sync::Arc::clone(&fail);
        move |ctx| {
            let fail = std::sync::Arc::clone(&fail);
            async move {
                if ctx.generation() == 0 {
                    fail.notified().await;
                    Err(std::io::Error::other("restart group").into())
                } else {
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }
    });
    tree.add_task("blocker", {
        let blocker_cancelled = std::sync::Arc::clone(&blocker_cancelled);
        let release_blocker = std::sync::Arc::clone(&release_blocker);
        move |ctx| {
            let blocker_cancelled = std::sync::Arc::clone(&blocker_cancelled);
            let release_blocker = std::sync::Arc::clone(&release_blocker);
            async move {
                ctx.shutdown_token().cancelled().await;
                if ctx.generation() == 0 {
                    blocker_cancelled.notify_one();
                    release_blocker.notified().await;
                }
                Ok(())
            }
        }
    });
    let peer = tree.add_task("peer", move |ctx| {
        let peer_started_tx = peer_started_tx.clone();
        async move {
            peer_started_tx
                .send(ctx.generation())
                .expect("peer start receiver remains available");
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    });
    let running_tree = tree.spawn().expect("tree builds");

    assert_eq!(
        timeout(WAIT, peer_started_rx.recv())
            .await
            .expect("initial peer starts"),
        Some(0)
    );
    let mut peer_wait = tokio::spawn(async move { peer.wait().await });

    fail.notify_one();
    timeout(WAIT, blocker_cancelled.notified())
        .await
        .expect("group drain reaches blocker");
    assert!(
        timeout(Duration::from_millis(100), &mut peer_wait)
            .await
            .is_err(),
        "a group-cancelled exit is not terminal"
    );

    release_blocker.notify_one();
    assert_eq!(
        timeout(WAIT, peer_started_rx.recv())
            .await
            .expect("replacement peer starts"),
        Some(1)
    );

    running_tree.shutdown().await.expect("tree stops");
    assert!(
        peer_wait
            .await
            .expect("wait task does not panic")
            .expect("task remains observable")
            .cancelled()
    );
}

#[tokio::test]
async fn task_ref_waits_through_one_for_all_restart() {
    assert_task_ref_waits_through_group_restart(Strategy::OneForAll).await;
}

#[tokio::test]
async fn task_ref_waits_through_rest_for_one_restart() {
    assert_task_ref_waits_through_group_restart(Strategy::RestForOne).await;
}

#[tokio::test]
async fn explicit_readiness_failure_is_reported() {
    let mut tree = Tree::new();
    let task = tree.add_task_spec(
        TaskSpec::new("job", |_| async { Ok(()) })
            .manual_readiness(WAIT)
            .restart(RestartPolicy::never()),
    );
    let running_tree = tree.spawn().expect("tree builds");

    assert_eq!(
        timeout(WAIT, task.wait_started())
            .await
            .expect("readiness resolves"),
        Err(TaskError::StoppedBeforeReady {
            task_id: "job".to_owned(),
        })
    );
    assert!(
        task.wait()
            .await
            .expect("exit remains observable")
            .is_completed()
    );

    running_tree.shutdown().await.expect("tree stops");
}
