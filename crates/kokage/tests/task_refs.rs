use std::time::Duration;

use kokage::{
    DynamicTree, ExitStatus, RestartPolicy, TaskError, TaskSpec, Tree, observe::ChildStateView,
};
use tokio::{sync::oneshot, time::timeout};

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn declared_ref_observes_fast_completion() {
    let mut tree = Tree::new();
    let task = tree.add_task("job", |_| async { Ok(()) });
    let runtime = tree.spawn().expect("tree builds");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("task completion is retained")
        .expect("task remains observable");
    assert_eq!(exit, ExitStatus::Completed { cancelled: false });

    runtime.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn temporary_dynamic_ref_retains_removed_task_exit() {
    let runtime = DynamicTree::new().spawn().expect("tree builds");
    let task = runtime
        .scope()
        .add_task_spec(TaskSpec::new("job", |_| async { Ok(()) }).temporary())
        .await
        .expect("task is inserted");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("removed task completion is retained")
        .expect("task exit is available");
    assert!(exit.is_completed());
    assert!(task.snapshot().is_none());

    runtime.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn old_ref_does_not_follow_same_id_replacement() {
    let runtime = DynamicTree::new().spawn().expect("tree builds");
    let scope = runtime.scope();
    let first = scope
        .add_task_spec(TaskSpec::new("job", |_| async { Ok(()) }).temporary())
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

    runtime.shutdown().await.expect("tree stops");
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
    let runtime = tree.spawn().expect("tree builds");

    let exit = timeout(WAIT, task.wait())
        .await
        .expect("restart completes")
        .expect("task remains observable");
    assert!(exit.is_completed());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    runtime.shutdown().await.expect("tree stops");
}

#[tokio::test]
async fn explicit_readiness_failure_is_reported() {
    let mut tree = Tree::new();
    let task = tree.add_task_spec(
        TaskSpec::new("job", |_| async { Ok(()) })
            .wait_for_ready()
            .restart(RestartPolicy::never()),
    );
    let runtime = tree.spawn().expect("tree builds");

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

    runtime.shutdown().await.expect("tree stops");
}
