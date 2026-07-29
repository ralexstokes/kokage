use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kokage_supervisor::Scheduler;
use kokage_tokio::TokioScheduler;

struct MarkDropped(Arc<AtomicBool>);

impl Drop for MarkDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn dropping_a_task_handle_aborts_its_task() {
    let scheduler = TokioScheduler::current();
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = Arc::clone(&dropped);
    let handle = scheduler.spawn(Box::pin(async move {
        let _guard = MarkDropped(task_dropped);
        std::future::pending::<()>().await;
    }));

    tokio::task::yield_now().await;
    drop(handle);
    for _ in 0..10 {
        if dropped.load(Ordering::Acquire) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("dropping the handle did not cancel the task");
}

#[tokio::test]
async fn panics_and_cancellation_remain_distinguishable() {
    let scheduler = TokioScheduler::current();
    let panic = scheduler.spawn(Box::pin(async { panic!("boom") }));
    assert!(panic.join().await.expect_err("task panics").is_panic());

    let cancelled = scheduler.spawn(Box::pin(std::future::pending()));
    cancelled.abort();
    assert!(
        cancelled
            .join()
            .await
            .expect_err("task is cancelled")
            .is_cancelled()
    );
}

#[tokio::test]
async fn yield_now_allows_another_runnable_task_to_progress() {
    let scheduler = TokioScheduler::current();
    let progressed = Arc::new(AtomicBool::new(false));
    let task_progressed = Arc::clone(&progressed);
    let task = scheduler.spawn(Box::pin(async move {
        task_progressed.store(true, Ordering::Release);
    }));

    scheduler.yield_now().await;
    assert!(progressed.load(Ordering::Acquire));
    task.join().await.expect("task joins cleanly");
}

#[tokio::test(start_paused = true)]
async fn clock_and_sleep_share_tokios_monotonic_time() {
    let scheduler = TokioScheduler::new(tokio::runtime::Handle::current());
    let start = scheduler.now();
    let sleep = scheduler.sleep_until(start + Duration::from_secs(5));
    tokio::pin!(sleep);

    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut sleep)
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    sleep.await;
    assert!(scheduler.now() >= start + Duration::from_secs(5));
}
