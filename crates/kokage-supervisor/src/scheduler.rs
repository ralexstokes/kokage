use std::{
    any::Any,
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use tokio::sync::oneshot;

/// A boxed, scheduler-ready future.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Runtime services required by Kokage's supervision and actor cores.
///
/// Implementations must be safe to call concurrently. Spawned futures must be
/// polled without concurrent polls of the same future, and may begin running
/// before `spawn` returns. The returned handle must report unwinding as
/// [`TaskError::Panicked`] and executor cancellation as
/// [`TaskError::Cancelled`]. Kokage enforces abort-on-drop through
/// [`TaskHandle`].
///
/// [`sleep_until`](Self::sleep_until) must use the same monotonic clock as
/// [`now`](Self::now), must not complete before its deadline, and must wake
/// after clock advancement. `spawn_blocking` must keep blocking closures off
/// asynchronous worker threads; cancellation may be unable to stop a closure
/// that has already begun, but joining must still distinguish cancellation
/// from panic.
pub trait Scheduler: Send + Sync + 'static {
    /// Spawns asynchronous work.
    fn spawn(&self, future: BoxFuture<()>) -> TaskHandle;

    /// Spawns blocking work without occupying an asynchronous worker thread.
    fn spawn_blocking(&self, function: Box<dyn FnOnce() + Send>) -> TaskHandle;

    /// Completes no earlier than `deadline`, according to this scheduler's
    /// monotonic clock.
    fn sleep_until(&self, deadline: Instant) -> BoxFuture<()>;

    /// Yields the current task so other runnable tasks can make progress.
    fn yield_now(&self) -> BoxFuture<()>;

    /// Reads this scheduler's monotonic clock.
    fn now(&self) -> Instant;
}

/// Why a scheduler task failed to join.
pub enum TaskError {
    /// The scheduler reports that the task was cancelled.
    Cancelled,
    /// The task unwound with this panic payload.
    Panicked(Box<dyn Any + Send + 'static>),
}

impl TaskError {
    /// Constructs a cancellation result for a scheduler binding.
    pub fn cancelled() -> Self {
        Self::Cancelled
    }

    /// Constructs a panic result for a scheduler binding.
    pub fn panicked(payload: Box<dyn Any + Send + 'static>) -> Self {
        Self::Panicked(payload)
    }

    /// Returns whether the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Returns whether the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }

    /// Returns the panic payload, or this error when it represents
    /// cancellation.
    pub fn into_panic(self) -> Result<Box<dyn Any + Send + 'static>, Self> {
        match self {
            Self::Panicked(payload) => Ok(payload),
            error => Err(error),
        }
    }
}

impl fmt::Debug for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("TaskError::Cancelled"),
            Self::Panicked(_) => formatter.write_str("TaskError::Panicked(..)"),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("task was cancelled"),
            Self::Panicked(_) => formatter.write_str("task panicked"),
        }
    }
}

impl std::error::Error for TaskError {}

struct TaskControl {
    abort: Box<dyn Fn() + Send + Sync>,
    is_finished: Box<dyn Fn() -> bool + Send + Sync>,
}

/// An abort-on-drop handle returned by a [`Scheduler`].
///
/// Await [`join`](Self::join) to observe normal completion, cancellation, or
/// panic. Dropping the handle, including by cancelling a join future, aborts
/// the task. Call [`detach`](Self::detach) only for deliberately unstructured
/// background work.
pub struct TaskHandle {
    join: Option<BoxFuture<Result<(), TaskError>>>,
    control: Arc<TaskControl>,
    abort_on_drop: bool,
}

impl TaskHandle {
    /// Constructs a handle for a third-party scheduler binding.
    ///
    /// `join` must resolve exactly once with the task's final status. `abort`
    /// must request cancellation without blocking, and `is_finished` must
    /// become true once joining can no longer wait on task execution. The
    /// closures may be invoked from any thread and may be invoked after task
    /// completion.
    pub fn new(
        join: BoxFuture<Result<(), TaskError>>,
        abort: impl Fn() + Send + Sync + 'static,
        is_finished: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            join: Some(join),
            control: Arc::new(TaskControl {
                abort: Box::new(abort),
                is_finished: Box::new(is_finished),
            }),
            abort_on_drop: true,
        }
    }

    /// Waits for the task to finish.
    pub async fn join(mut self) -> Result<(), TaskError> {
        self.join
            .take()
            .expect("task can only be joined once")
            .await
    }

    /// Waits without coupling cancellation of the join future to cancellation
    /// of the task.
    ///
    /// Nested supervisors have their own drop guard that chooses between a
    /// cooperative shutdown and an abort. That guard must remain the sole
    /// cancellation authority when its parent task is dropped.
    pub(crate) async fn join_detached(mut self) -> Result<(), TaskError> {
        self.abort_on_drop = false;
        self.join
            .take()
            .expect("task can only be joined once")
            .await
    }

    /// Requests cancellation at the scheduler's next cancellation boundary.
    pub fn abort(&self) {
        (self.control.abort)();
    }

    /// Returns whether the scheduler reports the task as finished.
    pub fn is_finished(&self) -> bool {
        (self.control.is_finished)()
    }

    /// Allows the task to outlive this handle.
    pub fn detach(mut self) {
        self.abort_on_drop = false;
    }

    /// Returns a cloneable abort and completion-status handle.
    #[doc(hidden)]
    pub fn abort_handle(&self) -> TaskAbortHandle {
        TaskAbortHandle {
            control: Arc::clone(&self.control),
            id: None,
        }
    }
}

impl fmt::Debug for TaskHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskHandle")
            .field("is_finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        if self.abort_on_drop {
            (self.control.abort)();
        }
    }
}

/// A cloneable cancellation and completion-status view of a scheduler task.
#[derive(Clone)]
#[doc(hidden)]
pub struct TaskAbortHandle {
    control: Arc<TaskControl>,
    id: Option<TaskId>,
}

impl TaskAbortHandle {
    pub fn abort(&self) {
        (self.control.abort)();
    }

    pub fn is_finished(&self) -> bool {
        (self.control.is_finished)()
    }

    pub fn id(&self) -> TaskId {
        self.id.expect("only task-set handles have task identities")
    }
}

impl fmt::Debug for TaskAbortHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskAbortHandle")
            .field("is_finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

/// Runtime-independent identity assigned to a task in a task set.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[doc(hidden)]
pub struct TaskId(u64);

/// A task-set completion with stable identity and explicit join status.
#[doc(hidden)]
pub struct TaskJoin<T> {
    pub id: TaskId,
    pub result: Result<T, TaskError>,
}

/// A small scheduler-backed equivalent of an abort-on-drop join set.
#[doc(hidden)]
pub struct TaskSet<T> {
    scheduler: Arc<dyn Scheduler>,
    tasks: FuturesUnordered<BoxFuture<TaskJoin<T>>>,
    controls: HashMap<TaskId, TaskAbortHandle>,
}

impl<T: Send + 'static> TaskSet<T> {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self {
            scheduler,
            tasks: FuturesUnordered::new(),
            controls: HashMap::new(),
        }
    }

    pub fn spawn(&mut self, future: impl Future<Output = T> + Send + 'static) -> TaskAbortHandle {
        static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);
        let id = TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let (result_tx, result_rx) = oneshot::channel();
        let task = self.scheduler.spawn(Box::pin(async move {
            let _ = result_tx.send(future.await);
        }));
        let mut abort = task.abort_handle();
        abort.id = Some(id);
        self.controls.insert(id, abort.clone());
        self.tasks.push(Box::pin(async move {
            let joined = task.join().await;
            let result = match joined {
                Ok(()) => result_rx.await.map_err(|_| TaskError::cancelled()),
                Err(error) => Err(error),
            };
            TaskJoin { id, result }
        }));
        abort
    }

    pub async fn join_next(&mut self) -> Option<TaskJoin<T>> {
        let joined = self.tasks.next().await?;
        self.controls.remove(&joined.id);
        Some(joined)
    }

    pub fn try_join_next(&mut self) -> Option<TaskJoin<T>> {
        let joined = self.tasks.next().now_or_never().flatten()?;
        self.controls.remove(&joined.id);
        Some(joined)
    }

    pub fn abort_all(&self) {
        for control in self.controls.values() {
            control.abort();
        }
    }

    pub fn len(&self) -> usize {
        self.controls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.controls.is_empty()
    }
}

/// Yields once without depending on a particular executor.
#[doc(hidden)]
pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(move |context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}
