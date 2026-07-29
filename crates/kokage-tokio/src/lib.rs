#![warn(missing_docs)]

//! Tokio runtime binding for Kokage's runtime-independent scheduler contract.

use std::{sync::Arc, time::Instant};

use kokage_supervisor::{
    BoxFuture, Scheduler, Supervisor, SupervisorHandle, TaskError, TaskHandle,
};

/// Tokio convenience methods for a runtime-independent supervisor.
pub trait TokioSupervisorExt {
    /// Spawns the supervisor on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime context.
    fn spawn(self) -> SupervisorHandle;
}

impl TokioSupervisorExt for Supervisor {
    fn spawn(self) -> SupervisorHandle {
        self.spawn_with(Arc::new(TokioScheduler::current()))
    }
}

/// A [`Scheduler`] backed by a Tokio runtime handle.
#[derive(Clone, Debug)]
pub struct TokioScheduler {
    handle: tokio::runtime::Handle,
}

impl TokioScheduler {
    /// Captures the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime context.
    pub fn current() -> Self {
        Self::new(tokio::runtime::Handle::current())
    }

    /// Uses an explicitly supplied Tokio runtime handle.
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }
}

impl Scheduler for TokioScheduler {
    fn spawn(&self, future: BoxFuture<()>) -> TaskHandle {
        task_handle(self.handle.spawn(future))
    }

    fn spawn_blocking(&self, function: Box<dyn FnOnce() + Send>) -> TaskHandle {
        task_handle(self.handle.spawn_blocking(function))
    }

    fn sleep_until(&self, deadline: Instant) -> BoxFuture<()> {
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }

    fn yield_now(&self) -> BoxFuture<()> {
        Box::pin(tokio::task::yield_now())
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

fn task_handle(handle: tokio::task::JoinHandle<()>) -> TaskHandle {
    let abort = handle.abort_handle();
    let finished = Arc::new(abort.clone());
    TaskHandle::new(
        Box::pin(async move {
            handle.await.map_err(|error| {
                if error.is_panic() {
                    TaskError::panicked(error.into_panic())
                } else {
                    TaskError::cancelled()
                }
            })
        }),
        move || abort.abort(),
        move || finished.is_finished(),
    )
}
