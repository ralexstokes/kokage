use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::mpsc;

use crate::supervisor::{CancellationToken, SupervisorHandle};

#[derive(Debug)]
pub(crate) struct ChildReady {
    pub(crate) key: usize,
    pub(crate) lineage: u64,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadySignal(Arc<Mutex<Option<ReadySignalInner>>>);

#[derive(Debug)]
struct ReadySignalInner {
    sender: mpsc::UnboundedSender<ChildReady>,
    ready: ChildReady,
}

impl ReadySignal {
    pub(crate) fn new(sender: mpsc::UnboundedSender<ChildReady>, ready: ChildReady) -> Self {
        Self(Arc::new(Mutex::new(Some(ReadySignalInner {
            sender,
            ready,
        }))))
    }

    fn send(&self) {
        if let Some(signal) = self.0.lock().unwrap_or_else(PoisonError::into_inner).take() {
            let _ = signal.sender.send(signal.ready);
        }
    }
}

/// Runtime context passed to a supervised task on each (re)start.
///
/// The task should select on [`shutdown_token`](Self::shutdown_token) to
/// detect when its scope asks it to stop.
#[derive(Clone, Debug)]
pub struct TaskContext {
    id: String,
    generation: u64,
    token: CancellationToken,
    abort_token: CancellationToken,
    scope: SupervisorHandle,
    ready: Option<ReadySignal>,
}

impl TaskContext {
    pub(crate) fn new(
        id: String,
        generation: u64,
        token: CancellationToken,
        abort_token: CancellationToken,
        scope: SupervisorHandle,
        ready: Option<ReadySignal>,
    ) -> Self {
        Self {
            id,
            generation,
            token,
            abort_token,
            scope,
            ready,
        }
    }

    /// Returns the task's unique identifier within its scope.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the incarnation counter (0 for the first spawn).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the cancellation token for this specific task instance.
    ///
    /// The supervisor cancels it when the task should stop. Task code can
    /// clone it or derive child tokens for its own cancellation tree.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.token
    }

    /// Returns the escalation token for this task instance.
    ///
    /// With a cooperative shutdown policy the supervisor first triggers
    /// [`shutdown_token`](Self::shutdown_token). If the task is still running
    /// when its grace period expires, it triggers this token and records the
    /// exit as [`Aborted { after_grace: true, .. }`](crate::observe::ChildExitView::Aborted).
    /// The task wrapper then has a short window to finish local accounting
    /// before the supervisor hard-aborts the task: a tenth of this task's own
    /// grace, clamped to between 1 ms and 10 ms. Work that cannot finish in
    /// that window belongs before the grace expires, not after it.
    ///
    /// Most child tasks only need `shutdown_token`. Wrappers that own inner
    /// tasks can select on this token to abort and join those tasks tidily.
    pub fn abort_token(&self) -> &CancellationToken {
        &self.abort_token
    }

    pub(crate) fn supervisor(&self) -> SupervisorHandle {
        self.scope.clone()
    }

    /// Reports that this task has completed initialization.
    ///
    /// The first call for an explicitly readiness-gated task transitions it
    /// from starting to running. Further calls, and calls made by tasks
    /// without [`TaskSpec::wait_for_ready`](crate::host::TaskSpec::wait_for_ready),
    /// are harmless.
    pub fn mark_ready(&self) {
        if let Some(ready) = &self.ready {
            ready.send();
        }
    }
}
