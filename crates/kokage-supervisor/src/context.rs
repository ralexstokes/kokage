use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::mpsc;

use crate::{CancellationToken, Scheduler, SupervisorHandle};

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

/// Runtime context passed to a child function on each (re)start.
///
/// The child should select on [`shutdown_token`](Self::shutdown_token) to
/// detect when the supervisor asks it to stop.
#[derive(Clone)]
pub struct ChildContext {
    id: String,
    generation: u64,
    token: CancellationToken,
    abort_token: CancellationToken,
    scope: SupervisorHandle,
    ready: Option<ReadySignal>,
    scheduler: Arc<dyn Scheduler>,
}

impl ChildContext {
    pub(crate) fn new(
        id: String,
        generation: u64,
        token: CancellationToken,
        abort_token: CancellationToken,
        scope: SupervisorHandle,
        ready: Option<ReadySignal>,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        Self {
            id,
            generation,
            token,
            abort_token,
            scope,
            ready,
            scheduler,
        }
    }

    /// Returns the child's unique identifier within its supervisor.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the incarnation counter (0 for the first spawn).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the cancellation token for this specific child instance.
    ///
    /// The supervisor cancels it when the child should stop. Child code can
    /// clone it or derive child tokens for its own cancellation tree.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.token
    }

    /// Returns the escalation token for this child instance.
    ///
    /// With a cooperative shutdown policy the supervisor first triggers
    /// [`shutdown_token`](Self::shutdown_token). If the child is still running
    /// when its grace period expires, it triggers this token and records the
    /// exit as [`Aborted { after_grace: true }`](crate::ExitStatusView::Aborted).
    /// The child wrapper then has a short window to finish local accounting
    /// before the supervisor hard-aborts the task: a tenth of this child's own
    /// grace, clamped to between 1 ms and 10 ms. Work that cannot finish in
    /// that window belongs before the grace expires, not after it.
    ///
    /// Most child tasks only need `shutdown_token`. Wrappers that own inner
    /// tasks can select on this token to abort and join those tasks tidily.
    pub fn abort_token(&self) -> &CancellationToken {
        &self.abort_token
    }

    /// Returns the stable handle for this child's enclosing supervisor scope.
    ///
    /// Awaiting control operations on the enclosing scope is safe. The
    /// remaining self-deadlock is awaiting removal of a sibling whose drain
    /// depends on this child draining its own input; pipeline that operation
    /// instead of awaiting it inline.
    ///
    /// A readiness-gated child must also not await
    /// [`SupervisorHandle::wait_started`](crate::SupervisorHandle::wait_started)
    /// on this enclosing scope before it calls [`mark_ready`](Self::mark_ready):
    /// the child itself is preventing that scope from becoming ready. Launch
    /// the wait as pipelined work, report readiness, and only then consume its
    /// result.
    pub fn supervisor(&self) -> SupervisorHandle {
        self.scope.clone()
    }

    /// Returns the scheduler driving this child incarnation.
    pub fn scheduler(&self) -> Arc<dyn Scheduler> {
        Arc::clone(&self.scheduler)
    }

    /// Reports that this child has completed initialization.
    ///
    /// The first call for an explicitly readiness-gated child transitions it
    /// from starting to running. Further calls, and calls made by children
    /// without [`ChildSpec::wait_for_ready`](crate::ChildSpec::wait_for_ready),
    /// are harmless.
    pub fn mark_ready(&self) {
        if let Some(ready) = &self.ready {
            ready.send();
        }
    }
}

impl std::fmt::Debug for ChildContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChildContext")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
