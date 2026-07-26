use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::SupervisorHandle;

#[derive(Debug)]
pub(crate) struct ChildReady {
    pub(crate) key: usize,
    pub(crate) instance: u64,
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
        if let Some(signal) = self.0.lock().expect("ready signal poisoned").take() {
            let _ = signal.sender.send(signal.ready);
        }
    }
}

/// Runtime context passed to a child function on each (re)start.
///
/// The child should select on [`shutdown_token`](Self::shutdown_token) to
/// detect when the supervisor asks it to stop. The
/// [`supervisor_token`](Self::supervisor_token) provides a read-only view of
/// the parent supervisor's cancellation state.
#[derive(Clone, Debug)]
pub struct ChildContext {
    id: String,
    generation: u64,
    token: CancellationToken,
    supervisor: SupervisorToken,
    scope: SupervisorHandle,
    ready: Option<ReadySignal>,
}

impl ChildContext {
    pub(crate) fn new(
        id: String,
        generation: u64,
        token: CancellationToken,
        supervisor: SupervisorToken,
        scope: SupervisorHandle,
        ready: Option<ReadySignal>,
    ) -> Self {
        Self {
            id,
            generation,
            token,
            supervisor,
            scope,
            ready,
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
    /// The supervisor cancels it when the child should stop. The returned type
    /// is deliberately Tokio Util's [`CancellationToken`], allowing child code
    /// to compose it directly with existing cancellation trees.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.token
    }

    /// Returns a read-only view of the supervisor's cancellation state.
    ///
    /// This fires when the supervisor begins a shutdown, in both ordered and
    /// dynamic scopes, and independently of when this particular child is
    /// cancelled. It does not fire when a supervisor dies of its own failure
    /// (for example [`SupervisorError::RestartIntensityExceeded`]): that path
    /// drops the child tasks rather than stopping them. Use it to observe an
    /// orderly stop, not as a general liveness signal for the supervisor.
    ///
    /// [`SupervisorError::RestartIntensityExceeded`]:
    ///     crate::SupervisorError::RestartIntensityExceeded
    pub fn supervisor_token(&self) -> &SupervisorToken {
        &self.supervisor
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

/// Read-only view of the supervisor's cancellation token.
///
/// Children can observe supervisor-level cancellation but cannot trigger it.
#[derive(Clone, Debug)]
pub struct SupervisorToken(CancellationToken);

impl SupervisorToken {
    pub(crate) fn new(token: CancellationToken) -> Self {
        Self(token)
    }

    /// Returns a future that completes when the supervisor is cancelled.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }

    /// Returns `true` if the supervisor has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}
