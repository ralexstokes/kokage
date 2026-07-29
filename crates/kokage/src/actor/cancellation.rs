use std::sync::Arc;

use kokage_supervisor::{CancellationToken, Scheduler};

/// Opaque token identifying one actor incarnation's lifetime.
///
/// A lifetime ends when the incarnation stops or restarts. It grants no
/// authority to stop the actor and has no direct observation methods. Pass it
/// to [`timers::send_after_to`](crate::timers::send_after_to) or
/// [`timers::interval_to`](crate::timers::interval_to) to bind cross-actor
/// timer work to the actor that scheduled it.
#[derive(Clone)]
pub struct Lifetime {
    cancellation: CancellationToken,
    scheduler: Arc<dyn Scheduler>,
}

impl Lifetime {
    pub(crate) fn from_token(
        cancellation: CancellationToken,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        Self {
            cancellation,
            scheduler,
        }
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn scheduler(&self) -> Arc<dyn Scheduler> {
        Arc::clone(&self.scheduler)
    }
}

impl std::fmt::Debug for Lifetime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Lifetime").finish_non_exhaustive()
    }
}

/// Cloneable handle for cancelling an actor-owned operation.
///
/// Returned by actor timers and watches. Clones refer to the same operation,
/// and dropping a handle does not cancel it; call [`cancel`](Self::cancel)
/// explicitly.
#[derive(Clone, Debug)]
pub struct CancellationHandle {
    cancellation: CancellationToken,
}

pub(crate) struct CancelOnDrop(CancellationToken);

impl CancelOnDrop {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self(cancellation)
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl CancellationHandle {
    pub(crate) fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
        }
    }

    pub(crate) fn from_token(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Cancels the operation. Cancellation is idempotent.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether the operation has been cancelled.
    ///
    /// An operation may also complete normally without being cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Waits until the operation is cancelled.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}
