use tokio_util::sync::CancellationToken;

/// Observe-only view of one actor incarnation's lifetime.
///
/// A lifetime ends when the incarnation stops or restarts. It grants no
/// authority to stop the actor; use it to bind background work to the actor
/// that scheduled it.
#[derive(Clone, Debug)]
pub struct Lifetime {
    cancellation: CancellationToken,
}

impl Lifetime {
    pub(crate) fn from_token(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    /// Returns whether the actor incarnation has ended.
    pub fn is_ended(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Waits until the actor incarnation ends.
    pub async fn ended(&self) {
        self.cancellation.cancelled().await;
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

impl CancellationHandle {
    /// Creates an independent cancellation handle.
    pub fn new() -> Self {
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

impl Default for CancellationHandle {
    fn default() -> Self {
        Self::new()
    }
}
