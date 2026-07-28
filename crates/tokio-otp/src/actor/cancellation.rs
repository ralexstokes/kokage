use tokio_util::sync::CancellationToken;

/// Opaque token identifying one actor incarnation's lifetime.
///
/// A lifetime ends when the incarnation stops or restarts. It grants no
/// authority to stop the actor and has no direct observation methods. Pass it
/// to [`timers::send_after_to`](crate::timers::send_after_to) or
/// [`timers::interval_to`](crate::timers::interval_to) to bind cross-actor
/// timer work to the actor that scheduled it.
#[derive(Clone, Debug)]
pub struct Lifetime {
    cancellation: CancellationToken,
}

impl Lifetime {
    pub(crate) fn from_token(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.cancellation.clone()
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
