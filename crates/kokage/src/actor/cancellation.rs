use kokage_supervisor::CancellationToken;

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
