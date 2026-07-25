use tokio_util::sync::CancellationToken;

/// Cloneable handle for cancelling an actor-owned operation.
///
/// Returned by actor timers and watches. Clones refer to the same operation,
/// and dropping a handle does not cancel it; call [`cancel`](Self::cancel)
/// explicitly. Cancellation cannot retract a message already accepted by an
/// actor mailbox.
#[derive(Clone, Debug)]
pub struct CancellationHandle {
    cancellation: CancellationToken,
}

impl CancellationHandle {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
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
}
