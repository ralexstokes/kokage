use std::{fmt, sync::Arc};

use crate::CancellationToken;

type CancelAction = Arc<dyn Fn() + Send + Sync>;
type FinishedProbe = Arc<dyn Fn() -> bool + Send + Sync>;

/// Ownership handle for a cancellable background operation.
///
/// Guards cancel their operation when dropped. Keep the guard for as long as
/// the operation should remain live, or consume it with [`detach`](Self::detach)
/// to make fire-and-forget ownership explicit. Clones refer to the same
/// operation; dropping any non-detached clone cancels it.
#[derive(Clone)]
#[must_use = "dropping the guard cancels the operation; call `.detach()` for explicit fire-and-forget"]
pub struct Guard {
    cancellation: CancellationToken,
    is_finished: FinishedProbe,
    cancel_action: Option<CancelAction>,
    cancel_on_drop: bool,
}

impl Guard {
    /// Creates a guard around shared operation state.
    ///
    /// This constructor supports framework integrations. Most applications
    /// receive guards from kokage operations rather than constructing them.
    #[doc(hidden)]
    pub fn from_tokens(cancellation: CancellationToken, finished: CancellationToken) -> Self {
        Self {
            cancellation,
            is_finished: Arc::new(move || finished.is_cancelled()),
            cancel_action: None,
            cancel_on_drop: true,
        }
    }

    /// Creates a guard whose completion is reported by an operation probe.
    #[doc(hidden)]
    pub fn from_probe(
        cancellation: CancellationToken,
        is_finished: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancellation,
            is_finished: Arc::new(is_finished),
            cancel_action: None,
            cancel_on_drop: true,
        }
    }

    /// Creates a probe-backed guard with an operation-specific cancel action.
    #[doc(hidden)]
    pub fn from_probe_with_cancel(
        cancellation: CancellationToken,
        is_finished: impl Fn() -> bool + Send + Sync + 'static,
        cancel_action: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancellation,
            is_finished: Arc::new(is_finished),
            cancel_action: Some(Arc::new(cancel_action)),
            cancel_on_drop: true,
        }
    }

    /// Cancels the operation.
    ///
    /// Cancellation is idempotent. Work or message delivery already accepted
    /// by another component cannot be retracted.
    pub fn cancel(&self) {
        self.cancellation.cancel();
        if let Some(cancel_action) = &self.cancel_action {
            cancel_action();
        }
    }

    /// Returns whether cancellation has been requested or observed.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns whether the guarded background operation has finished.
    ///
    /// Cancellation can be requested before the operation has observed it, so
    /// this may briefly remain `false` after [`cancel`](Self::cancel).
    pub fn is_finished(&self) -> bool {
        (self.is_finished)()
    }

    /// Leaves the operation running without retaining a guard.
    ///
    /// This consumes and disarms this guard value. Other clones, if any,
    /// continue to cancel the shared operation when dropped.
    pub fn detach(mut self) {
        self.cancel_on_drop = false;
    }
}

impl fmt::Debug for Guard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Guard")
            .field("cancelled", &self.is_cancelled())
            .field("finished", &self.is_finished())
            .finish()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.cancel();
        }
    }
}
