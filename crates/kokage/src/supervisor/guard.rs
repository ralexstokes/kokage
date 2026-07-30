use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::supervisor::CancellationToken;

struct CancelAction {
    invoked: AtomicBool,
    action: Box<dyn Fn() + Send + Sync>,
}

impl CancelAction {
    fn new(action: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            invoked: AtomicBool::new(false),
            action: Box::new(action),
        }
    }

    fn invoke(&self) {
        if self
            .invoked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            (self.action)();
        }
    }
}

/// Ownership handle for a cancellable background operation.
///
/// Guards cancel their operation when dropped. Keep the guard for as long as
/// the operation should remain live, or consume it with [`detach`](Self::detach)
/// to make fire-and-forget ownership explicit. A guard is intentionally not
/// cloneable: wrap it in [`Arc`](std::sync::Arc) when several owners need shared
/// access to the same cancellation authority.
#[must_use = "dropping the guard cancels the operation; call `.detach()` for explicit fire-and-forget"]
pub struct Guard {
    cancellation: CancellationToken,
    finished: CancellationToken,
    cancel_action: Option<Arc<CancelAction>>,
    cancel_on_drop: bool,
}

impl Guard {
    pub(crate) fn from_tokens(
        cancellation: CancellationToken,
        finished: CancellationToken,
    ) -> Self {
        Self {
            cancellation,
            finished,
            cancel_action: None,
            cancel_on_drop: true,
        }
    }

    pub(crate) fn from_tokens_with_cancel(
        cancellation: CancellationToken,
        finished: CancellationToken,
        cancel_action: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancellation,
            finished,
            cancel_action: Some(Arc::new(CancelAction::new(cancel_action))),
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
            cancel_action.invoke();
        }
    }

    /// Returns whether cancellation was explicitly requested through this
    /// guard.
    ///
    /// Calling [`cancel`](Self::cancel) or dropping an armed guard marks the
    /// operation cancelled. Normal completion and environmental termination,
    /// such as an actor incarnation or target ending, leave this `false`; use
    /// [`is_finished`](Self::is_finished) to observe those outcomes.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns whether the guarded background operation has finished.
    ///
    /// Cancellation can be requested before the operation has observed it, so
    /// this may briefly remain `false` after [`cancel`](Self::cancel).
    pub fn is_finished(&self) -> bool {
        self.finished.is_cancelled()
    }

    /// Waits until the guarded background operation has finished.
    ///
    /// Cancellation can be requested before the operation has observed it, so
    /// this waits for the operation itself to terminate after
    /// [`cancel`](Self::cancel).
    pub async fn finished(&self) {
        self.finished.cancelled().await;
    }

    /// Leaves the operation running without retaining a guard.
    ///
    /// This consumes and disarms the guard so its drop does not cancel the
    /// operation.
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::Guard;
    use crate::CancellationToken;

    #[test]
    fn dropping_an_armed_guard_cancels_the_operation() {
        let cancellation = CancellationToken::new();
        let observed = cancellation.clone();
        let guard = Guard::from_tokens(cancellation, CancellationToken::new());

        drop(guard);

        assert!(observed.is_cancelled());
    }

    #[test]
    fn detaching_a_guard_leaves_the_operation_running() {
        let cancellation = CancellationToken::new();
        let observed = cancellation.clone();
        let guard = Guard::from_tokens(cancellation, CancellationToken::new());

        guard.detach();
        assert!(!observed.is_cancelled());
    }

    #[tokio::test]
    async fn finished_state_is_independent_from_cancellation() {
        let cancellation = CancellationToken::new();
        let finished = CancellationToken::new();
        finished.cancel();
        let guard = Guard::from_tokens(cancellation, finished);

        guard.finished().await;
        assert!(guard.is_finished());
        assert!(!guard.is_cancelled());
    }

    #[test]
    fn operation_cancel_action_is_invoked_only_once() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&invocations);
        let guard = Guard::from_tokens_with_cancel(
            CancellationToken::new(),
            CancellationToken::new(),
            move || {
                counted.fetch_add(1, Ordering::Relaxed);
            },
        );
        guard.cancel();
        guard.cancel();
        drop(guard);

        assert_eq!(invocations.load(Ordering::Relaxed), 1);
    }
}
