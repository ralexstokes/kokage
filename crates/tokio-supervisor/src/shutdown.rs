use std::time::Duration;

/// Shortest tidy-abort window, so even a very small grace leaves a wrapper
/// enough room to abort and join an inner task.
const MIN_TIDY_ABORT_BEAT: Duration = Duration::from_millis(1);
/// Longest tidy-abort window, so a large grace does not buy an unboundedly
/// long accounting tail.
const MAX_TIDY_ABORT_BEAT: Duration = Duration::from_millis(10);

/// The window in which a child wrapper can turn grace expiry into a tidy,
/// truthfully classified exit before the supervisor hard-aborts its task.
///
/// Derived from the child's own grace rather than fixed, so the accounting
/// tail stays proportional: a short-grace child is not made to wait out a
/// window larger than the budget it was configured with, while a long-grace
/// child does not extend teardown any further than it has to.
pub(crate) fn tidy_abort_beat(grace: Duration) -> Duration {
    (grace / 10).clamp(MIN_TIDY_ABORT_BEAT, MAX_TIDY_ABORT_BEAT)
}

/// How the supervisor stops a child task during shutdown or removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ShutdownMode {
    /// Like [`CooperativeThenAbort`](ShutdownMode::CooperativeThenAbort), but
    /// failing to exit within the grace period makes the enclosing shutdown or
    /// removal operation return a timeout error. Both cooperative modes still
    /// expose a [`ShutdownTimedOut`](crate::ExitStatusView::ShutdownTimedOut)
    /// child exit.
    ///
    /// On expiry the supervisor first signals
    /// [`ChildContext::abort_token`](crate::ChildContext::abort_token), then
    /// hard-aborts the task after a short accounting beat proportional to
    /// `grace`. Abort remains cooperative at Tokio poll boundaries, so a
    /// non-yielding future can outlive the shutdown call briefly. For
    /// hard-stop guarantees, isolate blocking work outside the supervised
    /// Tokio task.
    CooperativeStrict,
    /// Wait for the grace period, then escalate and return from the enclosing
    /// shutdown operation without a timeout error.
    ///
    /// On expiry the supervisor first signals
    /// [`ChildContext::abort_token`](crate::ChildContext::abort_token), then
    /// hard-aborts the task after a short accounting beat proportional to
    /// `grace`. Abort remains cooperative at Tokio poll boundaries, so a
    /// non-yielding future can outlive the shutdown call briefly. For
    /// hard-stop guarantees, isolate blocking work outside the supervised
    /// Tokio task.
    CooperativeThenAbort,
    /// Issue a Tokio abort and return promptly.
    ///
    /// Abort remains cooperative at Tokio poll boundaries, so this mode does not
    /// forcibly preempt a non-yielding future. For a nested supervisor child,
    /// the abort cascades recursively through the nested subtree instead of
    /// leaving that subtree to drain without a supervisor above it.
    Abort,
}

/// Shutdown behaviour for a single child, combining a [`ShutdownMode`] with a
/// grace period.
///
/// The default is [`CooperativeThenAbort`](ShutdownMode::CooperativeThenAbort)
/// with a 5-second grace period.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ShutdownPolicy {
    /// How long to wait for the child to exit after its cancellation token is
    /// triggered.
    pub grace: Duration,
    /// What to do when the grace period expires (or immediately, for
    /// [`Abort`](ShutdownMode::Abort)).
    pub mode: ShutdownMode,
}

impl ShutdownPolicy {
    /// Creates a policy with an explicit mode and grace period.
    pub fn new(grace: Duration, mode: ShutdownMode) -> Self {
        Self { grace, mode }
    }

    /// Strict cooperative shutdown: cancel the child and wait up to `grace`
    /// for it to exit. If the child does not exit within the grace period, the
    /// task is aborted and a timeout error is reported.
    pub fn cooperative_strict(grace: Duration) -> Self {
        Self::new(grace, ShutdownMode::CooperativeStrict)
    }

    /// Cancel the child and wait up to `grace`; if it has not exited by then,
    /// abort the Tokio task.
    pub fn cooperative_then_abort(grace: Duration) -> Self {
        Self::new(grace, ShutdownMode::CooperativeThenAbort)
    }

    /// Abort the Tokio task immediately with no grace period.
    pub fn abort() -> Self {
        Self::new(Duration::ZERO, ShutdownMode::Abort)
    }
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        Self::cooperative_then_abort(Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tidy_abort_beat_stays_proportional_and_clamped() {
        // A short grace is not made to wait out a window of its own size.
        assert_eq!(
            tidy_abort_beat(Duration::from_millis(20)),
            Duration::from_millis(2)
        );
        // The clamp keeps the tail bounded for a long grace.
        assert_eq!(tidy_abort_beat(Duration::from_secs(5)), MAX_TIDY_ABORT_BEAT);
        // ...and usable for a tiny or zero grace.
        assert_eq!(tidy_abort_beat(Duration::ZERO), MIN_TIDY_ABORT_BEAT);
        assert_eq!(
            tidy_abort_beat(ShutdownPolicy::default().grace),
            MAX_TIDY_ABORT_BEAT
        );
    }
}
