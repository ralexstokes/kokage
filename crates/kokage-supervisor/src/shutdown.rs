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

/// How the supervisor stops a child task during shutdown, removal, or restart.
///
/// With the `serde` feature, this enum uses Serde's externally tagged enum
/// representation. It replaces the former `{ mode, grace }` struct shape, so
/// persisted policy values using that shape require migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ShutdownPolicy {
    /// Ask the child to stop and wait for `grace` before aborting its task.
    ///
    /// Timing out is reported by the enclosing shutdown or removal operation.
    /// During a group restart, expiry instead escalates the old generation to
    /// abort; the restart continues once the old task exits. A caller that only
    /// needs best-effort cleanup may explicitly ignore a shutdown or removal
    /// error. On expiry the supervisor first signals
    /// [`ChildContext::abort_token`](crate::ChildContext::abort_token), then
    /// hard-aborts the task after a short accounting beat proportional to
    /// `grace`. Abort remains cooperative at Tokio poll boundaries, so a
    /// non-yielding future can outlive the shutdown call briefly. For
    /// hard-stop guarantees, isolate blocking work outside the supervised
    /// Tokio task.
    Cooperative {
        /// Maximum time to wait before escalating to abort.
        grace: Duration,
    },
    /// Issue a Tokio abort and return promptly.
    ///
    /// Abort remains cooperative at Tokio poll boundaries, so this mode does not
    /// forcibly preempt a non-yielding future. For a nested supervisor child,
    /// the abort cascades recursively through the nested subtree instead of
    /// leaving that subtree to drain without a supervisor above it.
    Abort,
}

impl ShutdownPolicy {
    pub(crate) const fn grace(self) -> Duration {
        match self {
            Self::Cooperative { grace } => grace,
            Self::Abort => Duration::ZERO,
        }
    }

    pub(crate) const fn is_abort(self) -> bool {
        matches!(self, Self::Abort)
    }
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        Self::Cooperative {
            grace: Duration::from_secs(5),
        }
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
            tidy_abort_beat(ShutdownPolicy::default().grace()),
            MAX_TIDY_ABORT_BEAT
        );
    }
}
