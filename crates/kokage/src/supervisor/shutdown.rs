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

/// Shutdown timing for any supervised child.
///
/// The default is [`graceful_for`](Self::graceful_for) with a five-second
/// bound. The child receives cancellation and may finish cooperatively before
/// the bound expires. Actor mailbox draining is a separate
/// [`MailboxShutdown`] policy because it has no meaning for tasks or subtrees.
///
/// [`abort`](Self::abort) has no cooperative grace. For a nested supervisor it
/// cascades recursively through the subtree rather than leaving descendants to
/// stop without a supervisor above them. Downstream matches need a catch-all
/// arm because the enum is non-exhaustive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Shutdown {
    /// Request cooperative shutdown with a maximum bound.
    Graceful {
        /// Maximum time allowed for cooperative shutdown and cleanup.
        grace: Duration,
    },
    /// Abort the child immediately without cooperative grace.
    Abort,
}

impl Shutdown {
    /// Requests cooperative shutdown for at most `bound`.
    pub const fn graceful_for(bound: Duration) -> Self {
        Self::Graceful { grace: bound }
    }

    /// Aborts the child immediately.
    pub const fn abort() -> Self {
        Self::Abort
    }

    pub(crate) const fn grace(self) -> Option<Duration> {
        match self {
            Self::Graceful { grace } => Some(grace),
            Self::Abort => None,
        }
    }

    pub(crate) const fn is_abort(self) -> bool {
        matches!(self, Self::Abort)
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::graceful_for(Duration::from_secs(5))
    }
}

/// How a handler actor treats messages already accepted when shutdown begins.
///
/// This policy is actor-only. Configure an individual actor through
/// [`ActorSpec`](crate::ActorSpec), or actors directly inside a scope through
/// [`Tree::default_actor_mailbox_shutdown`](crate::Tree::default_actor_mailbox_shutdown)
/// and
/// [`DynamicTree::default_actor_mailbox_shutdown`](crate::DynamicTree::default_actor_mailbox_shutdown).
/// It does not live on [`TaskSpec`](crate::TaskSpec) or
/// [`SubtreeSpec`](crate::SubtreeSpec), so queue behavior cannot be configured
/// where no mailbox exists.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum MailboxShutdown {
    /// Close intake and finish every accepted message before `on_stop`.
    #[default]
    Drain,
    /// Finish only the in-flight handler and discard queued messages.
    Discard,
}

impl MailboxShutdown {
    pub(crate) const fn drains(self) -> bool {
        matches!(self, Self::Drain)
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
            tidy_abort_beat(Shutdown::default().grace().unwrap_or_default()),
            MAX_TIDY_ABORT_BEAT
        );
    }
}
