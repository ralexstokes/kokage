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

/// Complete shutdown behavior for a supervised child.
///
/// For handler actors the value controls both the actor receive loop and the
/// supervisor grace period, so a drain can never be configured without the
/// bound that contains it. For task children, the actor-drain half is inert.
///
/// The default is [`drain_for`](Self::drain_for) with a five-second bound:
/// accepted work is finished unless the bound expires. Keep draining when a
/// dropped message would lose work no peer will redo, such as an unflushed
/// write or a request whose caller awaits a reply.
/// Choose [`discard_after_current`](Self::discard_after_current) for
/// replaceable work such as snapshots, ticks, polls, or requests the sender
/// retries. Neither mode is an end-to-end delivery guarantee; applications
/// that require one need acknowledgements and replay.
///
/// A draining actor must remain correct while peers stop. Ordered siblings
/// stop in reverse declaration order, so a handler that sends during its drain
/// must tolerate a sibling already being gone. Size the bound for the whole
/// queued prefix—roughly mailbox depth times worst-case handler latency—plus
/// cleanup. Expiry discards the remaining queue and can skip cleanup.
///
/// [`abort`](Self::abort) has no cooperative grace. For a nested supervisor it
/// cascades recursively through the subtree rather than leaving descendants to
/// drain without a supervisor above them. Match this enum directly when
/// inspecting a declaration; unlike the cooperative variants, `Abort` carries
/// no grace value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Shutdown {
    /// Close actor intake and drain every accepted message within the grace.
    Drain {
        /// Maximum time allowed for draining and cleanup.
        grace: Duration,
    },
    /// Finish only the in-flight actor message and discard queued work.
    Discard {
        /// Maximum time allowed for the in-flight handler and cleanup.
        grace: Duration,
    },
    /// Abort the child immediately without cooperative grace.
    Abort,
}

impl Shutdown {
    /// Closes intake and drains every accepted actor message for at most `bound`.
    pub const fn drain_for(bound: Duration) -> Self {
        Self::Drain { grace: bound }
    }

    /// Finishes the in-flight handler, discards queued work, and allows
    /// cleanup to run for at most `bound`.
    pub const fn discard_after_current(bound: Duration) -> Self {
        Self::Discard { grace: bound }
    }

    /// Aborts the child immediately.
    pub const fn abort() -> Self {
        Self::Abort
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::drain_for(Duration::from_secs(5))
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
            tidy_abort_beat(match Shutdown::default() {
                Shutdown::Drain { grace } | Shutdown::Discard { grace } => grace,
                Shutdown::Abort => Duration::ZERO,
            }),
            MAX_TIDY_ABORT_BEAT
        );
    }
}
