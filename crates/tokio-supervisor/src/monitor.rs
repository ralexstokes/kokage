use std::fmt;

use tokio::sync::watch;

use crate::snapshot::SupervisorSnapshot;

/// Reliable observer of a supervisor's cumulative restart activity, created by
/// [`SupervisorHandle::watch_restarts`](crate::SupervisorHandle::watch_restarts).
///
/// The watch tracks [`SupervisorSnapshot::total_restarts`] over the lossless
/// snapshot `watch` channel. The channel conflates intermediate snapshots but
/// never lags, and the counter is cumulative, so none of the restarts the
/// counter covers is ever silently missed: a batch of conflated updates is
/// reported as a single delta covering every restart in the batch. This makes
/// it a sound control input for safety mechanisms such as aggregate restart
/// breakers — unlike counting
/// [`SupervisorEvent`](crate::SupervisorEvent)s from the lossy broadcast
/// channel.
///
/// The counter's scope is the watched supervisor's **direct children**:
/// restarts inside a nested supervisor are observed by watching that
/// supervisor's own handle, and under group strategies sibling respawns are
/// not counted. See [`SupervisorSnapshot::total_restarts`] for the exact
/// contract.
///
/// The baseline is captured when the watch is created; restarts recorded
/// before that are not reported.
#[deprecated(
    note = "use SupervisorHandle::watch_lifecycle and LifecycleEvent restart counters instead"
)]
pub struct RestartWatch {
    observed: u64,
    snapshots: watch::Receiver<SupervisorSnapshot>,
}

#[allow(deprecated)]
impl RestartWatch {
    pub(crate) fn new(mut snapshots: watch::Receiver<SupervisorSnapshot>) -> Self {
        let observed = snapshots.borrow_and_update().total_restarts;
        Self {
            observed,
            snapshots,
        }
    }

    /// The cumulative restart count observed so far, including the baseline
    /// captured at creation.
    pub fn observed(&self) -> u64 {
        self.observed
    }

    /// Waits until this supervisor's stable identity becomes terminal.
    ///
    /// Unlike [`next`](Self::next), this does not consume restart
    /// observations. It is useful when a consumer is temporarily blocked on
    /// another operation but must still react promptly to terminal closure.
    pub async fn closed(&self) {
        let mut snapshots = self.snapshots.clone();
        while snapshots.changed().await.is_ok() {}
    }

    /// Waits until the supervisor records further restarts and returns how
    /// many were recorded since the previous observation.
    ///
    /// A nested supervisor carries the counter across its own incarnations,
    /// so a watch on a restart-stable handle keeps reporting deltas through
    /// restarts of the watched supervisor itself — including restarts of an
    /// *ancestor*, which recreate the watched supervisor from the static
    /// configuration. (The nested supervisor's own restart is counted by the
    /// parent supervisor, not here.)
    ///
    /// Returns `None` once every recorded restart has been reported and the
    /// watched supervisor's stable identity can never produce another
    /// incarnation: the root supervisor stopped, the watched supervisor (or
    /// an ancestor) was removed, or it stopped by a decision no ancestor
    /// reincarnation can undo. `None` arrives eagerly (at the terminal event
    /// itself) for removal, for `OneForOne` stops, for
    /// [`RestartPolicy::Never`](crate::RestartPolicy::Never) children, and
    /// for a `RestForOne` first child; a child stopped at another position
    /// of a group-strategy supervisor could still be revived by a
    /// sibling-triggered group restart, so its watch closes only when that
    /// possibility ends — at the latest when the supervisor stops. A nested
    /// supervisor that is merely stopped under a live, restartable ancestor
    /// keeps the watch open — a later ancestor restart revives it and the
    /// watch resumes reporting.
    pub async fn next(&mut self) -> Option<u64> {
        loop {
            if let Some(delta) = self.observe() {
                return Some(delta);
            }

            if self.snapshots.changed().await.is_err() {
                return self.observe();
            }
        }
    }

    fn observe(&mut self) -> Option<u64> {
        let total = self.snapshots.borrow_and_update().total_restarts;
        // The counter is monotonic, even across nested-supervisor
        // incarnations; treat a (theoretically impossible) regression as a
        // fresh baseline rather than reporting a bogus delta.
        let delta = total.saturating_sub(self.observed);
        self.observed = total;
        (delta > 0).then_some(delta)
    }
}

#[allow(deprecated)]
impl fmt::Debug for RestartWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestartWatch")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}
