use std::{
    fmt,
    future::{Future, IntoFuture},
    pin::Pin,
};

use tokio::sync::watch;

use crate::{
    error::RestartMonitorError,
    snapshot::{ChildMembershipView, ChildStateView, SupervisorSnapshot, SupervisorStateView},
};

/// Awaitable created by [`SupervisorHandle::monitor_restart`](crate::SupervisorHandle::monitor_restart).
///
/// The monitor resolves when the watched direct child is next observed
/// [`Running`](ChildStateView::Running) with a generation greater than the
/// baseline captured at construction time. Explicitly readiness-gated
/// children become running only after they report readiness. This is a
/// convenience predicate over the primitive supervisor snapshot watch, not a
/// separate event channel.
pub struct RestartMonitor {
    id: String,
    baseline_generation: Option<u64>,
    snapshots: watch::Receiver<SupervisorSnapshot>,
}

impl RestartMonitor {
    pub(crate) fn new(
        id: String,
        baseline_generation: Option<u64>,
        snapshots: watch::Receiver<SupervisorSnapshot>,
    ) -> Self {
        Self {
            id,
            baseline_generation,
            snapshots,
        }
    }

    /// The child id this monitor watches.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The generation observed when the monitor was created, or `None` if the
    /// child was already unavailable.
    pub fn baseline_generation(&self) -> Option<u64> {
        self.baseline_generation
    }

    async fn wait(mut self) -> Result<u64, RestartMonitorError> {
        let Some(baseline_generation) = self.baseline_generation else {
            return Err(RestartMonitorError::ChildUnavailable(self.id));
        };

        loop {
            let observation = {
                let snapshot = self.snapshots.borrow();
                observe_snapshot(&snapshot, &self.id, baseline_generation)
            };

            match observation {
                RestartObservation::Resolved(generation) => return Ok(generation),
                RestartObservation::Failed(error) => return Err(error),
                RestartObservation::Pending => {}
            }

            self.snapshots
                .changed()
                .await
                .map_err(|_| RestartMonitorError::SupervisorStopped)?;
        }
    }
}

impl fmt::Debug for RestartMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestartMonitor")
            .field("id", &self.id)
            .field("baseline_generation", &self.baseline_generation)
            .finish_non_exhaustive()
    }
}

impl IntoFuture for RestartMonitor {
    type Output = Result<u64, RestartMonitorError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'static>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

/// Reliable observer of a supervisor's cumulative restart activity, created by
/// [`SupervisorHandle::watch_restarts`](crate::SupervisorHandle::watch_restarts).
///
/// This is a convenience fold over the primitive snapshot watch. It tracks
/// [`SupervisorSnapshot::total_restarts`] over the lossless `watch` channel.
/// The channel conflates intermediate snapshots but never lags, and the
/// counter is cumulative, so none of the restarts the counter covers is ever
/// silently missed: a batch of conflated updates is reported as a single
/// delta covering every restart in the batch. This makes it a sound control
/// input for safety mechanisms such as aggregate restart breakers — unlike counting
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
pub struct RestartWatch {
    observed: u64,
    snapshots: watch::Receiver<SupervisorSnapshot>,
}

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

impl fmt::Debug for RestartWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestartWatch")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

enum RestartObservation {
    Resolved(u64),
    Failed(RestartMonitorError),
    Pending,
}

fn observe_snapshot(
    snapshot: &SupervisorSnapshot,
    id: &str,
    baseline_generation: u64,
) -> RestartObservation {
    let Some(child) = snapshot.child(id) else {
        return RestartObservation::Failed(RestartMonitorError::ChildUnavailable(id.to_owned()));
    };

    if child.generation > baseline_generation && child.state == ChildStateView::Running {
        return RestartObservation::Resolved(child.generation);
    }

    if child.membership == ChildMembershipView::Removing {
        return RestartObservation::Failed(RestartMonitorError::ChildUnavailable(id.to_owned()));
    }

    if snapshot.state == SupervisorStateView::Stopped {
        return RestartObservation::Failed(RestartMonitorError::SupervisorStopped);
    }

    RestartObservation::Pending
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::Strategy;

    #[test]
    fn resolved_generation_wins_over_terminal_snapshot_state() {
        let snapshot = SupervisorSnapshot {
            state: SupervisorStateView::Stopped,
            strategy: Strategy::OneForOne,
            total_restarts: 1,
            children: vec![crate::ChildSnapshot {
                id: "worker".to_owned(),
                membership_epoch: 0,
                generation: 1,
                started: true,
                startup_aborted: false,
                state: ChildStateView::Running,
                membership: ChildMembershipView::Removing,
                last_exit: None,
                restart_count: 1,
                next_restart_in: None,
                supervisor: None,
            }],
        };

        match observe_snapshot(&snapshot, "worker", 0) {
            RestartObservation::Resolved(1) => {}
            RestartObservation::Resolved(generation) => {
                panic!("resolved with unexpected generation {generation}");
            }
            RestartObservation::Failed(error) => {
                panic!("terminal state won over resolution: {error}");
            }
            RestartObservation::Pending => panic!("snapshot should have resolved"),
        }
    }
}
