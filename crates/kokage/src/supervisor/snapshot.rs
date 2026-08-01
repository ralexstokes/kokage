use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};

/// Error returned when a supervisor snapshot stream can no longer change.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotRecvError {
    /// The supervisor identity is terminal and its snapshot sender was dropped.
    #[error("supervisor snapshot stream is closed")]
    Closed,
    /// An observed child membership disappeared before its predicate matched.
    #[error("child was removed before its snapshot predicate matched")]
    ChildRemoved,
}

/// A conflating stream of snapshots for one stable supervisor identity.
///
/// Each receiver tracks its own observed version. Cloning a receiver preserves
/// the source receiver's current observed version, while subsequent reads and
/// waits remain independent.
#[derive(Clone, Debug)]
pub struct SupervisorSnapshotReceiver {
    inner: watch::Receiver<SupervisorSnapshot>,
}

impl SupervisorSnapshotReceiver {
    pub(crate) fn new(inner: watch::Receiver<SupervisorSnapshot>) -> Self {
        Self { inner }
    }

    /// Returns a clone of the latest snapshot without marking it observed.
    pub fn latest(&self) -> SupervisorSnapshot {
        self.inner.borrow().clone()
    }

    fn latest_observed(&mut self) -> SupervisorSnapshot {
        self.inner.borrow_and_update().clone()
    }

    /// Waits for a new snapshot, marks it observed, and returns a clone.
    pub async fn changed(&mut self) -> Result<SupervisorSnapshot, SnapshotRecvError> {
        self.inner
            .changed()
            .await
            .map_err(|_| SnapshotRecvError::Closed)?;
        Ok(self.latest_observed())
    }

    /// Waits until the latest snapshot satisfies `predicate`.
    ///
    /// Every examined snapshot is marked observed by this receiver. Snapshot
    /// delivery is conflating, so intermediate values may be skipped.
    pub async fn wait_for(
        &mut self,
        mut predicate: impl FnMut(&SupervisorSnapshot) -> bool,
    ) -> Result<SupervisorSnapshot, SnapshotRecvError> {
        loop {
            let snapshot = self.latest_observed();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            self.inner
                .changed()
                .await
                .map_err(|_| SnapshotRecvError::Closed)?;
        }
    }

    /// Waits until `child_id` exists and its snapshot satisfies `predicate`.
    ///
    /// This is the concise path for predicates over the latest state, such as
    /// readiness or "running above this baseline generation." It delegates to
    /// [`wait_for`](Self::wait_for), so intermediate generations and exits may
    /// be skipped. Predicates that track progress should therefore be
    /// monotonic (for example, `generation > baseline`, not
    /// `generation == baseline + 1`). Use a lifecycle watch when every edge is
    /// required, and use [`wait_for`](Self::wait_for) on the whole snapshot for
    /// absence/removal predicates. A child that is not a current member may
    /// appear later in a dynamic scope. Once this receiver observes the child,
    /// removal before the predicate matches returns
    /// [`SnapshotRecvError::ChildRemoved`].
    pub async fn wait_for_child(
        &mut self,
        child_id: &str,
        mut predicate: impl FnMut(&ChildSnapshot) -> bool,
    ) -> Result<ChildSnapshot, SnapshotRecvError> {
        let mut seen = false;
        loop {
            let snapshot = self.latest_observed();
            match snapshot.child(child_id) {
                Some(child) => {
                    seen = true;
                    if predicate(child) {
                        return Ok(child.clone());
                    }
                }
                None if seen => return Err(SnapshotRecvError::ChildRemoved),
                None => {}
            }
            self.inner
                .changed()
                .await
                .map_err(|_| SnapshotRecvError::Closed)?;
        }
    }
}

use crate::supervisor::{
    event::ExitKind, restart::RestartPolicy, scope::ScopeKind, strategy::Strategy,
};

/// Point-in-time snapshot of a supervisor's state, including the state of every
/// child.
///
/// Snapshots are published via a `tokio::sync::watch` channel. The supervisor
/// updates the snapshot **before** staging the corresponding direct-child
/// [`LifecycleEvent`](crate::observe::LifecycleEvent), so watchers reading the snapshot
/// from an event handler will see already-consistent state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct SupervisorSnapshot {
    /// Current lifecycle state of the supervisor.
    pub state: SupervisorStateView,
    /// The supervisor's immutable membership and ordering model.
    #[cfg_attr(feature = "serde", serde(default))]
    pub kind: ScopeKind,
    /// The restart strategy in use.
    pub strategy: Strategy,
    /// Cumulative number of restarts this supervisor has scheduled for its
    /// direct children — exactly the occurrences the restart-intensity window
    /// records. That includes clean exits restarted under
    /// [`RestartPolicy::always()`](crate::RestartPolicy::always()); under group
    /// strategies such as [`Strategy::OneForAll`], sibling respawns caused by
    /// another child's exit do not increment it — only the exiting child's
    /// scheduled restart counts.
    ///
    /// The counter is monotonic for the supervisor's stable identity. Unlike
    /// the per-child [`ChildSnapshot::restart_count`], it keeps the restarts
    /// of children that have since been removed, and a nested supervisor
    /// carries it across its own incarnations: a replacement incarnation
    /// resumes from its predecessor's count (the nested supervisor's own
    /// restart increments the parent's counter).
    ///
    /// Because snapshots are delivered over a `watch` channel (which conflates
    /// intermediate values but never lags), deltas of this counter are a
    /// reliable way to observe restart activity. Lifecycle events carry this
    /// same counter at emission.
    ///
    /// The counter only covers direct children. Restarts inside a nested
    /// supervisor are visible on that nested snapshot's own `total_restarts`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub total_restarts: u64,
    /// Sequence of the last lifecycle event emitted when this snapshot was
    /// published.
    ///
    /// For a gap-free direct-child state-plus-stream view, use
    /// [`ScopeRef::observe_children`](crate::ScopeRef::observe_children). Its
    /// specialized watch uses this value internally to suppress transitions
    /// already reflected by the initial snapshot and later recovery resets.
    /// A pre-spawn snapshot projects statically configured children before
    /// their first `Added` transition; reducers should apply `Added` as an
    /// idempotent membership upsert.
    #[cfg_attr(feature = "serde", serde(default))]
    pub lifecycle_seq: u64,
    /// Ordered list of child snapshots, matching the supervisor's child order.
    pub children: Vec<ChildSnapshot>,
}

/// Point-in-time snapshot of a single child.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[non_exhaustive]
pub struct ChildSnapshot {
    /// The child's unique identifier.
    pub id: String,
    /// Supervisor-wide identity of this child's restart-linked occupancy chain.
    pub lineage: u64,
    /// Current generation counter. Incremented on each restart.
    pub generation: u64,
    /// Current lifecycle state, including readiness and prior-exit details
    /// that are meaningful for that state.
    pub state: ChildStateView,
    /// Whether the child is active or being removed.
    pub membership: ChildMembershipView,
    /// Total number of times this child has been restarted.
    pub restart_count: u64,
    /// Policy that determines whether the current exit is eligible to restart.
    #[cfg_attr(feature = "serde", serde(default))]
    pub restart_policy: RestartPolicy,
    /// Whether this membership is removed after a terminal exit.
    #[cfg_attr(feature = "serde", serde(default))]
    pub remove_on_terminal_exit: bool,
    /// Time remaining until the next scheduled restart, if a backoff delay is
    /// pending.
    pub next_restart_in: Option<Duration>,
    /// If this child is a first-class nested supervisor, this contains its
    /// recursive snapshot.
    pub supervisor: Option<Box<SupervisorSnapshot>>,
}

impl SupervisorSnapshot {
    /// Creates a supervisor snapshot from its state, strategy, and children.
    ///
    /// This is primarily useful for adapters and tests that produce snapshot
    /// streams without running a supervisor.
    #[cfg(test)]
    pub(crate) fn new(
        state: SupervisorStateView,
        strategy: Strategy,
        children: Vec<ChildSnapshot>,
    ) -> Self {
        Self {
            state,
            kind: ScopeKind::Ordered,
            strategy,
            total_restarts: 0,
            lifecycle_seq: 0,
            children,
        }
    }

    /// Looks up a direct child by id.
    pub fn child(&self, id: &str) -> Option<&ChildSnapshot> {
        self.children.iter().find(|child| child.id == id)
    }

    /// Walks a dot-separated path of child ids through nested supervisors.
    ///
    /// For example, `descendant(["db_pool", "writer"])` first finds child
    /// `"db_pool"` at this level, then looks for `"writer"` inside its nested
    /// supervisor snapshot.
    pub fn descendant<I, S>(&self, path: I) -> Option<&ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut path = path.into_iter();
        let mut child = self.child(path.next()?.as_ref())?;

        for segment in path {
            child = child.supervisor.as_deref()?.child(segment.as_ref())?;
        }

        Some(child)
    }
}

impl ChildSnapshot {
    /// Creates a child snapshot with active membership and no prior exit.
    ///
    /// Additional details can be assigned through the snapshot's public
    /// fields.
    #[cfg(test)]
    pub(crate) fn new(id: impl Into<String>, generation: u64, state: ChildStateView) -> Self {
        Self {
            id: id.into(),
            lineage: 0,
            generation,
            state,
            membership: ChildMembershipView::Active,
            restart_count: 0,
            restart_policy: RestartPolicy::default(),
            remove_on_terminal_exit: false,
            next_restart_in: None,
            supervisor: None,
        }
    }
}

/// Lifecycle state of a supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum SupervisorStateView {
    /// The supervisor is running and accepting commands.
    Running,
    /// The supervisor is shutting down (children are being stopped).
    Stopping,
    /// The supervisor has fully stopped.
    Stopped,
}

/// Public observational details of an actor or supervised child exit.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ExitStatus {
    /// The child returned `Ok(())`.
    Completed {
        /// Whether shutdown was requested for this generation.
        cancelled: bool,
    },
    /// The child returned an error.
    Failed {
        /// The error's `Display` output.
        message: String,
        /// Whether shutdown was requested for this generation.
        cancelled: bool,
    },
    /// The child task panicked.
    Panicked {
        /// Whether shutdown was requested for this generation.
        cancelled: bool,
    },
    /// The child task was aborted by its controller.
    Aborted {
        /// Whether cooperative shutdown exhausted its grace period first.
        after_grace: bool,
        /// Whether shutdown was requested for this generation.
        cancelled: bool,
    },
}

impl ExitStatus {
    pub(crate) fn new(status: ExitKind, cancelled: bool) -> Self {
        match status {
            ExitKind::Completed => Self::Completed { cancelled },
            ExitKind::Failed(message) => Self::Failed { message, cancelled },
            ExitKind::Panicked => Self::Panicked { cancelled },
            ExitKind::Aborted { after_grace } => Self::Aborted {
                after_grace,
                cancelled,
            },
        }
    }

    /// Returns whether the exit represents a failure.
    pub fn is_failure(&self) -> bool {
        !matches!(self, Self::Completed { .. })
    }

    /// Returns whether the child completed successfully.
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// Returns the failure message when the child returned an error.
    pub fn failure_message(&self) -> Option<&str> {
        match self {
            Self::Failed { message, .. } => Some(message),
            _ => None,
        }
    }

    /// Returns whether the child task panicked.
    pub fn is_panicked(&self) -> bool {
        matches!(self, Self::Panicked { .. })
    }

    /// Returns whether the controller aborted the child after its grace
    /// period expired.
    pub fn timed_out(&self) -> bool {
        matches!(
            self,
            Self::Aborted {
                after_grace: true,
                ..
            }
        )
    }

    /// Returns whether shutdown was requested for this generation.
    pub fn cancelled(&self) -> bool {
        match self {
            Self::Completed { cancelled }
            | Self::Failed { cancelled, .. }
            | Self::Panicked { cancelled }
            | Self::Aborted { cancelled, .. } => *cancelled,
        }
    }
}

/// Lifecycle state of a child task.
///
/// State-specific data is nested here so snapshots cannot encode impossible
/// combinations such as a running child whose startup was aborted.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildStateView {
    /// The child has been created but its task has not yet started running.
    Starting {
        /// Exit of the preceding generation, when this is a restart.
        previous_exit: Option<ExitStatus>,
    },
    /// The child task is running.
    Running {
        /// Exit of the preceding generation, when this is a restart.
        previous_exit: Option<ExitStatus>,
    },
    /// The child is in the process of being stopped (token cancelled, waiting
    /// for exit).
    Stopping {
        /// Whether this generation reported readiness before stopping began.
        started: bool,
        /// Exit of the preceding generation, when this generation is a restart.
        previous_exit: Option<ExitStatus>,
    },
    /// The child has exited.
    Stopped {
        /// Whether this generation reported readiness before it stopped.
        started: bool,
        /// Exit of this generation, or `None` if it was never spawned.
        exit: Option<ExitStatus>,
    },
    /// The child stopped permanently before reporting readiness.
    StartupAborted {
        /// Exit of this generation.
        exit: ExitStatus,
    },
}

impl ChildStateView {
    /// Returns whether the child is running.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// Returns whether the child can no longer make progress in its current
    /// generation.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped { .. } | Self::StartupAborted { .. })
    }

    /// Returns the newest observed exit, if any.
    pub fn last_exit(&self) -> Option<&ExitStatus> {
        match self {
            Self::Starting { previous_exit }
            | Self::Running { previous_exit }
            | Self::Stopping { previous_exit, .. } => previous_exit.as_ref(),
            Self::Stopped { exit, .. } => exit.as_ref(),
            Self::StartupAborted { exit } => Some(exit),
        }
    }
}

/// Whether a child is a permanent member of the supervisor or is being removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildMembershipView {
    /// The child is an active member of the supervisor.
    Active,
    /// A removal has been requested and is in progress.
    Removing,
}

// ---------------------------------------------------------------------------
// Internal: nested snapshot forwarding
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct NestedSnapshotNotification {
    pub(crate) parent_key: usize,
    pub(crate) parent_lineage: u64,
    pub(crate) generation: u64,
}

/// Coalescing state for nested supervisor snapshot updates.
///
/// Uses an atomic `queued` flag to avoid flooding the parent's notification
/// channel: if a notification is already in flight, subsequent updates replace
/// the stored snapshot but do not send another notification. The parent
/// dequeues the latest snapshot when it processes the notification.
#[derive(Clone, Default)]
pub(crate) struct NestedSnapshotState {
    latest: Arc<Mutex<Option<SupervisorSnapshot>>>,
    queued: Arc<AtomicBool>,
}

impl NestedSnapshotState {
    pub(crate) fn clear(&self) {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = None;
        self.queued.store(false, Ordering::Release);
    }

    pub(crate) fn replace_if_changed(&self, snapshot: SupervisorSnapshot) -> bool {
        let mut latest = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
        if latest.as_ref() == Some(&snapshot) {
            return false;
        }
        *latest = Some(snapshot);
        true
    }

    pub(crate) fn latest(&self) -> Option<SupervisorSnapshot> {
        self.latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Attempts to mark a notification as queued. Returns `true` if this call
    /// transitioned the flag from `false` to `true` (i.e. the caller should
    /// send a notification). Returns `false` if a notification was already
    /// queued.
    pub(crate) fn try_queue(&self) -> bool {
        !self.queued.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn mark_dequeued(&self) {
        self.queued.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct SnapshotCell {
    notifications: mpsc::UnboundedSender<NestedSnapshotNotification>,
    state: NestedSnapshotState,
    parent_key: usize,
    parent_lineage: u64,
}

impl SnapshotCell {
    pub(crate) fn new(
        notifications: mpsc::UnboundedSender<NestedSnapshotNotification>,
        state: NestedSnapshotState,
        parent_key: usize,
        parent_lineage: u64,
    ) -> Self {
        Self {
            notifications,
            state,
            parent_key,
            parent_lineage,
        }
    }

    pub(crate) fn forward(&self, snapshot: SupervisorSnapshot, generation: u64) {
        if !self.state.replace_if_changed(snapshot) {
            return;
        }
        if !self.state.try_queue() {
            return;
        }

        let notification = NestedSnapshotNotification {
            parent_key: self.parent_key,
            parent_lineage: self.parent_lineage,
            generation,
        };

        if self.notifications.send(notification).is_err() {
            self.state.mark_dequeued();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::watch;

    use super::{
        NestedSnapshotState, SnapshotRecvError, SupervisorSnapshot, SupervisorSnapshotReceiver,
        SupervisorStateView,
    };
    use crate::supervisor::Strategy;

    fn snapshot(total_restarts: u64) -> SupervisorSnapshot {
        let mut snapshot = SupervisorSnapshot::new(
            SupervisorStateView::Running,
            Strategy::OneForOne,
            Vec::new(),
        );
        snapshot.total_restarts = total_restarts;
        snapshot
    }

    #[tokio::test]
    async fn snapshot_receiver_conflates_and_tracks_versions_per_receiver() {
        let (sender, inner) = watch::channel(snapshot(0));
        let mut first = SupervisorSnapshotReceiver::new(inner);
        let mut second = first.clone();

        sender.send(snapshot(1)).expect("receivers remain open");
        sender.send(snapshot(2)).expect("receivers remain open");

        assert_eq!(first.latest().total_restarts, 2);
        assert_eq!(first.changed().await.unwrap().total_restarts, 2);

        assert_eq!(second.changed().await.unwrap().total_restarts, 2);
    }

    #[tokio::test]
    async fn snapshot_receiver_waits_for_matches_and_reports_closure() {
        let (sender, inner) = watch::channel(snapshot(0));
        let mut receiver = SupervisorSnapshotReceiver::new(inner);

        sender.send(snapshot(1)).expect("receiver remains open");
        sender.send(snapshot(3)).expect("receiver remains open");
        let matched = receiver
            .wait_for(|snapshot| snapshot.total_restarts >= 2)
            .await
            .expect("matching snapshot is available");
        assert_eq!(matched.total_restarts, 3);

        drop(sender);
        assert_eq!(receiver.changed().await, Err(SnapshotRecvError::Closed));
    }

    #[test]
    fn nested_snapshot_state_recovers_after_mutex_poisoning() {
        let state = NestedSnapshotState::default();
        let latest = Arc::clone(&state.latest);
        let _ = std::panic::catch_unwind(|| {
            let _guard = latest.lock().expect("fresh mutex locks");
            panic!("poison the nested snapshot state");
        });

        assert_eq!(state.latest(), None);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn lineage_is_required_when_deserializing() {
        use super::{ChildSnapshot, ChildStateView};

        let mut value = serde_json::to_value(ChildSnapshot::new(
            "worker",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        ))
        .expect("child snapshot serializes");
        value
            .as_object_mut()
            .expect("child snapshot serializes as an object")
            .remove("lineage");

        let error =
            serde_json::from_value::<ChildSnapshot>(value).expect_err("lineage must be present");
        assert!(error.to_string().contains("lineage"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn remove_on_terminal_exit_round_trips_and_rejects_the_retired_key() {
        use super::{ChildSnapshot, ChildStateView};

        let mut snapshot = ChildSnapshot::new(
            "worker",
            0,
            ChildStateView::Running {
                previous_exit: None,
            },
        );
        snapshot.remove_on_terminal_exit = true;
        let value = serde_json::to_value(&snapshot).expect("child snapshot serializes");
        assert_eq!(value["remove_on_terminal_exit"], true);
        let decoded: ChildSnapshot =
            serde_json::from_value(value.clone()).expect("child snapshot deserializes");
        assert!(decoded.remove_on_terminal_exit);

        let retired = value
            .to_string()
            .replacen("remove_on_terminal_exit", "remove_when_done", 1);
        let error = serde_json::from_str::<ChildSnapshot>(&retired)
            .expect_err("retired terminal membership key must not deserialize");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }
}
