use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{event::ExitStatusView, scope::ScopeKind, strategy::Strategy};

/// Point-in-time snapshot of a supervisor's state, including the state of every
/// child.
///
/// Snapshots are published via a `tokio::sync::watch` channel. The supervisor
/// updates the snapshot **before** staging the corresponding direct-child
/// [`LifecycleEvent`](crate::LifecycleEvent), so watchers reading the snapshot
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
    /// [`RestartPolicy::Always`](crate::RestartPolicy::Always); under group
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
    /// For a gap-free state-plus-stream view, create a lifecycle watch first,
    /// then read a snapshot, then discard watched events whose `seq` is less
    /// than or equal to this value. A pre-spawn snapshot projects statically
    /// configured children before their first `Added` transition; reducers
    /// should apply `Added` as an idempotent membership upsert.
    #[cfg_attr(feature = "serde", serde(default))]
    pub lifecycle_seq: u64,
    /// Ordered list of child snapshots, matching the supervisor's child order.
    pub children: Vec<ChildSnapshot>,
}

/// Point-in-time snapshot of a single child.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ChildSnapshot {
    /// The child's unique identifier.
    pub id: String,
    /// Monotonic identity of this membership within the stable supervisor
    /// identity.
    ///
    /// Unlike [`generation`](Self::generation), this changes when a child is
    /// removed and another child is added under the same id. Restarts of the
    /// same membership retain the epoch. The sequence continues across
    /// incarnations of a restart-stable nested supervisor; distinct nested
    /// supervisor identities maintain independent sequences. The counter
    /// saturates at [`u64::MAX`].
    pub membership_epoch: u64,
    /// Current generation counter. Incremented on each restart.
    pub generation: u64,
    /// Whether this child has reported readiness in its current generation.
    #[cfg_attr(feature = "serde", serde(default))]
    pub started: bool,
    /// Whether this generation exited permanently before reporting readiness.
    #[cfg_attr(feature = "serde", serde(default))]
    pub startup_aborted: bool,
    /// Current lifecycle state.
    pub state: ChildStateView,
    /// Whether the child is active or being removed.
    pub membership: ChildMembershipView,
    /// How the child last exited, if it has exited at least once.
    pub last_exit: Option<ExitStatusView>,
    /// Whether [`last_exit`](Self::last_exit) was the supervisor stopping the
    /// child rather than the child reaching its own conclusion.
    ///
    /// See [`LifecycleEventKind::Exited`](crate::LifecycleEventKind::Exited)
    /// for why a cancelled child can still report
    /// [`ExitStatusView::Completed`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_exit_cancelled: bool,
    /// Total number of times this child has been restarted.
    pub restart_count: u64,
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
    pub fn new(
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

    /// Sets the immutable scope kind represented by this snapshot.
    #[must_use]
    pub fn kind(mut self, kind: ScopeKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets the cumulative restart count recorded by this supervisor.
    #[must_use]
    pub fn total_restarts(mut self, total_restarts: u64) -> Self {
        self.total_restarts = total_restarts;
        self
    }

    /// Sets the sequence of the last emitted lifecycle event.
    #[must_use]
    pub fn lifecycle_seq(mut self, lifecycle_seq: u64) -> Self {
        self.lifecycle_seq = lifecycle_seq;
        self
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
            child = child.child(segment.as_ref())?;
        }

        Some(child)
    }
}

impl ChildSnapshot {
    /// Creates a child snapshot with active membership and no prior exit.
    ///
    /// Readiness, exit, restart, membership, and nested-supervisor details can
    /// be supplied with the builder-style setters.
    pub fn new(id: impl Into<String>, generation: u64, state: ChildStateView) -> Self {
        Self {
            id: id.into(),
            membership_epoch: 0,
            generation,
            started: false,
            startup_aborted: false,
            state,
            membership: ChildMembershipView::Active,
            last_exit: None,
            last_exit_cancelled: false,
            restart_count: 0,
            next_restart_in: None,
            supervisor: None,
        }
    }

    /// Sets the identity of this child membership.
    #[must_use]
    pub fn membership_epoch(mut self, membership_epoch: u64) -> Self {
        self.membership_epoch = membership_epoch;
        self
    }

    /// Sets whether the child reported readiness in this generation.
    #[must_use]
    pub fn started(mut self, started: bool) -> Self {
        self.started = started;
        self
    }

    /// Sets whether this generation permanently exited before readiness.
    #[must_use]
    pub fn startup_aborted(mut self, startup_aborted: bool) -> Self {
        self.startup_aborted = startup_aborted;
        self
    }

    /// Sets whether the child is active or being removed.
    #[must_use]
    pub fn membership(mut self, membership: ChildMembershipView) -> Self {
        self.membership = membership;
        self
    }

    /// Sets the child's most recent exit status.
    #[must_use]
    pub fn last_exit(mut self, last_exit: Option<ExitStatusView>) -> Self {
        self.last_exit = last_exit;
        self
    }

    /// Records whether the last exit was cancellation-driven.
    #[must_use]
    pub fn last_exit_cancelled(mut self, last_exit_cancelled: bool) -> Self {
        self.last_exit_cancelled = last_exit_cancelled;
        self
    }

    /// Sets the number of completed restarts.
    #[must_use]
    pub fn restart_count(mut self, restart_count: u64) -> Self {
        self.restart_count = restart_count;
        self
    }

    /// Sets the delay remaining before the next scheduled restart.
    #[must_use]
    pub fn next_restart_in(mut self, next_restart_in: Option<Duration>) -> Self {
        self.next_restart_in = next_restart_in;
        self
    }

    /// Sets the recursive snapshot for a nested supervisor child.
    #[must_use]
    pub fn supervisor(mut self, supervisor: Option<SupervisorSnapshot>) -> Self {
        self.supervisor = supervisor.map(Box::new);
        self
    }

    /// Looks up a grandchild by id within this child's nested supervisor
    /// snapshot. Returns `None` if this child is not a nested supervisor or
    /// has no child with the given id.
    pub fn child(&self, id: &str) -> Option<&ChildSnapshot> {
        self.supervisor.as_deref()?.child(id)
    }

    /// Walks a path through nested supervisor snapshots starting from this
    /// child. See [`SupervisorSnapshot::descendant`] for details.
    pub fn descendant<I, S>(&self, path: I) -> Option<&ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.supervisor.as_deref()?.descendant(path)
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

/// Lifecycle state of a child task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildStateView {
    /// The child has been created but its task has not yet started running.
    Starting,
    /// The child task is running.
    Running,
    /// The child is in the process of being stopped (token cancelled, waiting
    /// for exit).
    Stopping,
    /// The child has exited.
    Stopped,
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
    pub(crate) parent_instance: u64,
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
    parent_instance: u64,
}

impl SnapshotCell {
    pub(crate) fn new(
        notifications: mpsc::UnboundedSender<NestedSnapshotNotification>,
        state: NestedSnapshotState,
        parent_key: usize,
        parent_instance: u64,
    ) -> Self {
        Self {
            notifications,
            state,
            parent_key,
            parent_instance,
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
            parent_instance: self.parent_instance,
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

    use super::NestedSnapshotState;

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
    fn membership_epoch_is_required_when_deserializing() {
        use super::{ChildSnapshot, ChildStateView};

        let mut value =
            serde_json::to_value(ChildSnapshot::new("worker", 0, ChildStateView::Running))
                .expect("child snapshot serializes");
        value
            .as_object_mut()
            .expect("child snapshot serializes as an object")
            .remove("membership_epoch");

        let error = serde_json::from_value::<ChildSnapshot>(value)
            .expect_err("membership_epoch must be present");
        assert!(error.to_string().contains("membership_epoch"));
    }
}
