use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Notify, futures::Notified};

use crate::ExitStatusView;

const LIFECYCLE_BUFFER_CAPACITY: usize = 128;

const _: () = assert!(LIFECYCLE_BUFFER_CAPACITY >= 2);

/// One nested-supervisor edge in a lifecycle event's path.
///
/// The tuple `(id, lineage, generation)` identifies the exact
/// supervisor incarnation that forwarded the event. In particular,
/// `lineage` distinguishes a removed subtree from a later subtree
/// inserted under the same id.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct LifecyclePathSegment {
    /// Child id of the nested supervisor.
    pub id: String,
    /// Identity of that child membership in its parent scope.
    pub lineage: u64,
    /// Generation of the nested supervisor child.
    pub generation: u64,
}

impl LifecyclePathSegment {
    /// Creates one exact nested-supervisor path segment.
    ///
    /// This is primarily useful with [`LifecycleWatch::started_after`] when a
    /// caller already knows the supervisor membership it intends to observe.
    pub fn new(id: impl Into<String>, lineage: u64, generation: u64) -> Self {
        Self {
            id: id.into(),
            lineage,
            generation,
        }
    }
}

/// One ordered transition in a supervisor lifecycle stream.
///
/// Every non-lag event carries a supervisor path relative to the handle that
/// created the watch. An empty path identifies the watched scope itself;
/// child variants at that path describe its direct children. One segment
/// identifies a direct nested supervisor, and so on.
///
/// Child lifecycle sequences are scoped to a stable supervisor identity and
/// continue across its incarnations, including recreation caused by an
/// ancestor restart.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum LifecycleEvent {
    /// The emitting supervisor incarnation started.
    SupervisorStarted {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
    },
    /// The emitting supervisor entered its shutdown sequence.
    SupervisorStopping {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
    },
    /// The emitting supervisor fully stopped.
    SupervisorStopped {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
    },
    /// The runtime installed a child membership for this supervisor incarnation.
    ///
    /// A pre-spawn snapshot can already project a statically configured child
    /// as `Starting` before this transition is emitted. Consumers combining a
    /// snapshot and stream should therefore apply `Added` as an idempotent
    /// upsert keyed by `(child_id, lineage)`, not as an unchecked row
    /// insertion.
    Added {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Monotonic causal sequence for the emitting supervisor identity.
        seq: u64,
        /// Direct child membership that transitioned.
        child_id: String,
        /// Identity of this child membership within its stable supervisor scope.
        lineage: u64,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
        /// Subject child's cumulative restart count at emission.
        child_restart_count: u64,
    },
    /// A child became running. For readiness-gated children this is emitted
    /// only after readiness is reported.
    Started {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Monotonic causal sequence for the emitting supervisor identity.
        seq: u64,
        /// Direct child membership that transitioned.
        child_id: String,
        /// Identity of this child membership within its stable supervisor scope.
        lineage: u64,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
        /// Subject child's cumulative restart count at emission.
        child_restart_count: u64,
        /// Generation that became running.
        generation: u64,
    },
    /// A child generation exited.
    Exited {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Monotonic causal sequence for the emitting supervisor identity.
        seq: u64,
        /// Direct child membership that transitioned.
        child_id: String,
        /// Identity of this child membership within its stable supervisor scope.
        lineage: u64,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
        /// Subject child's cumulative restart count at emission.
        child_restart_count: u64,
        /// Generation that exited.
        generation: u64,
        /// Public classification of the exit.
        reason: ExitStatusView,
        /// Whether the supervisor stopped this generation rather than letting
        /// it reach its own conclusion.
        ///
        /// A child cancelled by shutdown, removal, or a sibling-driven group
        /// restart can still return `Ok(())` and so be classified as
        /// [`ExitStatusView::Completed`]. Such an exit is not finished work,
        /// and consumers deciding whether finite work is done — as
        /// [`wait_completed`](crate::SupervisorHandle::wait_completed) does —
        /// must exclude it. A child that failed or panicked on its own is not
        /// cancelled.
        cancelled: bool,
    },
    /// A child membership ended.
    Removed {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Monotonic causal sequence for the emitting supervisor identity.
        seq: u64,
        /// Direct child membership that transitioned.
        child_id: String,
        /// Identity of this child membership within its stable supervisor scope.
        lineage: u64,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
        /// Subject child's cumulative restart count at emission.
        child_restart_count: u64,
    },
    /// A restart was scheduled after a backoff delay.
    RestartScheduled {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Child identifier.
        child_id: String,
        /// Identity of the child membership in the emitting scope.
        lineage: u64,
        /// Generation that exited and will be replaced.
        generation: u64,
        /// Time before the replacement is spawned.
        delay: Duration,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
        /// Subject child's cumulative restart count at emission.
        child_restart_count: u64,
    },
    /// The emitting scope exceeded its restart intensity and will stop.
    RestartIntensityExceeded {
        /// Path from the watched scope to the emitting scope.
        supervisor_path: Vec<LifecyclePathSegment>,
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
    },
    /// Older transitions were discarded because this watch fell behind.
    ///
    /// A watch owns one buffer for its complete filtered stream. This marker
    /// therefore invalidates edge-derived state for that stream; resynchronize
    /// from [`SupervisorSnapshot`](crate::SupervisorSnapshot).
    Lagged {
        /// Number of transitions discarded since the preceding delivered
        /// event.
        dropped: u64,
    },
}

impl LifecycleEvent {
    /// Returns the path from the watched scope to the emitting scope.
    ///
    /// A lag marker covers the watch's whole filtered stream and therefore
    /// has no single emitting path.
    pub fn supervisor_path(&self) -> Option<&[LifecyclePathSegment]> {
        match self {
            Self::SupervisorStarted { supervisor_path }
            | Self::SupervisorStopping { supervisor_path }
            | Self::SupervisorStopped { supervisor_path }
            | Self::Added {
                supervisor_path, ..
            }
            | Self::Started {
                supervisor_path, ..
            }
            | Self::Exited {
                supervisor_path, ..
            }
            | Self::Removed {
                supervisor_path, ..
            }
            | Self::RestartScheduled {
                supervisor_path, ..
            }
            | Self::RestartIntensityExceeded {
                supervisor_path, ..
            } => Some(supervisor_path),
            Self::Lagged { .. } => None,
        }
    }

    /// Returns the per-scope sequence of a child membership transition.
    pub fn seq(&self) -> Option<u64> {
        match self {
            Self::Added { seq, .. }
            | Self::Started { seq, .. }
            | Self::Exited { seq, .. }
            | Self::Removed { seq, .. } => Some(*seq),
            _ => None,
        }
    }

    /// Returns the child id for a child membership or scheduled-restart event.
    pub fn child_id(&self) -> Option<&str> {
        match self {
            Self::Added { child_id, .. }
            | Self::Started { child_id, .. }
            | Self::Exited { child_id, .. }
            | Self::Removed { child_id, .. }
            | Self::RestartScheduled { child_id, .. } => Some(child_id),
            _ => None,
        }
    }

    /// Returns the child membership lineage when the event identifies one.
    pub fn lineage(&self) -> Option<u64> {
        match self {
            Self::Added { lineage, .. }
            | Self::Started { lineage, .. }
            | Self::Exited { lineage, .. }
            | Self::Removed { lineage, .. }
            | Self::RestartScheduled { lineage, .. } => Some(*lineage),
            _ => None,
        }
    }

    /// Returns the emitting scope's cumulative restart count when present.
    pub fn total_restarts(&self) -> Option<u64> {
        match self {
            Self::Added { total_restarts, .. }
            | Self::Started { total_restarts, .. }
            | Self::Exited { total_restarts, .. }
            | Self::Removed { total_restarts, .. }
            | Self::RestartScheduled { total_restarts, .. }
            | Self::RestartIntensityExceeded { total_restarts, .. } => Some(*total_restarts),
            _ => None,
        }
    }

    /// Returns the subject child's cumulative restart count when present.
    pub fn child_restart_count(&self) -> Option<u64> {
        match self {
            Self::Added {
                child_restart_count,
                ..
            }
            | Self::Started {
                child_restart_count,
                ..
            }
            | Self::Exited {
                child_restart_count,
                ..
            }
            | Self::Removed {
                child_restart_count,
                ..
            }
            | Self::RestartScheduled {
                child_restart_count,
                ..
            } => Some(*child_restart_count),
            _ => None,
        }
    }
}

/// Ordered, reliable lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle`](crate::SupervisorHandle::watch_lifecycle).
///
/// Both direct and recursive watches use this type. A direct watch includes
/// child transitions and restart decisions emitted by the watched scope, with
/// an empty supervisor path. Supervisor incarnation markers and descendant
/// events are topology observations available only from a recursive watch.
/// Each watch has its own bounded buffer. Sustained overflow drops the oldest
/// transitions and replaces them with one accumulated [`LifecycleEvent::Lagged`]
/// marker; loss is never silent.
pub struct LifecycleWatch {
    queue: Arc<LifecycleEventQueue>,
    watcher_count: Option<Arc<AtomicUsize>>,
}

impl LifecycleWatch {
    fn new(queue: Arc<LifecycleEventQueue>, watcher_count: Option<Arc<AtomicUsize>>) -> Self {
        Self {
            queue,
            watcher_count,
        }
    }

    /// Returns the next staged lifecycle event.
    ///
    /// Returns `None` after all staged events have been delivered and the
    /// watched stable supervisor identity can never run again.
    pub async fn next(&mut self) -> Option<LifecycleEvent> {
        loop {
            let notified = self.queue.waiter();
            if let Some(event) = self.queue.pop() {
                return Some(event);
            }
            if self.queue.is_terminal() {
                return None;
            }
            notified.await;
        }
    }

    /// Waits for `child_id` in `supervisor_path` to start above
    /// `after_generation`.
    ///
    /// The path identifies one exact supervisor incarnation. Returns `None`
    /// when the requested start can no longer be observed: that membership or
    /// supervisor incarnation ended, the watched identity became terminal, or
    /// a stream-wide [`LifecycleEvent::Lagged`] marker discarded a prefix that
    /// may have contained it. Pass an empty path to wait in the watched scope
    /// itself.
    pub async fn started_after(
        &mut self,
        supervisor_path: &[LifecyclePathSegment],
        child_id: &str,
        after_generation: u64,
    ) -> Option<u64> {
        loop {
            let event = self.next().await?;
            match event {
                LifecycleEvent::Lagged { .. } => return None,
                LifecycleEvent::Started {
                    supervisor_path: event_path,
                    child_id: event_child_id,
                    generation,
                    ..
                } if event_path == supervisor_path
                    && event_child_id == child_id
                    && generation > after_generation =>
                {
                    return Some(generation);
                }
                LifecycleEvent::Removed {
                    supervisor_path: event_path,
                    child_id: event_child_id,
                    ..
                } if event_path == supervisor_path && event_child_id == child_id => return None,
                LifecycleEvent::SupervisorStopped {
                    supervisor_path: event_path,
                } if event_path == supervisor_path => return None,
                _ => {}
            }
        }
    }

    /// Waits until the watched stable supervisor identity becomes terminal
    /// without consuming staged events.
    pub async fn closed(&self) {
        loop {
            let notified = self.queue.waiter();
            if self.queue.is_terminal() {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for LifecycleWatch {
    fn drop(&mut self) {
        if let Some(watcher_count) = self.watcher_count.take() {
            let previous = watcher_count.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "recursive lifecycle watcher count underflow");
        }
    }
}

impl fmt::Debug for LifecycleWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LifecycleWatch")
            .field("terminal", &self.queue.is_terminal())
            .finish_non_exhaustive()
    }
}

pub(crate) struct LifecycleEventDraft {
    pub(crate) child_id: String,
    pub(crate) lineage: u64,
    pub(crate) total_restarts: u64,
    pub(crate) child_restart_count: u64,
    pub(crate) kind: ChildLifecycleEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChildLifecycleEvent {
    Added,
    Started {
        generation: u64,
    },
    Exited {
        generation: u64,
        reason: ExitStatusView,
        cancelled: bool,
    },
    Removed,
}

struct LifecycleEventQueue {
    events: Mutex<VecDeque<LifecycleEvent>>,
    notify: Notify,
    terminal: AtomicBool,
}

impl LifecycleEventQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            terminal: AtomicBool::new(false),
        })
    }

    fn events(&self) -> MutexGuard<'_, VecDeque<LifecycleEvent>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, event: LifecycleEvent) {
        {
            let mut events = self.events();
            while events.len() >= LIFECYCLE_BUFFER_CAPACITY {
                Self::record_drop(&mut events);
            }
            events.push_back(event);
        }
        self.notify.notify_one();
    }

    fn record_drop(events: &mut VecDeque<LifecycleEvent>) {
        if matches!(events.front(), Some(LifecycleEvent::Lagged { .. })) {
            if events.remove(1).is_some()
                && let Some(LifecycleEvent::Lagged { dropped }) = events.front_mut()
            {
                *dropped = dropped.saturating_add(1);
            }
        } else if events.pop_front().is_some() {
            events.push_front(LifecycleEvent::Lagged { dropped: 1 });
        }
    }

    fn pop(&self) -> Option<LifecycleEvent> {
        self.events().pop_front()
    }

    fn waiter(&self) -> Notified<'_> {
        self.notify.notified()
    }

    fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }
}

/// Restart-stable lifecycle broadcaster shared by every incarnation of one
/// supervisor identity.
pub(crate) struct LifecycleHub {
    seq: AtomicU64,
    next_lineage: AtomicU64,
    recursive_watcher_count: Arc<AtomicUsize>,
    state: Mutex<LifecycleHubState>,
}

struct LifecycleHubState {
    terminal: bool,
    watchers: Vec<Weak<LifecycleEventQueue>>,
    recursive_watchers: Vec<Weak<LifecycleEventQueue>>,
}

impl LifecycleHub {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicU64::new(0),
            next_lineage: AtomicU64::new(0),
            recursive_watcher_count: Arc::new(AtomicUsize::new(0)),
            state: Mutex::new(LifecycleHubState {
                terminal: false,
                watchers: Vec::new(),
                recursive_watchers: Vec::new(),
            }),
        })
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Mints a lineage scoped to this restart-stable supervisor
    /// identity. The counter intentionally continues across incarnations.
    pub(crate) fn next_lineage(&self) -> u64 {
        self.next_lineage
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .unwrap_or_else(|current| current)
    }

    /// Returns the lineage [`next_lineage`](Self::next_lineage)
    /// would mint next, without consuming it.
    pub(crate) fn peek_lineage(&self) -> u64 {
        self.next_lineage.load(Ordering::Acquire)
    }

    /// Advances the lineage allocator past a membership projected before this
    /// hub began minting lineages (the root supervisor's initial snapshot).
    pub(crate) fn observe_lineage(&self, lineage: u64) {
        let next = lineage.saturating_add(1);
        let _ = self
            .next_lineage
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < next).then_some(next)
            });
    }

    pub(crate) fn watch(&self) -> LifecycleWatch {
        let queue = LifecycleEventQueue::new();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.watchers.retain(|watcher| watcher.strong_count() > 0);
        if state.terminal {
            queue.mark_terminal();
        } else {
            state.watchers.push(Arc::downgrade(&queue));
        }
        LifecycleWatch::new(queue, None)
    }

    pub(crate) fn watch_recursive(&self) -> LifecycleWatch {
        let queue = LifecycleEventQueue::new();
        self.recursive_watcher_count.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .recursive_watchers
            .retain(|watcher| watcher.strong_count() > 0);
        if state.terminal {
            self.recursive_watcher_count.fetch_sub(1, Ordering::AcqRel);
            queue.mark_terminal();
            LifecycleWatch::new(queue, None)
        } else {
            state.recursive_watchers.push(Arc::downgrade(&queue));
            LifecycleWatch::new(queue, Some(Arc::clone(&self.recursive_watcher_count)))
        }
    }

    /// Assigns a sequence, publishes the aligned snapshot, then stages the
    /// event while registration is excluded by the same hub lock.
    pub(crate) fn emit(
        &self,
        draft: LifecycleEventDraft,
        publish_aligned_snapshot: impl FnOnce(),
    ) -> LifecycleEvent {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = self
            .seq
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .unwrap_or_else(|current| current)
            .saturating_add(1);
        let LifecycleEventDraft {
            child_id,
            lineage,
            total_restarts,
            child_restart_count,
            kind,
        } = draft;
        let event = match kind {
            ChildLifecycleEvent::Added => LifecycleEvent::Added {
                supervisor_path: Vec::new(),
                seq,
                child_id,
                lineage,
                total_restarts,
                child_restart_count,
            },
            ChildLifecycleEvent::Started { generation } => LifecycleEvent::Started {
                supervisor_path: Vec::new(),
                seq,
                child_id,
                lineage,
                total_restarts,
                child_restart_count,
                generation,
            },
            ChildLifecycleEvent::Exited {
                generation,
                reason,
                cancelled,
            } => LifecycleEvent::Exited {
                supervisor_path: Vec::new(),
                seq,
                child_id,
                lineage,
                total_restarts,
                child_restart_count,
                generation,
                reason,
                cancelled,
            },
            ChildLifecycleEvent::Removed => LifecycleEvent::Removed {
                supervisor_path: Vec::new(),
                seq,
                child_id,
                lineage,
                total_restarts,
                child_restart_count,
            },
        };
        if state.terminal {
            return event;
        }
        publish_aligned_snapshot();
        state.watchers.retain(|watcher| {
            let Some(queue) = watcher.upgrade() else {
                return false;
            };
            queue.push(event.clone());
            true
        });
        event
    }

    fn emit_direct(&self, event: &LifecycleEvent) {
        // `watch_lifecycle` remains a direct-child stream. The unified event
        // type adds the two child-restart decisions that previously existed
        // only on the recursive stream; supervisor incarnation markers remain
        // recursive-only.
        if !matches!(
            event,
            LifecycleEvent::RestartScheduled { .. }
                | LifecycleEvent::RestartIntensityExceeded { .. }
        ) {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.terminal {
            return;
        }
        state.watchers.retain(|watcher| {
            let Some(queue) = watcher.upgrade() else {
                return false;
            };
            queue.push(event.clone());
            true
        });
    }

    fn emit_recursive(&self, event: &LifecycleEvent) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.terminal {
            return;
        }
        state.recursive_watchers.retain(|watcher| {
            let Some(queue) = watcher.upgrade() else {
                return false;
            };
            queue.push(event.clone());
            true
        });
    }

    fn has_recursive_watchers(&self) -> bool {
        self.recursive_watcher_count.load(Ordering::Acquire) > 0
    }

    pub(crate) fn terminal(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.terminal {
            return;
        }
        state.terminal = true;
        for watcher in state.watchers.drain(..) {
            if let Some(queue) = watcher.upgrade() {
                queue.mark_terminal();
            }
        }
        for watcher in state.recursive_watchers.drain(..) {
            if let Some(queue) = watcher.upgrade() {
                queue.mark_terminal();
            }
        }
    }
}

/// One incarnation's route into every recursive watch rooted at or above it.
#[derive(Clone)]
pub(crate) struct LifecycleTreeSink(Arc<LifecycleTreeSinkInner>);

struct LifecycleTreeSinkInner {
    hub: Arc<LifecycleHub>,
    parent: Option<(LifecycleTreeSink, LifecyclePathSegment)>,
}

impl LifecycleTreeSink {
    pub(crate) fn root(hub: Arc<LifecycleHub>) -> Self {
        Self(Arc::new(LifecycleTreeSinkInner { hub, parent: None }))
    }

    pub(crate) fn nested(
        hub: Arc<LifecycleHub>,
        parent: Self,
        segment: LifecyclePathSegment,
    ) -> Self {
        Self(Arc::new(LifecycleTreeSinkInner {
            hub,
            parent: Some((parent, segment)),
        }))
    }

    /// Emits an event produced outside the sequenced direct-child path.
    pub(crate) fn emit(&self, event: LifecycleEvent) {
        self.0.hub.emit_direct(&event);
        self.forward_recursive(event);
    }

    /// Forwards a child event already staged for direct watchers by
    /// [`LifecycleHub::emit`].
    pub(crate) fn forward_child(&self, event: LifecycleEvent) {
        self.forward_recursive(event);
    }

    fn has_recursive_watchers_in_chain(&self) -> bool {
        self.0.hub.has_recursive_watchers()
            || self
                .0
                .parent
                .as_ref()
                .is_some_and(|(parent, _)| parent.has_recursive_watchers_in_chain())
    }

    fn forward_recursive(&self, event: LifecycleEvent) {
        if !self.has_recursive_watchers_in_chain() {
            return;
        }
        self.forward(event);
    }

    fn forward(&self, mut event: LifecycleEvent) {
        if self.0.hub.has_recursive_watchers() {
            self.0.hub.emit_recursive(&event);
        }
        if let Some((parent, segment)) = &self.0.parent
            && parent.has_recursive_watchers_in_chain()
        {
            prepend_path(&mut event, segment.clone());
            parent.forward(event);
        }
    }
}

fn prepend_path(event: &mut LifecycleEvent, segment: LifecyclePathSegment) {
    let supervisor_path = match event {
        LifecycleEvent::SupervisorStarted { supervisor_path }
        | LifecycleEvent::SupervisorStopping { supervisor_path }
        | LifecycleEvent::SupervisorStopped { supervisor_path }
        | LifecycleEvent::Added {
            supervisor_path, ..
        }
        | LifecycleEvent::Started {
            supervisor_path, ..
        }
        | LifecycleEvent::Exited {
            supervisor_path, ..
        }
        | LifecycleEvent::Removed {
            supervisor_path, ..
        }
        | LifecycleEvent::RestartScheduled {
            supervisor_path, ..
        }
        | LifecycleEvent::RestartIntensityExceeded {
            supervisor_path, ..
        } => supervisor_path,
        LifecycleEvent::Lagged { .. } => return,
    };
    supervisor_path.insert(0, segment);
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, PoisonError};

    use super::{
        ChildLifecycleEvent, LifecycleEvent, LifecycleEventDraft, LifecycleHub,
        LifecyclePathSegment, LifecycleTreeSink,
    };

    /// `emit` sweeps dropped watchers, but an identity that never emits would
    /// otherwise accumulate them: per-connection watches on a quiet tree are a
    /// normal pattern, so registration sweeps too.
    #[test]
    fn registering_a_watch_sweeps_dropped_watchers() {
        let hub = LifecycleHub::new();

        for _ in 0..8 {
            drop(hub.watch());
        }
        let _live = hub.watch();

        assert_eq!(
            hub.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .watchers
                .len(),
            1
        );
    }

    #[test]
    fn recursive_watcher_count_tracks_live_registrations() {
        let hub = LifecycleHub::new();

        assert!(!hub.has_recursive_watchers());
        let first = hub.watch_recursive();
        let second = hub.watch_recursive();
        assert!(hub.has_recursive_watchers());

        drop(first);
        assert!(hub.has_recursive_watchers());
        drop(second);
        assert!(!hub.has_recursive_watchers());
    }

    #[test]
    fn terminal_local_hub_does_not_gate_recursive_ancestor() {
        let parent_hub = LifecycleHub::new();
        let child_hub = LifecycleHub::new();
        let parent_sink = LifecycleTreeSink::root(Arc::clone(&parent_hub));
        let path = LifecyclePathSegment {
            id: "nested".to_owned(),
            lineage: 3,
            generation: 5,
        };
        let child_sink =
            LifecycleTreeSink::nested(Arc::clone(&child_hub), parent_sink, path.clone());
        let parent_watch = parent_hub.watch_recursive();

        child_hub.terminal();
        let event = child_hub.emit(
            LifecycleEventDraft {
                child_id: "worker".to_owned(),
                lineage: 8,
                total_restarts: 13,
                child_restart_count: 2,
                kind: ChildLifecycleEvent::Started { generation: 1 },
            },
            || panic!("terminal hubs must not publish another local snapshot"),
        );
        child_sink.forward_child(event);

        let forwarded = parent_watch
            .queue
            .pop()
            .expect("ancestor receives the trailing child event");
        assert!(matches!(
            forwarded,
            LifecycleEvent::Started {
                supervisor_path,
                child_id,
                lineage: 8,
                total_restarts: 13,
                child_restart_count: 2,
                generation: 1,
                ..
            } if supervisor_path == vec![path] && child_id == "worker"
        ));
    }
}
