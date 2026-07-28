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

/// One ordered transition among a supervisor's direct child memberships.
///
/// Events are scoped to the stable identity represented by the handle that
/// created the watch. The sequence is continuous across incarnations of a
/// nested supervisor, including recreations caused by an ancestor restart.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct LifecycleEvent {
    /// Monotonic causal sequence for this stable supervisor identity.
    pub seq: u64,
    /// Direct child membership that transitioned.
    pub child_id: String,
    /// Identity of this child membership within its stable supervisor scope.
    pub lineage: u64,
    /// Stable-scope cumulative restart count at emission.
    pub total_restarts: u64,
    /// Subject child's cumulative restart count at emission.
    pub child_restart_count: u64,
    /// The transition that occurred.
    pub kind: LifecycleEventKind,
}

/// A transition in one direct child membership.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum LifecycleEventKind {
    /// The runtime installed the membership for this supervisor incarnation.
    ///
    /// A pre-spawn snapshot can already project a statically configured child
    /// as `Starting` before this transition is emitted. Consumers combining a
    /// snapshot and stream should therefore apply `Added` as an idempotent
    /// upsert keyed by `(child_id, lineage)`, not as an unchecked row
    /// insertion.
    Added,
    /// The child became running. For readiness-gated children this is emitted
    /// only after readiness is reported.
    Started {
        /// Generation that became running.
        generation: u64,
    },
    /// A child generation exited.
    Exited {
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
    /// The membership ended.
    Removed,
    /// Older transitions were discarded because this watch could not keep up.
    ///
    /// Consumers that maintain edge-derived state must resynchronize from a
    /// snapshot. This marker carries the envelope of the newest discarded
    /// transition, so its sequence and cumulative counters describe the full
    /// dropped prefix.
    Lagged {
        /// Number of transitions discarded since the preceding delivered
        /// event.
        dropped: u64,
    },
}

/// One nested-supervisor edge in a recursive lifecycle event's path.
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
    pub fn new(id: impl Into<String>, lineage: u64, generation: u64) -> Self {
        Self {
            id: id.into(),
            lineage,
            generation,
        }
    }
}

/// One event in a supervisor tree's recursive lifecycle stream.
///
/// `supervisor_path` is relative to the handle that created the watch. It is
/// empty for events emitted by that scope, contains one segment for a direct
/// nested supervisor, and so on.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct RecursiveLifecycleEvent {
    /// Nested-supervisor path from the watched scope to the emitting scope.
    pub supervisor_path: Vec<LifecyclePathSegment>,
    /// Transition that occurred in the emitting scope.
    pub kind: RecursiveLifecycleEventKind,
}

impl RecursiveLifecycleEvent {
    /// Creates a recursive event at `supervisor_path`.
    pub fn new(
        supervisor_path: Vec<LifecyclePathSegment>,
        kind: RecursiveLifecycleEventKind,
    ) -> Self {
        Self {
            supervisor_path,
            kind,
        }
    }

    fn local(kind: RecursiveLifecycleEventKind) -> Self {
        Self::new(Vec::new(), kind)
    }
}

/// A transition visible in a recursive supervisor lifecycle stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum RecursiveLifecycleEventKind {
    /// The emitting supervisor incarnation started.
    SupervisorStarted,
    /// The emitting supervisor entered its shutdown sequence.
    SupervisorStopping,
    /// The emitting supervisor fully stopped.
    SupervisorStopped,
    /// A direct-child lifecycle transition in the emitting scope.
    Child(LifecycleEvent),
    /// A restart was scheduled after a backoff delay.
    RestartScheduled {
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
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
    },
    /// Older tree transitions were discarded because this watch fell behind.
    ///
    /// A recursive watch owns one buffer for the whole watched tree. This
    /// marker therefore invalidates edge-derived state for the whole tree;
    /// resynchronize from the recursive [`SupervisorSnapshot`](crate::SupervisorSnapshot).
    /// The marker event's `supervisor_path` and `newest_dropped` kind preserve
    /// the complete envelope of the newest discarded transition. Consumers
    /// can therefore recover its per-scope sequence and cumulative counters
    /// when it was a child or restart event, even though the gap still
    /// invalidates derived state for the whole tree.
    Lagged {
        /// Number of tree transitions discarded since the preceding delivered
        /// event.
        dropped: u64,
        /// Kind of the newest discarded transition. Its supervisor path is
        /// carried by the surrounding [`RecursiveLifecycleEvent`].
        newest_dropped: Box<RecursiveLifecycleEventKind>,
    },
}

/// Ordered, reliable lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle`](crate::SupervisorHandle::watch_lifecycle).
///
/// Each watch has its own bounded buffer. Sustained overflow drops the oldest
/// transitions and replaces them with one accumulated
/// [`LifecycleEventKind::Lagged`] marker; loss is never silent.
pub struct LifecycleWatch {
    queue: Arc<DirectLifecycleQueue>,
}

/// Ordered recursive lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle_recursive`](crate::SupervisorHandle::watch_lifecycle_recursive).
///
/// Each watch has one bounded buffer for the entire watched tree. Events from
/// every scope retain their per-scope [`LifecycleEvent::seq`] inside
/// [`RecursiveLifecycleEventKind::Child`]. Sustained overflow replaces the
/// oldest events with one tree-wide [`RecursiveLifecycleEventKind::Lagged`]
/// marker; loss is never silent.
pub struct RecursiveLifecycleWatch {
    queue: Arc<RecursiveLifecycleQueue>,
    watcher_count: Option<Arc<AtomicUsize>>,
}

impl RecursiveLifecycleWatch {
    fn new(queue: Arc<RecursiveLifecycleQueue>, watcher_count: Option<Arc<AtomicUsize>>) -> Self {
        Self {
            queue,
            watcher_count,
        }
    }

    /// Returns the next staged tree event.
    ///
    /// Returns `None` after all staged events have been delivered and the
    /// watched stable supervisor identity can never run again.
    pub async fn next(&mut self) -> Option<RecursiveLifecycleEvent> {
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
    /// supervisor incarnation ended, the watched tree became terminal, or a
    /// tree-wide [`RecursiveLifecycleEventKind::Lagged`] marker discarded a
    /// prefix that may have contained it. Pass an empty path to wait in the
    /// watched scope itself.
    pub async fn started_after(
        &mut self,
        supervisor_path: &[LifecyclePathSegment],
        child_id: &str,
        after_generation: u64,
    ) -> Option<u64> {
        loop {
            let event = self.next().await?;
            if matches!(&event.kind, RecursiveLifecycleEventKind::Lagged { .. }) {
                return None;
            }
            if event.supervisor_path != supervisor_path {
                continue;
            }
            match event.kind {
                RecursiveLifecycleEventKind::Child(event) if event.child_id == child_id => {
                    match event.kind {
                        LifecycleEventKind::Started { generation }
                            if generation > after_generation =>
                        {
                            return Some(generation);
                        }
                        LifecycleEventKind::Removed => return None,
                        _ => {}
                    }
                }
                RecursiveLifecycleEventKind::SupervisorStopped => return None,
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

impl Drop for RecursiveLifecycleWatch {
    fn drop(&mut self) {
        if let Some(watcher_count) = self.watcher_count.take() {
            let previous = watcher_count.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "recursive lifecycle watcher count underflow");
        }
    }
}

impl fmt::Debug for RecursiveLifecycleWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecursiveLifecycleWatch")
            .field("terminal", &self.queue.is_terminal())
            .finish_non_exhaustive()
    }
}

impl LifecycleWatch {
    fn new(queue: Arc<DirectLifecycleQueue>) -> Self {
        Self { queue }
    }

    /// Returns the next staged event.
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

    /// Waits for `child_id` to start at a generation above `after_generation`.
    ///
    /// Returns `None` once that start can no longer be observed on this watch:
    /// the membership was removed, the watched supervisor identity became
    /// terminal, or a [`LifecycleEventKind::Lagged`] marker discarded a prefix
    /// that may have contained it. This is a convenience for one-shot restart
    /// waits — it reports that waiting longer is futile, not which of those
    /// happened. Callers that must distinguish them, or that need to resume
    /// waiting after lag, should realign from
    /// [`snapshot`](crate::SupervisorHandle::snapshot).
    pub async fn started_after(&mut self, child_id: &str, after_generation: u64) -> Option<u64> {
        loop {
            let event = self.next().await?;
            // Checked before the child filter: a marker's envelope describes
            // the newest discarded transition, which need not belong to
            // `child_id` even when the dropped prefix contained its `Started`.
            if matches!(event.kind, LifecycleEventKind::Lagged { .. }) {
                return None;
            }
            if event.child_id != child_id {
                continue;
            }
            match event.kind {
                LifecycleEventKind::Started { generation } if generation > after_generation => {
                    return Some(generation);
                }
                LifecycleEventKind::Removed => return None,
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
    pub(crate) kind: LifecycleEventKind,
}

type DirectLifecycleQueue = LifecycleQueue<LifecycleEvent>;
type RecursiveLifecycleQueue = LifecycleQueue<RecursiveLifecycleEvent>;

struct LifecycleQueue<T> {
    events: Mutex<VecDeque<T>>,
    notify: Notify,
    terminal: AtomicBool,
}

trait Laggable: Sized {
    fn is_lagged(&self) -> bool;
    fn into_lagged(self, dropped: u64) -> Self;
    fn accumulate_lagged(&mut self, newest_dropped: Self);
}

impl Laggable for LifecycleEvent {
    fn is_lagged(&self) -> bool {
        matches!(self.kind, LifecycleEventKind::Lagged { .. })
    }

    fn into_lagged(mut self, dropped: u64) -> Self {
        self.kind = LifecycleEventKind::Lagged { dropped };
        self
    }

    fn accumulate_lagged(&mut self, newest_dropped: Self) {
        let LifecycleEventKind::Lagged { dropped } = &self.kind else {
            return;
        };
        *self = newest_dropped.into_lagged(dropped.saturating_add(1));
    }
}

impl Laggable for RecursiveLifecycleEvent {
    fn is_lagged(&self) -> bool {
        matches!(self.kind, RecursiveLifecycleEventKind::Lagged { .. })
    }

    fn into_lagged(self, dropped: u64) -> Self {
        Self {
            supervisor_path: self.supervisor_path,
            kind: RecursiveLifecycleEventKind::Lagged {
                dropped,
                newest_dropped: Box::new(self.kind),
            },
        }
    }

    fn accumulate_lagged(&mut self, newest_dropped: Self) {
        let RecursiveLifecycleEventKind::Lagged { dropped, .. } = &self.kind else {
            return;
        };
        *self = newest_dropped.into_lagged(dropped.saturating_add(1));
    }
}

impl<T: Laggable> LifecycleQueue<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            terminal: AtomicBool::new(false),
        })
    }

    fn events(&self) -> MutexGuard<'_, VecDeque<T>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, event: T) {
        {
            let mut events = self.events();
            while events.len() >= LIFECYCLE_BUFFER_CAPACITY {
                Self::record_drop(&mut events);
            }
            events.push_back(event);
        }
        self.notify.notify_one();
    }

    fn record_drop(events: &mut VecDeque<T>) {
        if events.front().is_some_and(Laggable::is_lagged) {
            if let Some(newest_dropped) = events.remove(1)
                && let Some(marker) = events.front_mut()
            {
                marker.accumulate_lagged(newest_dropped);
            }
        } else if let Some(dropped_event) = events.pop_front() {
            events.push_front(dropped_event.into_lagged(1));
        }
    }

    fn pop(&self) -> Option<T> {
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
    watchers: Vec<Weak<DirectLifecycleQueue>>,
    recursive_watchers: Vec<Weak<RecursiveLifecycleQueue>>,
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
        let queue = LifecycleQueue::new();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.watchers.retain(|watcher| watcher.strong_count() > 0);
        if state.terminal {
            queue.mark_terminal();
        } else {
            state.watchers.push(Arc::downgrade(&queue));
        }
        LifecycleWatch::new(queue)
    }

    pub(crate) fn watch_recursive(&self) -> RecursiveLifecycleWatch {
        let queue = RecursiveLifecycleQueue::new();
        self.recursive_watcher_count.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .recursive_watchers
            .retain(|watcher| watcher.strong_count() > 0);
        if state.terminal {
            self.recursive_watcher_count.fetch_sub(1, Ordering::AcqRel);
            queue.mark_terminal();
            RecursiveLifecycleWatch::new(queue, None)
        } else {
            state.recursive_watchers.push(Arc::downgrade(&queue));
            RecursiveLifecycleWatch::new(queue, Some(Arc::clone(&self.recursive_watcher_count)))
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
        let event = LifecycleEvent {
            seq,
            child_id: draft.child_id,
            lineage: draft.lineage,
            total_restarts: draft.total_restarts,
            child_restart_count: draft.child_restart_count,
            kind: draft.kind,
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

    fn emit_recursive(&self, event: &RecursiveLifecycleEvent) {
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

    pub(crate) fn emit(&self, kind: RecursiveLifecycleEventKind) {
        if !self.has_recursive_watchers_in_chain() {
            return;
        }
        self.forward(RecursiveLifecycleEvent::local(kind));
    }

    fn has_recursive_watchers_in_chain(&self) -> bool {
        self.0.hub.has_recursive_watchers()
            || self
                .0
                .parent
                .as_ref()
                .is_some_and(|(parent, _)| parent.has_recursive_watchers_in_chain())
    }

    fn forward(&self, mut event: RecursiveLifecycleEvent) {
        if self.0.hub.has_recursive_watchers() {
            self.0.hub.emit_recursive(&event);
        }
        if let Some((parent, segment)) = &self.0.parent
            && parent.has_recursive_watchers_in_chain()
        {
            event.supervisor_path.insert(0, segment.clone());
            parent.forward(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, PoisonError};

    use super::{
        LifecycleEventDraft, LifecycleEventKind, LifecycleHub, LifecyclePathSegment,
        LifecycleTreeSink, RecursiveLifecycleEventKind,
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
        let path = LifecyclePathSegment::new("nested", 3, 5);
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
                kind: LifecycleEventKind::Started { generation: 1 },
            },
            || panic!("terminal hubs must not publish another local snapshot"),
        );
        child_sink.emit(RecursiveLifecycleEventKind::Child(event));

        let forwarded = parent_watch
            .queue
            .pop()
            .expect("ancestor receives the trailing child event");
        assert_eq!(forwarded.supervisor_path, vec![path]);
        assert!(matches!(
            forwarded.kind,
            RecursiveLifecycleEventKind::Child(event)
                if event.child_id == "worker"
                    && event.lineage == 8
                    && event.total_restarts == 13
                    && event.child_restart_count == 2
                    && event.kind == LifecycleEventKind::Started { generation: 1 }
        ));
    }
}
