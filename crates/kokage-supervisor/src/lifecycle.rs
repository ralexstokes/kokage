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
    #[allow(dead_code)]
    pub(crate) fn new(id: impl Into<String>, lineage: u64, generation: u64) -> Self {
        Self {
            id: id.into(),
            lineage,
            generation,
        }
    }
}

/// One ordered transition among a supervisor's direct child memberships.
///
/// The sequence is scoped to the stable supervisor identity represented by
/// the handle that created the watch and continues across its incarnations.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ChildLifecycleEvent {
    /// Monotonic causal sequence for the emitting supervisor identity.
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
    pub kind: ChildLifecycleEventKind,
}

impl ChildLifecycleEvent {
    /// Creates a direct-child lifecycle event with total identity and counter fields.
    #[allow(dead_code)]
    pub(crate) fn new(
        seq: u64,
        child_id: impl Into<String>,
        lineage: u64,
        total_restarts: u64,
        child_restart_count: u64,
        kind: ChildLifecycleEventKind,
    ) -> Self {
        Self {
            seq,
            child_id: child_id.into(),
            lineage,
            total_restarts,
            child_restart_count,
            kind,
        }
    }
}

/// A transition in one direct child membership.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildLifecycleEventKind {
    /// The runtime installed the membership for this supervisor incarnation.
    Added,
    /// The child became running. Readiness-gated children emit this only after
    /// reporting readiness.
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
        cancelled: bool,
    },
    /// The membership ended.
    Removed,
    /// A restart was scheduled after a backoff delay.
    RestartScheduled {
        /// Generation that exited and will be replaced.
        generation: u64,
        /// Time before the replacement is spawned.
        delay: Duration,
    },
    /// Older transitions were discarded because this watch fell behind.
    ///
    /// The surrounding event retains the newest discarded transition's
    /// identity, sequence, and counters so those fields remain total.
    Lagged {
        /// Number of transitions discarded since the preceding delivered
        /// event.
        dropped: u64,
    },
}

/// One event in a supervisor tree's recursive lifecycle stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct LifecycleEvent {
    /// Path from the watched scope to the emitting scope.
    pub supervisor_path: Vec<LifecyclePathSegment>,
    /// Transition that occurred in the emitting scope.
    pub kind: LifecycleEventKind,
}

impl LifecycleEvent {
    /// Creates a recursive lifecycle envelope at `supervisor_path`.
    pub(crate) fn new(
        supervisor_path: Vec<LifecyclePathSegment>,
        kind: LifecycleEventKind,
    ) -> Self {
        Self {
            supervisor_path,
            kind,
        }
    }

    pub(crate) fn local(kind: LifecycleEventKind) -> Self {
        Self::new(Vec::new(), kind)
    }
}

/// A transition visible in a recursive supervisor lifecycle stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum LifecycleEventKind {
    /// A direct-child transition in the emitting scope.
    Child(ChildLifecycleEvent),
    /// A transition of the emitting supervisor incarnation.
    Supervisor(SupervisorLifecycleEvent),
    /// The emitting scope exceeded its restart intensity and will stop.
    RestartIntensityExceeded {
        /// Stable-scope cumulative restart count at emission.
        total_restarts: u64,
    },
    /// Older tree transitions were discarded because this watch fell behind.
    Lagged {
        /// Number of transitions discarded since the preceding delivered
        /// event.
        dropped: u64,
    },
}

/// A supervisor-incarnation transition in a recursive lifecycle stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum SupervisorLifecycleEvent {
    /// The supervisor incarnation started.
    Started,
    /// The supervisor entered its shutdown sequence.
    Stopping,
    /// The supervisor fully stopped.
    Stopped,
}

/// Direct-child lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle`](crate::SupervisorHandle::watch_lifecycle).
pub struct ChildLifecycleWatch {
    queue: Arc<DirectLifecycleQueue>,
}

impl ChildLifecycleWatch {
    fn new(queue: Arc<DirectLifecycleQueue>) -> Self {
        Self { queue }
    }

    /// Returns the next staged child event, or `None` after the stable
    /// supervisor identity becomes terminal and staged events are drained.
    pub async fn next(&mut self) -> Option<ChildLifecycleEvent> {
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

    /// Waits for `child_id` to start above `after_generation`.
    pub async fn started_after(&mut self, child_id: &str, after_generation: u64) -> Option<u64> {
        loop {
            let event = self.next().await?;
            if matches!(event.kind, ChildLifecycleEventKind::Lagged { .. }) {
                return None;
            }
            if event.child_id != child_id {
                continue;
            }
            match event.kind {
                ChildLifecycleEventKind::Started { generation }
                    if generation > after_generation =>
                {
                    return Some(generation);
                }
                ChildLifecycleEventKind::Removed => return None,
                _ => {}
            }
        }
    }
}

impl fmt::Debug for ChildLifecycleWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildLifecycleWatch")
            .field("terminal", &self.queue.is_terminal())
            .finish_non_exhaustive()
    }
}

/// Recursive lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle_recursive`](crate::SupervisorHandle::watch_lifecycle_recursive).
pub struct LifecycleWatch {
    queue: Arc<RecursiveLifecycleQueue>,
    watcher_count: Option<Arc<AtomicUsize>>,
}

impl LifecycleWatch {
    fn new(queue: Arc<RecursiveLifecycleQueue>, watcher_count: Option<Arc<AtomicUsize>>) -> Self {
        Self {
            queue,
            watcher_count,
        }
    }

    /// Returns the next staged tree event, or `None` after the watched stable
    /// supervisor identity becomes terminal and staged events are drained.
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
    pub async fn started_after(
        &mut self,
        supervisor_path: &[LifecyclePathSegment],
        child_id: &str,
        after_generation: u64,
    ) -> Option<u64> {
        loop {
            let event = self.next().await?;
            if matches!(event.kind, LifecycleEventKind::Lagged { .. }) {
                return None;
            }
            if event.supervisor_path != supervisor_path {
                continue;
            }
            match event.kind {
                LifecycleEventKind::Child(child) if child.child_id == child_id => {
                    match child.kind {
                        ChildLifecycleEventKind::Started { generation }
                            if generation > after_generation =>
                        {
                            return Some(generation);
                        }
                        ChildLifecycleEventKind::Removed => return None,
                        _ => {}
                    }
                }
                LifecycleEventKind::Supervisor(SupervisorLifecycleEvent::Stopped) => return None,
                _ => {}
            }
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
    pub(crate) kind: ChildLifecycleEventKind,
}

type DirectLifecycleQueue = LifecycleEventQueue<ChildLifecycleEvent>;
type RecursiveLifecycleQueue = LifecycleEventQueue<LifecycleEvent>;

struct LifecycleEventQueue<T> {
    events: Mutex<VecDeque<T>>,
    notify: Notify,
    terminal: AtomicBool,
}

trait Laggable: Sized {
    fn is_lagged(&self) -> bool;
    fn into_lagged(self, dropped: u64) -> Self;
    fn accumulate_lagged(&mut self, newest_dropped: Self);
}

impl Laggable for ChildLifecycleEvent {
    fn is_lagged(&self) -> bool {
        matches!(self.kind, ChildLifecycleEventKind::Lagged { .. })
    }

    fn into_lagged(mut self, dropped: u64) -> Self {
        self.kind = ChildLifecycleEventKind::Lagged { dropped };
        self
    }

    fn accumulate_lagged(&mut self, newest_dropped: Self) {
        let ChildLifecycleEventKind::Lagged { dropped } = self.kind else {
            return;
        };
        *self = newest_dropped.into_lagged(dropped.saturating_add(1));
    }
}

impl Laggable for LifecycleEvent {
    fn is_lagged(&self) -> bool {
        matches!(self.kind, LifecycleEventKind::Lagged { .. })
    }

    fn into_lagged(self, dropped: u64) -> Self {
        Self::new(Vec::new(), LifecycleEventKind::Lagged { dropped })
    }

    fn accumulate_lagged(&mut self, newest_dropped: Self) {
        let LifecycleEventKind::Lagged { dropped } = self.kind else {
            return;
        };
        *self = newest_dropped.into_lagged(dropped.saturating_add(1));
    }
}

impl<T: Laggable> LifecycleEventQueue<T> {
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
                && let Some(lagged) = events.front_mut()
            {
                lagged.accumulate_lagged(newest_dropped);
            }
        } else if let Some(dropped) = events.pop_front() {
            events.push_front(dropped.into_lagged(1));
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

    pub(crate) fn watch(&self) -> ChildLifecycleWatch {
        let queue = LifecycleEventQueue::new();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.watchers.retain(|watcher| watcher.strong_count() > 0);
        if state.terminal {
            queue.mark_terminal();
        } else {
            state.watchers.push(Arc::downgrade(&queue));
        }
        ChildLifecycleWatch::new(queue)
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
    ) -> ChildLifecycleEvent {
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
        let event = ChildLifecycleEvent {
            seq,
            child_id,
            lineage,
            total_restarts,
            child_restart_count,
            kind,
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
        self.forward_recursive(event);
    }

    /// Forwards a child event already staged for direct watchers by
    /// [`LifecycleHub::emit`].
    pub(crate) fn forward_child(&self, event: ChildLifecycleEvent) {
        self.forward_recursive(LifecycleEvent::local(LifecycleEventKind::Child(event)));
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
    event.supervisor_path.insert(0, segment);
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, PoisonError};

    use super::{
        ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleEvent, LifecycleEventDraft,
        LifecycleEventKind, LifecycleHub, LifecyclePathSegment, LifecycleTreeSink,
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
    fn recursive_lagged_marker_is_tree_wide_and_pathless() {
        let queue = super::LifecycleEventQueue::new();
        let nested = LifecyclePathSegment::new("nested", 3, 5);

        for _ in 0..=super::LIFECYCLE_BUFFER_CAPACITY {
            queue.push(LifecycleEvent::new(
                vec![nested.clone()],
                LifecycleEventKind::Supervisor(super::SupervisorLifecycleEvent::Started),
            ));
        }

        let lagged = queue.pop().expect("overflow stages a lagged marker");
        assert!(lagged.supervisor_path.is_empty());
        assert!(matches!(
            lagged.kind,
            LifecycleEventKind::Lagged { dropped: 2 }
        ));
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
                kind: ChildLifecycleEventKind::Started { generation: 1 },
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
            LifecycleEvent {
                supervisor_path,
                kind: LifecycleEventKind::Child(ChildLifecycleEvent {
                    child_id,
                    lineage: 8,
                    total_restarts: 13,
                    child_restart_count: 2,
                    kind: ChildLifecycleEventKind::Started { generation: 1 },
                    ..
                }),
            } if supervisor_path == vec![path] && child_id == "worker"
        ));
    }
}
