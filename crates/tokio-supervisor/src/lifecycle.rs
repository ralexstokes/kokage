use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
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
    pub membership_epoch: u64,
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
    /// upsert keyed by `(child_id, membership_epoch)`, not as an unchecked row
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

/// Ordered, reliable lifecycle stream created by
/// [`SupervisorHandle::watch_lifecycle`](crate::SupervisorHandle::watch_lifecycle).
///
/// Each watch has its own bounded buffer. Sustained overflow drops the oldest
/// transitions and replaces them with one accumulated
/// [`LifecycleEventKind::Lagged`] marker; loss is never silent.
pub struct LifecycleWatch {
    queue: Arc<LifecycleQueue>,
}

impl LifecycleWatch {
    fn new(queue: Arc<LifecycleQueue>) -> Self {
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
    pub async fn wait_started(&mut self, child_id: &str, after_generation: u64) -> Option<u64> {
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
    pub(crate) membership_epoch: u64,
    pub(crate) total_restarts: u64,
    pub(crate) child_restart_count: u64,
    pub(crate) kind: LifecycleEventKind,
}

struct LifecycleQueue {
    events: Mutex<VecDeque<LifecycleEvent>>,
    notify: Notify,
    terminal: AtomicBool,
}

impl LifecycleQueue {
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
        if matches!(
            events.front().map(|event| &event.kind),
            Some(LifecycleEventKind::Lagged { .. })
        ) {
            let newest_dropped = events.remove(1);
            if let Some(LifecycleEvent {
                seq,
                child_id,
                membership_epoch,
                total_restarts,
                child_restart_count,
                kind: LifecycleEventKind::Lagged { dropped },
            }) = events.front_mut()
            {
                if let Some(newest_dropped) = newest_dropped {
                    *seq = newest_dropped.seq;
                    *child_id = newest_dropped.child_id;
                    *membership_epoch = newest_dropped.membership_epoch;
                    *total_restarts = newest_dropped.total_restarts;
                    *child_restart_count = newest_dropped.child_restart_count;
                }
                *dropped = dropped.saturating_add(1);
            }
        } else if let Some(mut dropped_event) = events.pop_front() {
            dropped_event.kind = LifecycleEventKind::Lagged { dropped: 1 };
            events.push_front(dropped_event);
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
    next_membership_epoch: AtomicU64,
    state: Mutex<LifecycleHubState>,
}

struct LifecycleHubState {
    terminal: bool,
    watchers: Vec<Weak<LifecycleQueue>>,
}

impl LifecycleHub {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicU64::new(0),
            next_membership_epoch: AtomicU64::new(0),
            state: Mutex::new(LifecycleHubState {
                terminal: false,
                watchers: Vec::new(),
            }),
        })
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Mints a membership epoch scoped to this restart-stable supervisor
    /// identity. The counter intentionally continues across incarnations.
    pub(crate) fn next_membership_epoch(&self) -> u64 {
        self.next_membership_epoch
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .unwrap_or_else(|current| current)
    }

    /// Advances the epoch allocator past a membership projected before this
    /// hub began minting epochs (the root supervisor's initial snapshot).
    pub(crate) fn observe_membership_epoch(&self, membership_epoch: u64) {
        let next = membership_epoch.saturating_add(1);
        let _ =
            self.next_membership_epoch
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

    /// Assigns a sequence, publishes the aligned snapshot, then stages the
    /// event while registration is excluded by the same hub lock.
    pub(crate) fn emit(&self, draft: LifecycleEventDraft, publish_aligned_snapshot: impl FnOnce()) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.terminal {
            return;
        }
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
            membership_epoch: draft.membership_epoch,
            total_restarts: draft.total_restarts,
            child_restart_count: draft.child_restart_count,
            kind: draft.kind,
        };
        publish_aligned_snapshot();
        state.watchers.retain(|watcher| {
            let Some(queue) = watcher.upgrade() else {
                return false;
            };
            queue.push(event.clone());
            true
        });
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
    }
}

#[cfg(test)]
mod tests {
    use super::LifecycleHub;

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
                .expect("hub state is not poisoned")
                .watchers
                .len(),
            1
        );
    }
}
