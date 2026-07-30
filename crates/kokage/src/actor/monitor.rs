use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::supervisor::CancellationToken;
use tokio::sync::{Notify, futures::Notified};

/// Maximum number of undelivered events staged for a single watch.
///
/// Lifecycle events are rare in normal operation (one per restart cycle), so
/// this bound is only reached when an observer's mailbox stays full while its
/// target restarts in a tight loop. Beyond the bound the oldest staged event
/// is dropped, which coalesces a restart storm into recent history plus the
/// current state; the terminal [`MonitorEvent::Removed`] is always the
/// newest event, so it is never dropped. This caps the memory a stalled
/// observer can pin regardless of how fast its target churns.
const WATCH_BUFFER_CAP: usize = 128;
// Overflow needs room for both a `Lagged` marker and a retained real event.
const _: () = assert!(WATCH_BUFFER_CAP >= 2);

/// Shared completion signal for one actor watch.
#[derive(Clone)]
pub(crate) struct Finished(CancellationToken);

impl Finished {
    fn new() -> Self {
        Self(CancellationToken::new())
    }

    pub(crate) fn signal(&self) {
        self.0.cancel();
    }

    fn is_signalled(&self) -> bool {
        self.0.is_cancelled()
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.0.clone()
    }
}

/// The reason carried by a [`MonitorEvent::Exited`] notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExitReason {
    /// The actor stopped cleanly or as part of an orderly shutdown.
    Normal,
    /// The actor failed, panicked, or was aborted.
    Failure,
}

/// Lifecycle transition of a watched logical actor.
///
/// Delivered by [`RawContext::watch`](crate::host::RawContext::watch). Events
/// for one watch arrive in lifecycle order: every [`Started`](Self::Started)
/// for a generation precedes its [`Exited`](Self::Exited), and
/// [`Removed`](Self::Removed) is final.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MonitorEvent {
    /// An incarnation of the watched actor is running.
    Started {
        /// Stable id of the watched actor.
        actor_id: String,
        /// Incarnation counter, starting at zero and increasing on every
        /// restart.
        generation: u64,
    },
    /// The current incarnation exited. If the supervisor restarts the actor,
    /// a matching [`Started`](Self::Started) follows.
    Exited {
        /// Stable id of the watched actor.
        actor_id: String,
        /// Incarnation counter, starting at zero and increasing on every
        /// restart.
        generation: u64,
        /// How the watched incarnation exited.
        reason: ExitReason,
    },
    /// One or more transitions were dropped because the observer could not
    /// keep up (its mailbox stayed full while the target churned), and the
    /// per-watch buffer overflowed.
    ///
    /// This is a resynchronization point, not an edge: the events immediately
    /// before it are gone, so a consumer that reacts to individual
    /// `Started`/`Exited` transitions should treat the following events as the
    /// target's current state rather than assuming strict `Started`/`Exited`
    /// alternation. Emitted only under sustained overload; a healthy observer
    /// never sees it.
    Lagged {
        /// Stable id of the watched actor.
        actor_id: String,
        /// Number of transitions dropped since the last delivered event.
        dropped: u64,
    },
    /// The actor is permanently gone. No further events will be delivered.
    Removed {
        /// Stable id of the watched actor.
        actor_id: String,
        /// The last incarnation that ran, or `None` if the actor never
        /// started.
        generation: Option<u64>,
    },
}

/// Bounded, drop-oldest staging buffer between a target's [`MonitorHub`] and
/// one observer's forwarder task.
///
/// The hub pushes events without awaiting (it holds its own lock); the
/// forwarder drains them and applies the observer's mailbox backpressure. The
/// bound lives here rather than in the mailbox because the hub cannot block on
/// a full mailbox, so an unbounded hand-off would let a churning target pin
/// arbitrary memory behind a stalled observer.
pub(crate) struct WatchQueue {
    actor_id: String,
    events: Mutex<VecDeque<MonitorEvent>>,
    notify: Notify,
    closed: AtomicBool,
}

impl WatchQueue {
    fn new(actor_id: &str) -> Arc<Self> {
        Arc::new(Self {
            actor_id: actor_id.to_owned(),
            events: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    fn events(&self) -> MutexGuard<'_, VecDeque<MonitorEvent>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stages one event. When the buffer is full the oldest real events are
    /// dropped to make room, and the loss is recorded in a single
    /// [`MonitorEvent::Lagged`] marker kept at the front, so overflow is
    /// signalled rather than silent. The terminal event is always the newest,
    /// so overflow never drops it.
    fn push(&self, event: MonitorEvent) {
        {
            let mut events = self.events();
            while events.len() >= WATCH_BUFFER_CAP {
                self.record_drop(&mut events);
            }
            events.push_back(event);
        }
        self.notify.notify_one();
    }

    /// Frees one slot by dropping the oldest real event, folding the loss into
    /// a single `Lagged` marker at the front of the buffer.
    fn record_drop(&self, events: &mut VecDeque<MonitorEvent>) {
        if let Some(MonitorEvent::Lagged { .. }) = events.front() {
            // A marker already leads the buffer: drop the oldest real event
            // that follows it and bump the count.
            events.remove(1);
            if let Some(MonitorEvent::Lagged { dropped, .. }) = events.front_mut() {
                *dropped = dropped.saturating_add(1);
            }
        } else {
            // Replace the oldest real event with a fresh marker. This keeps
            // the length unchanged, so the caller's loop drops one more real
            // event before there is room to append.
            events.pop_front();
            events.push_front(MonitorEvent::Lagged {
                actor_id: self.actor_id.clone(),
                dropped: 1,
            });
        }
    }

    /// Removes the next staged event, if any. Called only by the forwarder.
    pub(crate) fn pop(&self) -> Option<MonitorEvent> {
        self.events().pop_front()
    }

    /// A future that resolves when an event may be waiting. Arm it before
    /// observing an empty queue to avoid a lost wake-up.
    pub(crate) fn waiter(&self) -> Notified<'_> {
        self.notify.notified()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

/// Owns a forwarder's end of a [`WatchQueue`] and closes it on drop, so the
/// hub stops staging into the queue whether the forwarder exits normally or
/// unwinds through a panicking `map` closure.
pub(crate) struct WatchQueueGuard {
    queue: Arc<WatchQueue>,
    finished: Finished,
}

impl WatchQueueGuard {
    pub(crate) fn queue(&self) -> &WatchQueue {
        &self.queue
    }
}

impl Drop for WatchQueueGuard {
    fn drop(&mut self) {
        self.queue.close();
        self.finished.signal();
    }
}

struct Watcher {
    cancellation: CancellationToken,
    stop: CancellationToken,
    queue: Arc<WatchQueue>,
}

impl Watcher {
    fn is_live(&self) -> bool {
        !self.cancellation.is_cancelled() && !self.stop.is_cancelled() && !self.queue.is_closed()
    }

    fn notify(&self, event: &MonitorEvent) -> bool {
        if !self.is_live() {
            return false;
        }
        self.queue.push(event.clone());
        true
    }
}

#[derive(Clone, Copy)]
enum Lifecycle {
    Pending,
    Running(u64),
    Exited(u64),
    Terminated(Option<u64>),
}

struct MonitorState {
    next_generation: u64,
    lifecycle: Lifecycle,
    watchers: Vec<Watcher>,
}

pub(crate) struct MonitorHub {
    actor_id: String,
    state: Mutex<MonitorState>,
}

impl MonitorHub {
    pub(crate) fn new(actor_id: &str) -> Self {
        Self {
            actor_id: actor_id.to_owned(),
            state: Mutex::new(MonitorState {
                next_generation: 0,
                lifecycle: Lifecycle::Pending,
                watchers: Vec::new(),
            }),
        }
    }

    /// Registers a persistent watch on this logical actor and returns its
    /// staging queue for the caller's forwarder to drain.
    ///
    /// A running target stages an immediate [`MonitorEvent::Started`] for the
    /// current incarnation. A target between incarnations (or before its
    /// first start) stays silent until the next start. A removed target
    /// stages an immediate final [`MonitorEvent::Removed`] and is not
    /// registered.
    ///
    /// Events are staged while holding the hub lock, which totally orders
    /// them per watch; staging is a non-blocking buffer push, so no user code
    /// runs under the lock.
    pub(crate) fn register_watch(
        &self,
        cancellation: CancellationToken,
        stop: CancellationToken,
        finished: Finished,
    ) -> WatchQueueGuard {
        let queue = WatchQueue::new(&self.actor_id);
        let mut state = self.state();
        state.watchers.retain(Watcher::is_live);
        match state.lifecycle {
            Lifecycle::Terminated(generation) => {
                queue.push(self.removed_event(generation));
                return WatchQueueGuard { queue, finished };
            }
            Lifecycle::Running(generation) => {
                queue.push(self.started_event(generation));
            }
            Lifecycle::Pending | Lifecycle::Exited(_) => {}
        }
        state.watchers.push(Watcher {
            cancellation: cancellation.clone(),
            stop,
            queue: Arc::clone(&queue),
        });
        WatchQueueGuard { queue, finished }
    }

    pub(crate) fn started(&self) {
        let mut state = self.state();
        let generation = state.next_generation;
        state.next_generation = state.next_generation.saturating_add(1);
        state.lifecycle = Lifecycle::Running(generation);
        let started = self.started_event(generation);
        state.watchers.retain(|watcher| watcher.notify(&started));
    }

    pub(crate) fn exited(&self, reason: ExitReason) {
        let mut state = self.state();
        let Lifecycle::Running(generation) = state.lifecycle else {
            return;
        };
        state.lifecycle = Lifecycle::Exited(generation);
        let exited = self.exited_event(generation, reason);
        state.watchers.retain(|watcher| watcher.notify(&exited));
    }

    pub(crate) fn terminated(&self) {
        let mut state = self.state();
        let (exited, generation) = match state.lifecycle {
            Lifecycle::Pending => (None, None),
            Lifecycle::Running(generation) => (
                Some(self.exited_event(generation, ExitReason::Failure)),
                Some(generation),
            ),
            Lifecycle::Exited(generation) => (None, Some(generation)),
            Lifecycle::Terminated(_) => return,
        };
        state.lifecycle = Lifecycle::Terminated(generation);
        let removed = self.removed_event(generation);
        for watcher in state.watchers.drain(..) {
            if !watcher.is_live() {
                continue;
            }
            if let Some(exited) = &exited {
                watcher.queue.push(exited.clone());
            }
            watcher.queue.push(removed.clone());
        }
    }

    fn started_event(&self, generation: u64) -> MonitorEvent {
        MonitorEvent::Started {
            actor_id: self.actor_id.clone(),
            generation,
        }
    }

    fn exited_event(&self, generation: u64, reason: ExitReason) -> MonitorEvent {
        MonitorEvent::Exited {
            actor_id: self.actor_id.clone(),
            generation,
            reason,
        }
    }

    fn removed_event(&self, generation: Option<u64>) -> MonitorEvent {
        MonitorEvent::Removed {
            actor_id: self.actor_id.clone(),
            generation,
        }
    }

    fn state(&self) -> MutexGuard<'_, MonitorState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) struct MonitorExitGuard {
    hub: Arc<MonitorHub>,
    reported: bool,
}

impl MonitorExitGuard {
    pub(crate) fn new(hub: Arc<MonitorHub>) -> Self {
        Self {
            hub,
            reported: false,
        }
    }

    pub(crate) fn report(&mut self, reason: ExitReason) {
        self.hub.exited(reason);
        self.reported = true;
    }
}

impl Drop for MonitorExitGuard {
    fn drop(&mut self) {
        if !self.reported {
            self.hub.exited(ExitReason::Failure);
        }
    }
}

struct MembershipWatch {
    subject: Weak<MonitorHub>,
    cancellation: CancellationToken,
    stop: CancellationToken,
    finished: Finished,
}

/// Owns the watches created by one actor membership.
///
/// Unlike timers, this scope belongs to the restart-stable binding rather
/// than an incarnation. Re-registering a watch from a replacement
/// incarnation finds the existing observer/subject pair, while terminating
/// the binding ends every outbound watch.
pub(crate) struct ActorMonitors {
    lifetime: CancellationToken,
    watches: Mutex<Vec<MembershipWatch>>,
}

impl ActorMonitors {
    pub(crate) fn new() -> Self {
        Self {
            lifetime: CancellationToken::new(),
            watches: Mutex::new(Vec::new()),
        }
    }

    /// Returns the shared state for the unique live watch on `subject` and
    /// whether the caller must install its forwarder.
    pub(crate) fn register(
        &self,
        subject: &Arc<MonitorHub>,
    ) -> (CancellationToken, CancellationToken, Finished, bool) {
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        watches.retain(|watch| {
            !watch.cancellation.is_cancelled()
                && !watch.stop.is_cancelled()
                && !watch.finished.is_signalled()
                && watch.subject.strong_count() > 0
        });
        if let Some(watch) = watches.iter().find(|watch| {
            watch
                .subject
                .upgrade()
                .is_some_and(|registered| Arc::ptr_eq(&registered, subject))
        }) {
            return (
                watch.cancellation.clone(),
                watch.stop.clone(),
                watch.finished.clone(),
                false,
            );
        }

        let cancellation = CancellationToken::new();
        let stop = self.lifetime.child_token();
        let finished = Finished::new();
        watches.push(MembershipWatch {
            subject: Arc::downgrade(subject),
            cancellation: cancellation.clone(),
            stop: stop.clone(),
            finished: finished.clone(),
        });
        (cancellation, stop, finished, true)
    }

    pub(crate) fn terminate(&self) {
        self.lifetime.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started_event(generation: u64) -> MonitorEvent {
        MonitorEvent::Started {
            actor_id: "peer".to_owned(),
            generation,
        }
    }

    fn exited_event(generation: u64) -> MonitorEvent {
        MonitorEvent::Exited {
            actor_id: "peer".to_owned(),
            generation,
            reason: ExitReason::Failure,
        }
    }

    fn lagged_count(events: &VecDeque<MonitorEvent>) -> u64 {
        let markers = events
            .iter()
            .filter_map(|event| match event {
                MonitorEvent::Lagged { dropped, .. } => Some(*dropped),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            markers.len() <= 1,
            "at most one coalesced Lagged marker is kept"
        );
        assert!(
            markers.is_empty() || matches!(events.front(), Some(MonitorEvent::Lagged { .. })),
            "the Lagged marker leads the buffer"
        );
        markers.first().copied().unwrap_or(0)
    }

    #[test]
    fn queue_coalesces_overflow_into_lagged() {
        let queue = WatchQueue::new("peer");
        let overflow = 5;
        let total = WATCH_BUFFER_CAP as u64 + overflow;
        for generation in 0..total {
            queue.push(started_event(generation));
        }

        let events = queue.events();
        assert_eq!(events.len(), WATCH_BUFFER_CAP);
        // Every dropped event is accounted for by the single leading marker,
        // and the newest event is always retained.
        assert!(lagged_count(&events) > 0);
        assert_eq!(events.back(), Some(&started_event(total - 1)));
    }

    #[test]
    fn alternating_overflow_is_flagged_not_silent() {
        let queue = WatchQueue::new("peer");
        // Twice the capacity of alternating Started/Exited forces heavy overflow.
        for generation in 0..(WATCH_BUFFER_CAP as u64) {
            queue.push(started_event(generation));
            queue.push(exited_event(generation));
        }

        let events = queue.events();
        assert_eq!(events.len(), WATCH_BUFFER_CAP);
        // A consumer never silently sees an Exited without its Started: the dropped
        // span is fronted by an explicit Lagged resync marker.
        assert!(
            matches!(events.front(), Some(MonitorEvent::Lagged { .. })),
            "overflow must surface a resync marker, not silently orphan a transition"
        );
        assert!(lagged_count(&events) > 0);
    }

    #[test]
    fn dropping_guard_closes_queue_and_prunes_watcher() {
        let hub = MonitorHub::new("peer");
        let guard = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(hub.state().watchers.len(), 1);

        // A panicking `map` closure unwinds the forwarder, which drops the
        // guard; that must close the queue so the hub stops staging into it.
        drop(guard);
        hub.started();
        assert_eq!(
            hub.state().watchers.len(),
            0,
            "a closed watch must be pruned on the next lifecycle event"
        );
    }

    #[test]
    fn terminal_event_survives_overflow() {
        let queue = WatchQueue::new("peer");
        for generation in 0..(WATCH_BUFFER_CAP as u64 * 2) {
            queue.push(started_event(generation));
        }
        let removed = MonitorEvent::Removed {
            actor_id: "peer".to_owned(),
            generation: Some(7),
        };
        queue.push(removed.clone());

        let mut last = None;
        while let Some(event) = queue.pop() {
            last = Some(event);
        }
        assert_eq!(last, Some(removed));
    }
}
