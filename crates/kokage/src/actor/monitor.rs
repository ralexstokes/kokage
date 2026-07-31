use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::supervisor::{CancellationToken, ExitStatus};
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

/// Lifecycle transition of a watched logical actor.
///
/// Delivered by [`RawContext::watch`](crate::raw::RawContext::watch). Events
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
        status: ExitStatus,
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
    min_generation: u64,
}

impl Watcher {
    fn is_live(&self) -> bool {
        !self.cancellation.is_cancelled() && !self.stop.is_cancelled() && !self.queue.is_closed()
    }

    fn notify(&self, event: &MonitorEvent) -> bool {
        if !self.is_live() {
            return false;
        }
        if matches!(
            event,
            MonitorEvent::Started { generation, .. } | MonitorEvent::Exited { generation, .. }
                if *generation < self.min_generation
        ) {
            return true;
        }
        self.queue.push(event.clone());
        true
    }
}

#[derive(Clone, Copy)]
enum Lifecycle {
    Pending,
    Running { run_id: u64, generation: u64 },
    Exited,
    Removed(Option<u64>),
}

struct MonitorState {
    next_run_id: u64,
    next_generation: u64,
    current_epoch: u64,
    current_last_generation: Option<u64>,
    lifecycle: Lifecycle,
    active_runs: HashMap<u64, ActiveRun>,
    removal_requested: bool,
    watchers: Vec<Watcher>,
    retiring: Vec<RetiringMembership>,
}

#[derive(Clone, Copy)]
struct ActiveRun {
    epoch: u64,
    generation: u64,
}

struct RetiringMembership {
    epoch: u64,
    last_generation: Option<u64>,
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
                next_run_id: 0,
                next_generation: 0,
                current_epoch: 0,
                current_last_generation: None,
                lifecycle: Lifecycle::Pending,
                active_runs: HashMap::new(),
                removal_requested: false,
                watchers: Vec::new(),
                retiring: Vec::new(),
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
        let min_generation = match state.lifecycle {
            Lifecycle::Removed(generation) => {
                queue.push(self.removed_event(generation));
                return WatchQueueGuard { queue, finished };
            }
            Lifecycle::Running { generation, .. } => {
                queue.push(self.started_event(generation));
                generation
            }
            Lifecycle::Pending | Lifecycle::Exited => state.next_generation,
        };
        state.watchers.push(Watcher {
            cancellation: cancellation.clone(),
            stop,
            queue: Arc::clone(&queue),
            min_generation,
        });
        WatchQueueGuard { queue, finished }
    }

    pub(crate) fn new_run(self: &Arc<Self>, reopens_terminal: bool) -> MonitorRun {
        let mut state = self.state();
        let run_id = state.next_run_id;
        state.next_run_id = state.next_run_id.saturating_add(1);
        let reopens_epoch = reopens_terminal.then_some(state.current_epoch);
        MonitorRun {
            hub: Arc::clone(self),
            run_id,
            reopens_epoch,
        }
    }

    fn started(&self, run_id: u64, reopens_epoch: Option<u64>) -> Option<bool> {
        let mut state = self.state();
        let begins_epoch = reopens_epoch == Some(state.current_epoch);
        let joins_reopened_epoch = reopens_epoch.is_some_and(|epoch| {
            epoch.saturating_add(1) == state.current_epoch
                && !matches!(state.lifecycle, Lifecycle::Removed(_))
                && !state.removal_requested
        });
        if reopens_epoch.is_some() && !begins_epoch && !joins_reopened_epoch {
            return None;
        }
        if matches!(state.lifecycle, Lifecycle::Removed(_)) && !begins_epoch {
            return None;
        }
        if begins_epoch {
            if !matches!(state.lifecycle, Lifecycle::Removed(_)) {
                let retiring_epoch = state.current_epoch;
                let last_generation = state.current_last_generation;
                let mut watchers = std::mem::take(&mut state.watchers);
                let has_active = state
                    .active_runs
                    .values()
                    .any(|run| run.epoch == retiring_epoch);
                if has_active {
                    state.retiring.push(RetiringMembership {
                        epoch: retiring_epoch,
                        last_generation,
                        watchers,
                    });
                } else {
                    let removed = self.removed_event(last_generation);
                    for watcher in watchers.drain(..) {
                        if watcher.is_live() {
                            watcher.queue.push(removed.clone());
                        }
                    }
                }
            }
            state.current_epoch = state.current_epoch.saturating_add(1);
            state.current_last_generation = None;
            state.lifecycle = Lifecycle::Pending;
            state.removal_requested = false;
        }
        let generation = state.next_generation;
        state.next_generation = state.next_generation.saturating_add(1);
        state.current_last_generation = Some(generation);
        let epoch = state.current_epoch;
        state
            .active_runs
            .insert(run_id, ActiveRun { epoch, generation });
        state.lifecycle = Lifecycle::Running { run_id, generation };
        let started = self.started_event(generation);
        state.watchers.retain(|watcher| watcher.notify(&started));
        Some(begins_epoch)
    }

    fn exited(&self, run_id: u64, status: ExitStatus) {
        let mut state = self.state();
        let Some(run) = state.active_runs.remove(&run_id) else {
            return;
        };
        let exited = self.exited_event(run.generation, status);
        if run.epoch == state.current_epoch {
            if matches!(state.lifecycle, Lifecycle::Running { run_id: current, .. } if current == run_id)
            {
                state.lifecycle = Lifecycle::Exited;
            }
            let has_active = state
                .active_runs
                .values()
                .any(|active| active.epoch == run.epoch);
            if state.removal_requested && !has_active {
                let last_generation = state.current_last_generation;
                state.lifecycle = Lifecycle::Removed(last_generation);
                let removed = self.removed_event(last_generation);
                for watcher in state.watchers.drain(..) {
                    if watcher.notify(&exited) {
                        watcher.notify(&removed);
                    }
                }
            } else {
                state.watchers.retain(|watcher| watcher.notify(&exited));
            }
            return;
        }

        let has_active = state
            .active_runs
            .values()
            .any(|active| active.epoch == run.epoch);
        let Some(index) = state
            .retiring
            .iter()
            .position(|membership| membership.epoch == run.epoch)
        else {
            return;
        };
        if has_active {
            state.retiring[index]
                .watchers
                .retain(|watcher| watcher.notify(&exited));
        } else {
            let mut membership = state.retiring.remove(index);
            let removed = self.removed_event(membership.last_generation);
            for watcher in membership.watchers.drain(..) {
                if watcher.notify(&exited) {
                    watcher.notify(&removed);
                }
            }
        }
    }

    pub(crate) fn current_epoch(&self) -> u64 {
        self.state().current_epoch
    }

    pub(crate) fn removed(&self, epoch: u64) {
        let mut state = self.state();
        if epoch != state.current_epoch {
            return;
        }
        if matches!(state.lifecycle, Lifecycle::Removed(_)) {
            return;
        }
        if state.active_runs.values().any(|run| run.epoch == epoch) {
            // Binding teardown can race controllers that still own exact exit
            // classifications. Keep removal pending until every started run
            // has staged its own generation-specific exit.
            state.removal_requested = true;
            return;
        }
        let generation = state.current_last_generation;
        state.lifecycle = Lifecycle::Removed(generation);
        let removed = self.removed_event(generation);
        for watcher in state.watchers.drain(..) {
            if !watcher.is_live() {
                continue;
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

    fn exited_event(&self, generation: u64, status: ExitStatus) -> MonitorEvent {
        MonitorEvent::Exited {
            actor_id: self.actor_id.clone(),
            generation,
            status,
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

#[derive(Clone)]
pub(crate) struct MonitorRun {
    hub: Arc<MonitorHub>,
    run_id: u64,
    reopens_epoch: Option<u64>,
}

impl MonitorRun {
    pub(crate) fn id(&self) -> u64 {
        self.run_id
    }

    pub(crate) fn reopens_terminal(&self) -> bool {
        self.reopens_epoch.is_some()
    }

    pub(crate) fn started(&self) -> Option<bool> {
        self.hub.started(self.run_id, self.reopens_epoch)
    }

    pub(crate) fn exited(&self, status: ExitStatus) {
        self.hub.exited(self.run_id, status);
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
    state: Mutex<ActorMonitorsState>,
}

struct ActorMonitorsState {
    epoch: u64,
    lifetime: CancellationToken,
    watches: Vec<MembershipWatch>,
}

/// One actor incarnation's authority to register membership-owned watches.
///
/// Reopening a terminated binding advances the owner's epoch, so contexts
/// from the retired membership cannot attach new watches to its replacement.
#[derive(Clone)]
pub(crate) struct ActorMonitorLease {
    owner: Arc<ActorMonitors>,
    epoch: u64,
}

impl ActorMonitorLease {
    /// Returns the shared state for the unique live watch on `subject` and
    /// whether the caller must install its forwarder.
    pub(crate) fn register(
        &self,
        subject: &Arc<MonitorHub>,
    ) -> (CancellationToken, CancellationToken, Finished, bool) {
        self.owner.register(self.epoch, subject)
    }
}

impl ActorMonitors {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ActorMonitorsState {
                epoch: 0,
                lifetime: CancellationToken::new(),
                watches: Vec::new(),
            }),
        }
    }

    pub(crate) fn lease(self: &Arc<Self>) -> ActorMonitorLease {
        let epoch = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .epoch;
        ActorMonitorLease {
            owner: Arc::clone(self),
            epoch,
        }
    }

    fn register(
        &self,
        epoch: u64,
        subject: &Arc<MonitorHub>,
    ) -> (CancellationToken, CancellationToken, Finished, bool) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.epoch != epoch {
            let finished = Finished::new();
            finished.signal();
            return (
                CancellationToken::new(),
                CancellationToken::new(),
                finished,
                false,
            );
        }
        state.watches.retain(|watch| {
            !watch.cancellation.is_cancelled()
                && !watch.stop.is_cancelled()
                && !watch.finished.is_signalled()
                && watch.subject.strong_count() > 0
        });
        if let Some(watch) = state.watches.iter().find(|watch| {
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
        let stop = state.lifetime.child_token();
        let finished = Finished::new();
        state.watches.push(MembershipWatch {
            subject: Arc::downgrade(subject),
            cancellation: cancellation.clone(),
            stop: stop.clone(),
            finished: finished.clone(),
        });
        (cancellation, stop, finished, true)
    }

    pub(crate) fn terminate(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lifetime
            .cancel();
    }

    pub(crate) fn reopen(self: &Arc<Self>) -> ActorMonitorLease {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.lifetime.cancel();
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("actor monitor membership epoch overflowed");
        state.lifetime = CancellationToken::new();
        state.watches.clear();
        ActorMonitorLease {
            owner: Arc::clone(self),
            epoch: state.epoch,
        }
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
            status: ExitStatus::Failed {
                message: "boom".to_owned(),
                cancelled: false,
            },
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
    fn outbound_monitor_lifetime_rotates_for_a_reopened_membership() {
        let monitors = Arc::new(ActorMonitors::new());
        let subject = Arc::new(MonitorHub::new("peer"));
        let old_lease = monitors.lease();
        let (_, old_stop, _, installed) = old_lease.register(&subject);
        assert!(installed);
        assert!(!old_stop.is_cancelled());

        monitors.terminate();
        assert!(old_stop.is_cancelled());
        let new_lease = monitors.reopen();

        let (_, _, stale_finished, installed) = old_lease.register(&subject);
        assert!(!installed);
        assert!(stale_finished.is_signalled());

        let (_, new_stop, _, installed) = new_lease.register(&subject);
        assert!(installed);
        assert!(!new_stop.is_cancelled());
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
        let hub = Arc::new(MonitorHub::new("peer"));
        let guard = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(hub.state().watchers.len(), 1);

        // A panicking `map` closure unwinds the forwarder, which drops the
        // guard; that must close the queue so the hub stops staging into it.
        drop(guard);
        assert_eq!(hub.new_run(false).started(), Some(false));
        assert_eq!(
            hub.state().watchers.len(),
            0,
            "a closed watch must be pruned on the next lifecycle event"
        );
    }

    #[test]
    fn late_exit_keeps_the_generation_of_its_own_run() {
        let hub = Arc::new(MonitorHub::new("peer"));
        let watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        let first = hub.new_run(false);
        let second = hub.new_run(false);

        assert_eq!(first.started(), Some(false));
        assert_eq!(second.started(), Some(false));
        hub.removed(hub.current_epoch());
        first.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });
        second.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });

        assert_eq!(watch.queue().pop(), Some(started_event(0)));
        assert_eq!(watch.queue().pop(), Some(started_event(1)));
        assert_eq!(watch.queue().pop(), Some(exited_event(0)));
        assert_eq!(watch.queue().pop(), Some(exited_event(1)));
        assert_eq!(
            watch.queue().pop(),
            Some(MonitorEvent::Removed {
                actor_id: "peer".to_owned(),
                generation: Some(1),
            })
        );
        assert_eq!(watch.queue().pop(), None);
    }

    #[test]
    fn watch_registered_during_overlap_ignores_displaced_run_exit() {
        let hub = Arc::new(MonitorHub::new("peer"));
        let first = hub.new_run(false);
        let second = hub.new_run(false);
        assert_eq!(first.started(), Some(false));
        assert_eq!(second.started(), Some(false));
        let watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );

        first.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });
        second.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });

        assert_eq!(watch.queue().pop(), Some(started_event(1)));
        assert_eq!(watch.queue().pop(), Some(exited_event(1)));
        assert_eq!(watch.queue().pop(), None);
    }

    #[test]
    fn terminal_hub_rejects_a_late_run_start() {
        let hub = Arc::new(MonitorHub::new("peer"));
        let late = hub.new_run(false);
        hub.removed(hub.current_epoch());

        assert_eq!(late.started(), None);
        let watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(
            watch.queue().pop(),
            Some(MonitorEvent::Removed {
                actor_id: "peer".to_owned(),
                generation: None,
            })
        );
        assert_eq!(watch.queue().pop(), None);

        let replacement = hub.new_run(true);
        assert_eq!(replacement.started(), Some(true));
        let replacement_watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(replacement_watch.queue().pop(), Some(started_event(0)));
    }

    #[test]
    fn reopen_retires_old_watchers_even_when_replacement_starts_first() {
        let hub = Arc::new(MonitorHub::new("peer"));
        let first = hub.new_run(false);
        assert_eq!(first.started(), Some(false));
        let old_watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(old_watch.queue().pop(), Some(started_event(0)));
        let old_epoch = hub.current_epoch();

        let replacement = hub.new_run(true);
        assert_eq!(replacement.started(), Some(true));
        hub.removed(old_epoch);
        let replacement_watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(replacement_watch.queue().pop(), Some(started_event(1)));
        assert_eq!(old_watch.queue().pop(), None);

        first.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });
        assert_eq!(old_watch.queue().pop(), Some(exited_event(0)));
        assert_eq!(
            old_watch.queue().pop(),
            Some(MonitorEvent::Removed {
                actor_id: "peer".to_owned(),
                generation: Some(0),
            })
        );
        assert_eq!(old_watch.queue().pop(), None);
        assert_eq!(replacement_watch.queue().pop(), None);

        replacement.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: false,
        });
        assert_eq!(replacement_watch.queue().pop(), Some(exited_event(1)));
    }

    #[test]
    fn old_reopen_token_cannot_join_a_termination_pending_epoch() {
        let hub = Arc::new(MonitorHub::new("peer"));
        let first_replacement = hub.new_run(true);
        let stale_replacement = hub.new_run(true);
        assert_eq!(first_replacement.started(), Some(true));
        hub.removed(hub.current_epoch());

        assert_eq!(stale_replacement.started(), None);
        first_replacement.exited(ExitStatus::Failed {
            message: "boom".to_owned(),
            cancelled: true,
        });
        let watch = hub.register_watch(
            CancellationToken::new(),
            CancellationToken::new(),
            Finished::new(),
        );
        assert_eq!(
            watch.queue().pop(),
            Some(MonitorEvent::Removed {
                actor_id: "peer".to_owned(),
                generation: Some(0),
            })
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
