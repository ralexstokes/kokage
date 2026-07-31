use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::supervisor::{RestartPolicy, ScopePathSegment};
use tokio::{
    sync::{Notify, mpsc, watch},
    time::{Instant, sleep_until},
};

use crate::actor::{
    error::{SendError, SendErrorKind},
    monitor::{ActorMonitorLease, ActorMonitors, MonitorHub, MonitorRun},
    observability::{MessageRejection, MessageSizeMetrics, ScopeObservability},
};

/// A point-in-time snapshot of one actor's message and mailbox statistics.
///
/// Sample these stats through [`ActorRef::stats`](crate::ActorRef::stats).
/// Message counters accumulate for the lifetime of the actor binding and
/// therefore survive restarts. Outstanding-work gauges and mailbox fields
/// describe the currently bound incarnation and are zero while no mailbox is
/// bound. Enabling the `serde` feature implements `Serialize` and
/// `Deserialize` for this type.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ActorStats {
    /// Actor id used to correlate these stats with supervisor snapshots.
    pub actor_id: String,
    /// Messages delivered to the actor for handling.
    ///
    /// This includes actor-local continuations, timer events, and offload
    /// completions, which bypass the mailbox. It can also be lower than
    /// [`messages_accepted`](Self::messages_accepted): accepted messages may be
    /// conflated before receipt or discarded when an incarnation stops.
    pub messages_received: u64,
    /// Messages accepted into the mailbox by `send`, `send_timeout`, or
    /// `try_send`.
    ///
    /// Acceptance does not mean the actor handled the message. In particular,
    /// a conflating mailbox counts every successful send here and separately
    /// counts unread messages replaced by newer ones in
    /// [`messages_conflated`](Self::messages_conflated).
    pub messages_accepted: u64,
    /// Previously unread messages replaced by newer messages in a conflating mailbox.
    pub messages_conflated: u64,
    /// Total bytes reported for accepted messages when message-size
    /// observation is enabled for this actor.
    ///
    /// `None` means the actor did not opt in or its declaration has not yet
    /// been materialized. The total accumulates across actor restarts, like
    /// the message counters.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub message_bytes_accepted: Option<u64>,
    /// Mailbox sends rejected before acceptance.
    ///
    /// This includes `send`, `send_timeout`, or `try_send` calls that returned
    /// an error.
    pub sends_rejected: u64,
    /// Offloads currently owned by this actor incarnation.
    ///
    /// This is a gauge rather than a lifetime counter. It returns to zero
    /// when offloads finish, time out, or are aborted.
    pub outstanding_offloads: u64,
    /// Messages currently occupying the bound mailbox, or zero when sampled
    /// while the actor has no bound incarnation.
    pub mailbox_depth: usize,
    /// Maximum capacity of the currently bound mailbox, or zero when sampled
    /// while the actor has no bound incarnation.
    pub mailbox_capacity: usize,
}

/// Actor statistics paired with their current supervision membership.
///
/// Returned by [`ScopeRef::actor_stats`](crate::ScopeRef::actor_stats). A
/// direct child of the sampled scope has an empty `scope_path`; nested path
/// segments carry the subtree lineage and generation needed to distinguish
/// identical local actor ids in different or restarted subtrees.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ScopedActorStats {
    /// Identity path of the nested scopes containing the actor.
    pub scope_path: Vec<ScopePathSegment>,
    /// Identity of the actor's current supervisor membership.
    pub lineage: u64,
    /// Actor-local counters, gauges, and mailbox data.
    pub stats: ActorStats,
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use super::{ActorStats, ScopePathSegment, ScopedActorStats};
    use serde_json::json;

    #[test]
    fn scoped_actor_stats_round_trip() {
        let stats = ScopedActorStats {
            scope_path: vec![ScopePathSegment {
                id: "workers".into(),
                lineage: 7,
                generation: 2,
            }],
            lineage: 11,
            stats: ActorStats {
                actor_id: "worker".into(),
                messages_received: 13,
                messages_accepted: 17,
                messages_conflated: 3,
                message_bytes_accepted: Some(1_024),
                sends_rejected: 1,
                outstanding_offloads: 2,
                mailbox_depth: 5,
                mailbox_capacity: 32,
            },
        };

        let value = serde_json::to_value(&stats).expect("actor stats serialize");
        assert_eq!(
            value["scope_path"],
            json!([{"id": "workers", "lineage": 7, "generation": 2}])
        );
        let decoded: ScopedActorStats =
            serde_json::from_value(value).expect("actor stats deserialize");
        assert_eq!(decoded, stats);
    }

    #[test]
    fn actor_stats_omit_absent_optional_fields() {
        let stats = ActorStats {
            actor_id: "worker".into(),
            messages_received: 0,
            messages_accepted: 0,
            messages_conflated: 0,
            message_bytes_accepted: None,
            sends_rejected: 0,
            outstanding_offloads: 0,
            mailbox_depth: 0,
            mailbox_capacity: 0,
        };

        let value = serde_json::to_value(stats).expect("actor stats serialize");
        assert!(value.get("message_bytes_accepted").is_none());
    }
}

#[derive(Debug)]
pub(crate) struct ActorStatsCounters {
    messages_received: AtomicU64,
    messages_accepted: AtomicU64,
    messages_conflated: AtomicU64,
    sends_rejected: AtomicU64,
    message_bytes_accepted: AtomicU64,
    observe_message_size: AtomicBool,
    outstanding_offloads: AtomicU64,
}

impl ActorStatsCounters {
    pub(crate) fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_accepted: AtomicU64::new(0),
            messages_conflated: AtomicU64::new(0),
            sends_rejected: AtomicU64::new(0),
            message_bytes_accepted: AtomicU64::new(0),
            observe_message_size: AtomicBool::new(false),
            outstanding_offloads: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_send(&self, accepted: bool) {
        let counter = if accepted {
            &self.messages_accepted
        } else {
            &self.sends_rejected
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_conflated(&self, count: u64) {
        if count > 0 {
            self.messages_conflated.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_message_size(&self, size: usize) {
        self.message_bytes_accepted
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    pub(crate) fn enable_message_size(&self) {
        self.observe_message_size.store(true, Ordering::Relaxed);
    }

    pub(crate) fn set_outstanding_offloads(&self, outstanding: usize) {
        self.outstanding_offloads
            .store(outstanding as u64, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(
        &self,
        actor_id: &str,
        mailbox_depth: usize,
        mailbox_capacity: usize,
    ) -> ActorStats {
        ActorStats {
            actor_id: actor_id.to_owned(),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_accepted: self.messages_accepted.load(Ordering::Relaxed),
            messages_conflated: self.messages_conflated.load(Ordering::Relaxed),
            message_bytes_accepted: self
                .observe_message_size
                .load(Ordering::Relaxed)
                .then(|| self.message_bytes_accepted.load(Ordering::Relaxed)),
            sends_rejected: self.sends_rejected.load(Ordering::Relaxed),
            outstanding_offloads: self.outstanding_offloads.load(Ordering::Relaxed),
            mailbox_depth,
            mailbox_capacity,
        }
    }
}

type KeyMatcher<M> = Arc<dyn Fn(&M, &M) -> bool + Send + Sync>;

/// Configures how an actor stores unread messages.
///
/// FIFO [`queue`](Self::queue) mailboxes apply backpressure at their capacity.
/// Latest-wins mailboxes never wait for capacity: they replace stale unread
/// state and are intended for idempotent snapshots, not commands.
#[non_exhaustive]
pub struct Mailbox<M> {
    kind: MailboxKind<M>,
    capacity: Option<usize>,
}

enum MailboxKind<M> {
    Queue,
    Latest,
    LatestByKey { key_matches: KeyMatcher<M> },
}

impl<M> Mailbox<M> {
    /// Creates a bounded FIFO queue.
    ///
    /// `capacity` must be non-zero. Supervised placement validates it when the
    /// tree is built; a direct actor host reports the error when it starts.
    pub fn queue(capacity: usize) -> Self {
        Self {
            kind: MailboxKind::Queue,
            capacity: Some(capacity),
        }
    }

    /// One latest-wins slot for the whole mailbox.
    ///
    /// Sending never waits for capacity. Awaited
    /// [`ActorRef::send`](crate::ActorRef::send) calls consume Tokio's
    /// cooperative task budget so tight producer loops remain fair.
    pub fn latest() -> Self {
        Self {
            kind: MailboxKind::Latest,
            capacity: Some(1),
        }
    }

    /// Creates a keyed latest-wins mailbox using `key` to group messages.
    ///
    /// The mailbox stores one latest unread message per key, bounded by its
    /// `capacity`. When that capacity is already occupied by
    /// distinct keys, a message for a new key evicts the oldest unread key.
    /// Sending never waits for capacity.
    ///
    /// Each send scans at most `capacity` entries and may call `key` for both
    /// the incoming and each queued message. Keep extraction cheap and prefer
    /// clone-free keys such as numeric or interned ids.
    /// `capacity` must be non-zero.
    pub fn latest_by_key<K, F>(capacity: usize, key: F) -> Self
    where
        K: Eq,
        F: Fn(&M) -> K + Send + Sync + 'static,
    {
        Self {
            kind: MailboxKind::LatestByKey {
                key_matches: Arc::new(move |left, right| key(left) == key(right)),
            },
            capacity: Some(capacity),
        }
    }

    pub(crate) fn inherited_queue() -> Self {
        Self {
            kind: MailboxKind::Queue,
            capacity: None,
        }
    }

    pub(crate) fn capacity_or(&self, default: usize) -> usize {
        self.capacity.unwrap_or(default)
    }
}

impl<M> Clone for Mailbox<M> {
    fn clone(&self) -> Self {
        let kind = match &self.kind {
            MailboxKind::Queue => MailboxKind::Queue,
            MailboxKind::Latest => MailboxKind::Latest,
            MailboxKind::LatestByKey { key_matches } => MailboxKind::LatestByKey {
                key_matches: Arc::clone(key_matches),
            },
        };
        Self {
            kind,
            capacity: self.capacity,
        }
    }
}

impl<M> fmt::Debug for Mailbox<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            MailboxKind::Queue => f
                .debug_struct("Queue")
                .field("capacity", &self.capacity)
                .finish(),
            MailboxKind::Latest => f.write_str("Latest"),
            MailboxKind::LatestByKey { .. } => f
                .debug_struct("LatestByKey")
                .field("capacity", &self.capacity)
                .finish(),
        }
    }
}

pub(crate) enum MailboxReceiver<M> {
    Queue {
        receiver: mpsc::Receiver<M>,
        accepting_external: Arc<AtomicBool>,
    },
    Conflating(ConflatingReceiver<M>),
}

impl<M> MailboxReceiver<M> {
    pub(crate) async fn recv(&mut self) -> Option<M> {
        match self {
            Self::Queue { receiver, .. } => receiver.recv().await,
            Self::Conflating(receiver) => receiver.recv().await,
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<M, mpsc::error::TryRecvError> {
        match self {
            Self::Queue { receiver, .. } => receiver.try_recv(),
            Self::Conflating(receiver) => receiver.try_recv(),
        }
    }

    pub(crate) fn usage(&self) -> (usize, usize) {
        match self {
            Self::Queue { receiver, .. } => (receiver.len(), receiver.max_capacity()),
            Self::Conflating(receiver) => receiver.usage(),
        }
    }

    pub(crate) fn close_external(&mut self) {
        match self {
            Self::Queue {
                accepting_external, ..
            } => accepting_external.store(false, Ordering::Release),
            Self::Conflating(receiver) => receiver.close_external(),
        }
    }
}

pub(crate) fn mailbox<M>(
    mode: &Mailbox<M>,
    capacity: usize,
) -> (MailboxSender<M>, MailboxReceiver<M>) {
    match &mode.kind {
        MailboxKind::Queue => {
            let (sender, receiver) = mpsc::channel(capacity);
            let accepting_external = Arc::new(AtomicBool::new(true));
            (
                MailboxSender::Queue {
                    sender,
                    accepting_external: Arc::clone(&accepting_external),
                },
                MailboxReceiver::Queue {
                    receiver,
                    accepting_external,
                },
            )
        }
        MailboxKind::Latest => {
            let (sender, receiver) = conflating_channel(1, None);
            (
                MailboxSender::Conflating(sender),
                MailboxReceiver::Conflating(receiver),
            )
        }
        MailboxKind::LatestByKey { key_matches } => {
            let (sender, receiver) = conflating_channel(capacity, Some(Arc::clone(key_matches)));
            (
                MailboxSender::Conflating(sender),
                MailboxReceiver::Conflating(receiver),
            )
        }
    }
}

pub(crate) enum SendOutcome<M> {
    Accepted { conflated: u64 },
    Closed(M),
}

pub(crate) enum TimedSendOutcome<M> {
    Accepted { conflated: u64 },
    Closed(M),
    Timeout(M),
}

#[derive(Debug)]
pub(crate) struct TrySendFailure<M> {
    pub(crate) error: SendError<M>,
    /// Exact mailbox-level rejection for telemetry.
    ///
    /// A public `NotRunning` error can mean there is no live actor binding or
    /// that a mailbox resolved just before it closed. Keeping the telemetry
    /// reason alongside it preserves that distinction without exposing it in
    /// the delivery API.
    pub(crate) rejection: MessageRejection,
}

/// Sender for one bound mailbox instance of an actor.
pub(crate) struct MailboxRef<M> {
    actor_id: Arc<str>,
    sender: MailboxSender<M>,
}

impl<M> Clone for MailboxRef<M> {
    fn clone(&self) -> Self {
        Self {
            actor_id: Arc::clone(&self.actor_id),
            sender: self.sender.clone(),
        }
    }
}

impl<M> MailboxRef<M> {
    pub(crate) fn new(actor_id: Arc<str>, sender: MailboxSender<M>) -> Self {
        Self { actor_id, sender }
    }

    /// Sends, returning the message on failure so callers can retry after a
    /// rebind.
    pub(crate) async fn send_retaining(&self, message: M) -> SendOutcome<M> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return SendOutcome::Closed(message);
                }
                match sender.reserve().await {
                    // This narrows the close race after reserving capacity;
                    // close_external is an intake signal, not a linearizable
                    // fence against a sender already in this operation.
                    Ok(permit) if accepting_external.load(Ordering::Acquire) => {
                        permit.send(message);
                        SendOutcome::Accepted { conflated: 0 }
                    }
                    Ok(_) | Err(_) => SendOutcome::Closed(message),
                }
            }
            MailboxSender::Conflating(sender) => {
                // Cooperate before acceptance so cancellation while yielding
                // still drops the message, matching `ActorRef::send`'s
                // cancellation contract.
                tokio::task::coop::consume_budget().await;
                sender.send(message)
            }
        }
    }

    /// Sends before `deadline`, retaining the message when capacity or a
    /// cooperative yield does not complete in time.
    pub(crate) async fn send_retaining_until(
        &self,
        message: M,
        deadline: Instant,
    ) -> TimedSendOutcome<M> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return TimedSendOutcome::Closed(message);
                }
                let reserved = tokio::select! {
                    biased;
                    () = sleep_until(deadline) => return TimedSendOutcome::Timeout(message),
                    reserved = sender.reserve() => reserved,
                };
                match reserved {
                    Ok(_) if !accepting_external.load(Ordering::Acquire) => {
                        TimedSendOutcome::Closed(message)
                    }
                    Ok(_) if Instant::now() >= deadline => TimedSendOutcome::Timeout(message),
                    Ok(permit) => {
                        permit.send(message);
                        TimedSendOutcome::Accepted { conflated: 0 }
                    }
                    Err(_) => TimedSendOutcome::Closed(message),
                }
            }
            MailboxSender::Conflating(sender) => {
                tokio::select! {
                    biased;
                    () = sleep_until(deadline) => TimedSendOutcome::Timeout(message),
                    () = tokio::task::coop::consume_budget() => {
                        sender.send_until(message, deadline)
                    },
                }
            }
        }
    }

    pub(crate) fn try_send(&self, message: M) -> Result<u64, TrySendFailure<M>> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return Err(TrySendFailure {
                        error: SendError {
                            actor_id: self.actor_id.to_string(),
                            message,
                            kind: SendErrorKind::NotRunning,
                        },
                        rejection: MessageRejection::MailboxClosed,
                    });
                }
                match sender.try_reserve() {
                    Ok(permit) if accepting_external.load(Ordering::Acquire) => {
                        permit.send(message);
                        Ok(0)
                    }
                    Ok(_) | Err(mpsc::error::TrySendError::Closed(_)) => Err(TrySendFailure {
                        error: SendError {
                            actor_id: self.actor_id.to_string(),
                            message,
                            kind: SendErrorKind::NotRunning,
                        },
                        rejection: MessageRejection::MailboxClosed,
                    }),
                    Err(mpsc::error::TrySendError::Full(_)) => Err(TrySendFailure {
                        error: SendError {
                            actor_id: self.actor_id.to_string(),
                            message,
                            kind: SendErrorKind::Full,
                        },
                        rejection: MessageRejection::MailboxFull,
                    }),
                }
            }
            MailboxSender::Conflating(sender) => match sender.send(message) {
                SendOutcome::Accepted { conflated } => Ok(conflated),
                SendOutcome::Closed(message) => Err(TrySendFailure {
                    error: SendError {
                        actor_id: self.actor_id.to_string(),
                        message,
                        kind: SendErrorKind::NotRunning,
                    },
                    rejection: MessageRejection::MailboxClosed,
                }),
            },
        }
    }

    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }

    pub(crate) fn usage(&self) -> (usize, usize) {
        self.sender.usage()
    }
}

pub(crate) enum MailboxSender<M> {
    Queue {
        sender: mpsc::Sender<M>,
        accepting_external: Arc<AtomicBool>,
    },
    Conflating(ConflatingSender<M>),
}

impl<M> Clone for MailboxSender<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Queue {
                sender,
                accepting_external,
            } => Self::Queue {
                sender: sender.clone(),
                accepting_external: Arc::clone(accepting_external),
            },
            Self::Conflating(sender) => Self::Conflating(sender.clone()),
        }
    }
}

impl<M> MailboxSender<M> {
    fn same_channel(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Queue { sender: left, .. }, Self::Queue { sender: right, .. }) => {
                left.same_channel(right)
            }
            (Self::Conflating(left), Self::Conflating(right)) => left.same_channel(right),
            _ => false,
        }
    }

    fn usage(&self) -> (usize, usize) {
        match self {
            Self::Queue { sender, .. } => {
                let capacity = sender.max_capacity();
                (capacity.saturating_sub(sender.capacity()), capacity)
            }
            Self::Conflating(sender) => sender.usage(),
        }
    }
}

struct ConflatingState<M> {
    messages: VecDeque<M>,
    capacity: usize,
    key_matches: Option<KeyMatcher<M>>,
    sender_count: usize,
    receiver_closed: bool,
    accepting_external: bool,
}

struct ConflatingShared<M> {
    state: Mutex<ConflatingState<M>>,
    notify: Notify,
}

impl<M> ConflatingShared<M> {
    fn lock(&self) -> std::sync::MutexGuard<'_, ConflatingState<M>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct ConflatingSender<M> {
    shared: Arc<ConflatingShared<M>>,
}

impl<M> ConflatingSender<M> {
    fn send(&self, message: M) -> SendOutcome<M> {
        let mut state = self.shared.lock();
        if state.receiver_closed || !state.accepting_external {
            return SendOutcome::Closed(message);
        }

        let conflated = if let Some(key_matches) = &state.key_matches {
            if let Some(index) = state
                .messages
                .iter()
                .position(|queued| key_matches(queued, &message))
            {
                state.messages[index] = message;
                1
            } else {
                let evicted = u64::from(state.messages.len() == state.capacity);
                if evicted == 1 {
                    state.messages.pop_front();
                }
                state.messages.push_back(message);
                evicted
            }
        } else if state.messages.is_empty() {
            state.messages.push_back(message);
            0
        } else {
            state.messages[0] = message;
            1
        };
        drop(state);
        self.shared.notify.notify_one();
        SendOutcome::Accepted { conflated }
    }

    fn send_until(&self, message: M, deadline: Instant) -> TimedSendOutcome<M> {
        let mut state = self.shared.lock();
        if state.receiver_closed || !state.accepting_external {
            return TimedSendOutcome::Closed(message);
        }
        if Instant::now() >= deadline {
            return TimedSendOutcome::Timeout(message);
        }

        // Key matching is user code and can run for an arbitrary amount of
        // time. Compute the stable position under the lock, then recheck the
        // acceptance conditions immediately before mutating the queue.
        let matched = state.key_matches.as_ref().and_then(|key_matches| {
            state
                .messages
                .iter()
                .position(|queued| key_matches(queued, &message))
        });
        let keyed = state.key_matches.is_some();
        if state.receiver_closed || !state.accepting_external {
            return TimedSendOutcome::Closed(message);
        }
        if Instant::now() >= deadline {
            return TimedSendOutcome::Timeout(message);
        }

        let conflated = if let Some(index) = matched {
            state.messages[index] = message;
            1
        } else if keyed {
            let evicted = u64::from(state.messages.len() == state.capacity);
            if evicted == 1 {
                state.messages.pop_front();
            }
            state.messages.push_back(message);
            evicted
        } else if state.messages.is_empty() {
            state.messages.push_back(message);
            0
        } else {
            state.messages[0] = message;
            1
        };
        drop(state);
        self.shared.notify.notify_one();
        TimedSendOutcome::Accepted { conflated }
    }

    fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    fn usage(&self) -> (usize, usize) {
        let state = self.shared.lock();
        (state.messages.len(), state.capacity)
    }
}

impl<M> Clone for ConflatingSender<M> {
    fn clone(&self) -> Self {
        let mut state = self.shared.lock();
        state.sender_count += 1;
        drop(state);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<M> Drop for ConflatingSender<M> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.sender_count -= 1;
        let last = state.sender_count == 0;
        drop(state);
        if last {
            self.shared.notify.notify_one();
        }
    }
}

pub(crate) struct ConflatingReceiver<M> {
    shared: Arc<ConflatingShared<M>>,
}

impl<M> ConflatingReceiver<M> {
    async fn recv(&mut self) -> Option<M> {
        loop {
            let notified = self.shared.notify.notified();
            {
                let mut state = self.shared.lock();
                if let Some(message) = state.messages.pop_front() {
                    return Some(message);
                }
                if state.receiver_closed || state.sender_count == 0 {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn try_recv(&mut self) -> Result<M, mpsc::error::TryRecvError> {
        let mut state = self.shared.lock();
        if let Some(message) = state.messages.pop_front() {
            Ok(message)
        } else if state.receiver_closed || state.sender_count == 0 {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }

    fn close_external(&mut self) {
        self.shared.lock().accepting_external = false;
    }

    fn usage(&self) -> (usize, usize) {
        let state = self.shared.lock();
        (state.messages.len(), state.capacity)
    }
}

impl<M> Drop for ConflatingReceiver<M> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.receiver_closed = true;
        drop(state);
        self.shared.notify.notify_waiters();
    }
}

fn conflating_channel<M>(
    capacity: usize,
    key_matches: Option<KeyMatcher<M>>,
) -> (ConflatingSender<M>, ConflatingReceiver<M>) {
    let shared = Arc::new(ConflatingShared {
        state: Mutex::new(ConflatingState {
            messages: VecDeque::with_capacity(capacity),
            capacity,
            key_matches,
            sender_count: 1,
            receiver_closed: false,
            accepting_external: true,
        }),
        notify: Notify::new(),
    });
    (
        ConflatingSender {
            shared: Arc::clone(&shared),
        },
        ConflatingReceiver { shared },
    )
}

/// The current lifecycle state of an actor mailbox binding.
pub(crate) enum BindingState<M> {
    /// Not yet started, or between restarts where a new mailbox is expected.
    Unbound,
    Bound(MailboxRef<M>),
    /// No restart is scheduled.
    Terminated,
}

impl<M> Clone for BindingState<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Unbound => Self::Unbound,
            Self::Bound(mailbox) => Self::Bound(mailbox.clone()),
            Self::Terminated => Self::Terminated,
        }
    }
}

pub(crate) trait BindingLifecycle: Send + Sync {
    fn identity(&self) -> &Arc<()>;
    fn unbind(&self);
    fn terminate(&self);
    fn monitor_run(&self) -> MonitorRun;
    fn stats(&self) -> ActorStats;
}

/// Long-lived binding slot for one actor's current mailbox.
///
/// [`ActorRef`](crate::ActorRef)s subscribe to this slot, so they
/// transparently follow the current mailbox across per-actor restarts.
pub(crate) struct BindingCore<M> {
    identity: Arc<()>,
    actor_id: Arc<str>,
    current: watch::Sender<BindingState<M>>,
    stats: Arc<ActorStatsCounters>,
    message_size: Arc<OnceLock<MessageSizeObserver<M>>>,
    monitors: Arc<MonitorHub>,
    outbound_monitors: Arc<ActorMonitors>,
    latest_bind_run: Mutex<Option<u64>>,
}

pub(crate) struct MessageSizeObserver<M> {
    size_hint: fn(&M) -> usize,
    metrics: MessageSizeMetrics,
}

impl<M> MessageSizeObserver<M> {
    pub(crate) fn size_hint(&self, message: &M) -> usize {
        (self.size_hint)(message)
    }

    pub(crate) fn record_metrics(&self, size: usize) {
        self.metrics.record(size);
    }
}

impl<M> BindingCore<M> {
    pub(crate) fn new(actor_id: Arc<str>) -> Self {
        let (current, _receiver) = watch::channel(BindingState::Unbound);
        let monitors = Arc::new(MonitorHub::new(&actor_id));
        let outbound_monitors = Arc::new(ActorMonitors::new());
        Self {
            identity: Arc::new(()),
            actor_id,
            current,
            stats: Arc::new(ActorStatsCounters::new()),
            message_size: Arc::new(OnceLock::new()),
            monitors,
            outbound_monitors,
            latest_bind_run: Mutex::new(None),
        }
    }

    pub(crate) fn set_message_size(&self, size_hint: fn(&M) -> usize) {
        // Materialization calls this before the first mailbox can bind, so
        // accepted sends cannot race the observer's one-time installation.
        let observer = MessageSizeObserver {
            size_hint,
            metrics: MessageSizeMetrics::new(&self.actor_id),
        };
        assert!(
            self.message_size.set(observer).is_ok(),
            "an actor binding's message-size hint is set only once"
        );
        self.stats.enable_message_size();
    }

    pub(crate) fn actor_id(&self) -> &Arc<str> {
        &self.actor_id
    }

    pub(crate) fn identity(&self) -> &Arc<()> {
        &self.identity
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<BindingState<M>> {
        self.current.subscribe()
    }

    pub(crate) fn stats_counters(&self) -> Arc<ActorStatsCounters> {
        Arc::clone(&self.stats)
    }

    pub(crate) fn message_size(&self) -> Arc<OnceLock<MessageSizeObserver<M>>> {
        Arc::clone(&self.message_size)
    }

    pub(crate) fn monitor_hub(&self) -> Arc<MonitorHub> {
        Arc::clone(&self.monitors)
    }

    #[cfg(test)]
    pub(crate) fn outbound_monitors(&self) -> Arc<ActorMonitors> {
        Arc::clone(&self.outbound_monitors)
    }

    pub(crate) fn stats(&self) -> ActorStats {
        let state = self.current.borrow();
        let (depth, capacity) = match &*state {
            BindingState::Bound(mailbox) => mailbox.usage(),
            BindingState::Unbound | BindingState::Terminated => (0, 0),
        };
        self.stats.snapshot(&self.actor_id, depth, capacity)
    }

    fn bind(&self, mailbox: MailboxRef<M>, monitor_run: &MonitorRun) -> Option<ActorMonitorLease> {
        let mut latest_run = self
            .latest_bind_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if latest_run.is_some_and(|latest| monitor_run.id() < latest) {
            return None;
        }
        let bound = self.current.send_if_modified(|state| {
            if matches!(state, BindingState::Terminated) && !monitor_run.reopens_terminal() {
                false
            } else {
                *state = BindingState::Bound(mailbox.clone());
                true
            }
        });
        if !bound {
            return None;
        }
        *latest_run = Some(monitor_run.id());
        if let Some(reopened) = monitor_run.started() {
            let lease = if reopened {
                self.outbound_monitors.reopen()
            } else {
                self.outbound_monitors.lease()
            };
            return Some(lease);
        }
        // Terminal monitor state can win after the mailbox transition but
        // before this run registers. Preserve that terminal decision.
        self.current.send_if_modified(|state| {
            if matches!(state, BindingState::Bound(current) if current.same_channel(&mailbox)) {
                *state = BindingState::Terminated;
                true
            } else {
                false
            }
        });
        None
    }

    pub(crate) fn monitor_run(&self) -> MonitorRun {
        // A terminal observation grants this run authority to reopen the
        // corresponding monitor epoch. Keep that observation and the epoch
        // capture atomic with binding and terminalization, or a paused caller
        // could mint authority from a replacement that has already reopened.
        let _bind_order = self
            .latest_bind_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reopens_terminal = matches!(*self.current.borrow(), BindingState::Terminated);
        self.monitors.new_run(reopens_terminal)
    }

    /// Only a bound mailbox can be unbound: once a binding is terminated, a
    /// racing unbind from a late run teardown must not regress it to
    /// `Unbound`, or senders would wait for a rebind that never comes.
    pub(crate) fn unbind(&self) {
        self.current.send_if_modified(|state| {
            if matches!(state, BindingState::Bound(_)) {
                *state = BindingState::Unbound;
                true
            } else {
                false
            }
        });
    }

    fn unbind_mailbox(&self, mailbox: &MailboxRef<M>) -> bool {
        self.current.send_if_modified(|state| {
            if matches!(state, BindingState::Bound(current) if current.same_channel(mailbox)) {
                *state = BindingState::Unbound;
                true
            } else {
                false
            }
        })
    }

    fn terminate_mailbox(&self, mailbox: &MailboxRef<M>) -> bool {
        let _bind_order = self
            .latest_bind_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let monitor_epoch = self.monitors.current_epoch();
        let terminated = self.current.send_if_modified(|state| {
            if matches!(state, BindingState::Bound(current) if current.same_channel(mailbox)) {
                *state = BindingState::Terminated;
                true
            } else {
                false
            }
        });
        if terminated {
            self.monitors.removed(monitor_epoch);
            self.outbound_monitors.terminate();
        }
        terminated
    }

    pub(crate) fn terminate(&self) {
        let _bind_order = self
            .latest_bind_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let monitor_epoch = self.monitors.current_epoch();
        self.current.send_replace(BindingState::Terminated);
        self.monitors.removed(monitor_epoch);
        self.outbound_monitors.terminate();
    }
}

impl<M: Send + 'static> BindingLifecycle for BindingCore<M> {
    fn identity(&self) -> &Arc<()> {
        BindingCore::identity(self)
    }

    fn unbind(&self) {
        BindingCore::unbind(self);
    }

    fn terminate(&self) {
        BindingCore::terminate(self);
    }

    fn monitor_run(&self) -> MonitorRun {
        BindingCore::monitor_run(self)
    }

    fn stats(&self) -> ActorStats {
        BindingCore::stats(self)
    }
}

impl<M> Drop for BindingCore<M> {
    fn drop(&mut self) {
        let monitor_epoch = self.monitors.current_epoch();
        self.monitors.removed(monitor_epoch);
        self.outbound_monitors.terminate();
    }
}

/// Binds a mailbox on creation and clears the binding when the actor's run
/// ends.
pub(crate) struct BindingGuard<M> {
    core: Arc<BindingCore<M>>,
    mailbox: MailboxRef<M>,
    observability: ScopeObservability,
    restart_policy: RestartPolicy,
    monitor_lease: ActorMonitorLease,
}

impl<M> BindingGuard<M> {
    pub(crate) fn bind(
        core: Arc<BindingCore<M>>,
        mailbox: MailboxRef<M>,
        monitor_run: &MonitorRun,
        observability: ScopeObservability,
        restart_policy: RestartPolicy,
    ) -> Option<Self> {
        let monitor_lease = core.bind(mailbox.clone(), monitor_run)?;
        observability.emit_mailbox_bound(core.actor_id());
        Some(Self {
            core,
            mailbox,
            observability,
            restart_policy,
            monitor_lease,
        })
    }

    pub(crate) fn monitor_lease(&self) -> ActorMonitorLease {
        self.monitor_lease.clone()
    }
}

impl<M> Drop for BindingGuard<M> {
    fn drop(&mut self) {
        let cleared = if self.restart_policy.is_never() {
            self.core.terminate_mailbox(&self.mailbox)
        } else {
            // A dropped run is a failure for restart purposes. Unknown future
            // policies remain rebindable until `run_disposition` can make the
            // final decision from the observed exit status.
            self.core.unbind_mailbox(&self.mailbox)
        };
        if cleared {
            self.observability
                .emit_mailbox_cleared(self.core.actor_id());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_mailbox_closed<M>(failure: TrySendFailure<M>) {
        assert!(matches!(
            failure.error,
            SendError {
                actor_id,
                kind: SendErrorKind::NotRunning,
                ..
            } if actor_id == "worker"
        ));
        assert_eq!(failure.rejection, MessageRejection::MailboxClosed);
    }

    #[test]
    fn try_send_keeps_closed_queue_rejection_private() {
        let (sender, mut receiver) = mailbox(&Mailbox::queue(1), 1);
        receiver.close_external();
        let mailbox = MailboxRef::new(Arc::from("worker"), sender);

        assert_mailbox_closed(mailbox.try_send(()).expect_err("mailbox is closed"));
    }

    #[test]
    fn try_send_keeps_dropped_queue_rejection_private() {
        let (sender, receiver) = mailbox(&Mailbox::queue(1), 1);
        drop(receiver);
        let mailbox = MailboxRef::new(Arc::from("worker"), sender);

        assert_mailbox_closed(mailbox.try_send(()).expect_err("mailbox is dropped"));
    }

    #[test]
    fn try_send_keeps_dropped_conflating_rejection_private() {
        let (sender, receiver) = mailbox(&Mailbox::latest(), 1);
        drop(receiver);
        let mailbox = MailboxRef::new(Arc::from("worker"), sender);

        assert_mailbox_closed(mailbox.try_send(()).expect_err("mailbox is dropped"));
    }

    #[test]
    fn try_send_keeps_closed_conflating_rejection_private() {
        let (sender, mut receiver) = mailbox(&Mailbox::latest(), 1);
        receiver.close_external();
        let mailbox = MailboxRef::new(Arc::from("worker"), sender);

        assert_mailbox_closed(mailbox.try_send(()).expect_err("mailbox is closed"));
    }

    fn incarnation_mailbox(actor_id: &Arc<str>) -> (MailboxRef<()>, MailboxReceiver<()>) {
        let (sender, receiver) = mailbox(&Mailbox::queue(1), 1);
        (MailboxRef::new(Arc::clone(actor_id), sender), receiver)
    }

    fn assert_bound_to(core: &BindingCore<()>, mailbox: &MailboxRef<()>) {
        assert!(
            matches!(&*core.current.borrow(), BindingState::Bound(current) if current.same_channel(mailbox))
        );
    }

    fn bind_for_test(core: &BindingCore<()>, mailbox: MailboxRef<()>) {
        let monitor_run = core.monitor_run();
        assert!(core.bind(mailbox, &monitor_run).is_some());
    }

    /// A cancelled incarnation can drop its `BindingGuard` after a replacement
    /// has already bound. Teardown must then find its own mailbox gone and
    /// leave the replacement alone.
    #[test]
    fn unbind_ignores_a_mailbox_that_is_no_longer_bound() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (cancelled, _cancelled_rx) = incarnation_mailbox(&actor_id);
        let (replacement, _replacement_rx) = incarnation_mailbox(&actor_id);

        bind_for_test(&core, cancelled.clone());
        bind_for_test(&core, replacement.clone());

        assert!(!core.unbind_mailbox(&cancelled));
        assert_bound_to(&core, &replacement);

        assert!(core.unbind_mailbox(&replacement));
        assert!(matches!(&*core.current.borrow(), BindingState::Unbound));
    }

    /// The same race under [`RestartPolicy::never`], where a late teardown would
    /// otherwise make a live replacement binding permanently terminal.
    #[test]
    fn terminate_ignores_a_mailbox_that_is_no_longer_bound() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (cancelled, _cancelled_rx) = incarnation_mailbox(&actor_id);
        let (replacement, _replacement_rx) = incarnation_mailbox(&actor_id);

        bind_for_test(&core, cancelled.clone());
        bind_for_test(&core, replacement.clone());

        assert!(!core.terminate_mailbox(&cancelled));
        assert_bound_to(&core, &replacement);

        assert!(core.terminate_mailbox(&replacement));
        assert!(matches!(&*core.current.borrow(), BindingState::Terminated));
    }

    /// Teardown of the last incarnation is also a no-op once the owning host
    /// has already terminated the binding, so a terminal state never regresses.
    #[test]
    fn teardown_does_not_regress_a_terminated_binding() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (cancelled, _cancelled_rx) = incarnation_mailbox(&actor_id);

        bind_for_test(&core, cancelled.clone());
        core.terminate();

        assert!(!core.unbind_mailbox(&cancelled));
        assert!(matches!(&*core.current.borrow(), BindingState::Terminated));
    }

    #[test]
    fn terminal_binding_rejects_a_late_run() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (late, _late_rx) = incarnation_mailbox(&actor_id);
        let monitor_run = core.monitor_run();
        core.terminate();

        assert!(core.bind(late, &monitor_run).is_none());
        assert!(matches!(&*core.current.borrow(), BindingState::Terminated));
    }

    #[test]
    fn new_run_after_terminal_teardown_can_reopen_the_binding() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (replacement, _replacement_rx) = incarnation_mailbox(&actor_id);
        let subject = Arc::new(MonitorHub::new("peer"));
        let outbound = core.outbound_monitors();
        let old_lease = outbound.lease();
        let (_, old_stop, _, installed) = old_lease.register(&subject);
        assert!(installed);
        core.terminate();
        assert!(old_stop.is_cancelled());

        let monitor_run = core.monitor_run();
        let replacement_lease = core
            .bind(replacement.clone(), &monitor_run)
            .expect("a new run reopens the terminated binding");
        assert_bound_to(&core, &replacement);
        let (_, _, stale_finished, installed) = old_lease.register(&subject);
        assert!(!installed);
        assert!(stale_finished.token().is_cancelled());
        let (_, replacement_stop, _, installed) = replacement_lease.register(&subject);
        assert!(installed);
        assert!(!replacement_stop.is_cancelled());
    }

    #[test]
    fn late_older_run_cannot_replace_a_newer_binding() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = BindingCore::new(Arc::clone(&actor_id));
        let (older, _older_rx) = incarnation_mailbox(&actor_id);
        let (newer, _newer_rx) = incarnation_mailbox(&actor_id);
        let older_run = core.monitor_run();
        let newer_run = core.monitor_run();

        assert!(core.bind(newer.clone(), &newer_run).is_some());
        assert!(core.bind(older, &older_run).is_none());
        assert_bound_to(&core, &newer);
    }
}
