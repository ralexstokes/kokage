use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use kokage_supervisor::{CancellationToken, RestartPolicy};
use tokio::sync::{Notify, mpsc, watch};

use crate::actor::{
    error::TrySendError,
    monitor::{ActorMonitors, MonitorHub},
    observability::{MessageSizeMetrics, ScopeObservability},
};

/// A point-in-time snapshot of one actor's message and mailbox statistics.
///
/// Sample these stats either through [`ActorRef::stats`](crate::ActorRef::stats)
/// or [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats).
/// Message counters accumulate for the lifetime of the actor binding and
/// therefore survive restarts. Outstanding-work gauges and mailbox fields
/// describe the currently bound incarnation and are zero while no mailbox is
/// bound. Enabling the `serde` feature implements `Serialize` and
/// `Deserialize` for this type and [`SupervisorPathSegment`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ActorStats {
    /// Actor id used to correlate these stats with supervisor snapshots.
    pub actor_id: String,
    /// Identity path of the supervisors containing this actor when sampled
    /// through [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats).
    ///
    /// A direct child of the sampled runtime has an empty path. Each nested
    /// segment includes the supervisor child's lineage and generation
    /// so identical actor ids and local lineages in sibling or restarted
    /// subtrees remain distinguishable. Samples taken from `ActorRef::stats`
    /// have no supervisor context and report `None`. The element type is the
    /// public [`SupervisorPathSegment`].
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub supervisor_path: Option<Vec<SupervisorPathSegment>>,
    /// Identity of the actor's current supervisor membership, when sampled
    /// through [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats).
    ///
    /// Pair this with [`actor_id`](Self::actor_id) and
    /// [`supervisor_path`](Self::supervisor_path) to distinguish a removed
    /// actor from a later actor added under the same id, including actors with
    /// identical local identities in different subtrees. Samples taken from
    /// `ActorRef::stats` have no supervisor membership and report `None`.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub lineage: Option<u64>,
    /// Messages delivered to the actor for handling.
    ///
    /// This includes actor-local continuations, timer events, and offload
    /// completions, which bypass the mailbox. It can also be lower than
    /// [`messages_accepted`](Self::messages_accepted): accepted messages may be
    /// conflated before receipt or discarded when an incarnation stops.
    pub messages_received: u64,
    /// Messages accepted into the mailbox by `send` or `try_send`, including
    /// mapped lifecycle-wait results delivered through the ordinary mailbox.
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
    /// `None` means the actor did not opt in. The total accumulates across
    /// actor restarts, like the message counters.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub message_bytes_accepted: Option<u64>,
    /// Mailbox sends rejected before acceptance.
    ///
    /// This includes `send` or `try_send` calls that returned an error and a
    /// mapped lifecycle-wait result rejected because its captured incarnation
    /// mailbox had closed.
    pub sends_rejected: u64,
    /// Offloads currently owned by this actor incarnation.
    ///
    /// This is a gauge rather than a lifetime counter. It returns to zero
    /// when offloads finish, time out, or are aborted.
    pub outstanding_offloads: u64,
    /// Lifecycle waits currently owned by this actor incarnation.
    ///
    /// This is a gauge rather than a lifetime counter. It returns to zero
    /// when waits finish, are cancelled, or the incarnation ends.
    pub outstanding_scope_waits: u64,
    /// Messages currently occupying the bound mailbox, or zero when sampled
    /// while the actor has no bound incarnation.
    pub mailbox_depth: usize,
    /// Maximum capacity of the currently bound mailbox, or zero when sampled
    /// while the actor has no bound incarnation.
    pub mailbox_capacity: usize,
}

/// Identity of one nested supervisor on an actor stats path.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct SupervisorPathSegment {
    /// Child id under which the nested supervisor is mounted.
    pub id: String,
    /// Membership identity of that child in its parent supervisor.
    pub lineage: u64,
    /// Current generation of the nested supervisor child.
    pub generation: u64,
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use super::{ActorStats, SupervisorPathSegment};
    use serde_json::json;

    #[test]
    fn actor_stats_round_trip_with_supervisor_identity() {
        let stats = ActorStats {
            actor_id: "worker".into(),
            supervisor_path: Some(vec![SupervisorPathSegment {
                id: "workers".into(),
                lineage: 7,
                generation: 2,
            }]),
            lineage: Some(11),
            messages_received: 13,
            messages_accepted: 17,
            messages_conflated: 3,
            message_bytes_accepted: Some(1_024),
            sends_rejected: 1,
            outstanding_offloads: 2,
            outstanding_scope_waits: 4,
            mailbox_depth: 5,
            mailbox_capacity: 32,
        };

        let value = serde_json::to_value(&stats).expect("actor stats serialize");
        assert_eq!(
            value["supervisor_path"],
            json!([{"id": "workers", "lineage": 7, "generation": 2}])
        );
        let decoded: ActorStats = serde_json::from_value(value).expect("actor stats deserialize");
        assert_eq!(decoded, stats);
    }

    #[test]
    fn actor_stats_omit_absent_optional_fields() {
        let stats = ActorStats {
            actor_id: "worker".into(),
            supervisor_path: None,
            lineage: None,
            messages_received: 0,
            messages_accepted: 0,
            messages_conflated: 0,
            message_bytes_accepted: None,
            sends_rejected: 0,
            outstanding_offloads: 0,
            outstanding_scope_waits: 0,
            mailbox_depth: 0,
            mailbox_capacity: 0,
        };

        let value = serde_json::to_value(stats).expect("actor stats serialize");
        assert!(value.get("supervisor_path").is_none());
        assert!(value.get("lineage").is_none());
        assert!(value.get("message_bytes_accepted").is_none());
    }
}

#[derive(Debug)]
pub(crate) struct ActorStatsCounters {
    messages_received: AtomicU64,
    messages_accepted: AtomicU64,
    messages_conflated: AtomicU64,
    sends_rejected: AtomicU64,
    message_bytes_accepted: Option<AtomicU64>,
    outstanding_offloads: AtomicU64,
    outstanding_scope_waits: AtomicU64,
}

impl ActorStatsCounters {
    pub(crate) fn new(observe_message_size: bool) -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_accepted: AtomicU64::new(0),
            messages_conflated: AtomicU64::new(0),
            sends_rejected: AtomicU64::new(0),
            message_bytes_accepted: observe_message_size.then(|| AtomicU64::new(0)),
            outstanding_offloads: AtomicU64::new(0),
            outstanding_scope_waits: AtomicU64::new(0),
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
        if let Some(total) = &self.message_bytes_accepted {
            total.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn set_outstanding_offloads(&self, outstanding: usize) {
        self.outstanding_offloads
            .store(outstanding as u64, Ordering::Relaxed);
    }

    pub(crate) fn set_outstanding_scope_waits(&self, outstanding: usize) {
        self.outstanding_scope_waits
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
            supervisor_path: None,
            lineage: None,
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_accepted: self.messages_accepted.load(Ordering::Relaxed),
            messages_conflated: self.messages_conflated.load(Ordering::Relaxed),
            message_bytes_accepted: self
                .message_bytes_accepted
                .as_ref()
                .map(|total| total.load(Ordering::Relaxed)),
            sends_rejected: self.sends_rejected.load(Ordering::Relaxed),
            outstanding_offloads: self.outstanding_offloads.load(Ordering::Relaxed),
            outstanding_scope_waits: self.outstanding_scope_waits.load(Ordering::Relaxed),
            mailbox_depth,
            mailbox_capacity,
        }
    }
}

type KeyMatcher<M> = Arc<dyn Fn(&M, &M) -> bool + Send + Sync>;

/// Selects how an actor stores unread messages.
///
/// FIFO [`queue`](Self::queue) mailboxes apply backpressure at the configured
/// capacity. Conflating mailboxes never wait for capacity: they replace stale
/// unread state and are intended for idempotent snapshots, not commands.
#[non_exhaustive]
pub struct MailboxMode<M> {
    kind: MailboxKind<M>,
}

enum MailboxKind<M> {
    Queue,
    Conflate,
    ConflateByKey { key_matches: KeyMatcher<M> },
}

impl<M> Default for MailboxMode<M> {
    fn default() -> Self {
        Self::queue()
    }
}

impl<M> MailboxMode<M> {
    /// Creates a bounded FIFO queue. This is the default.
    pub fn queue() -> Self {
        Self {
            kind: MailboxKind::Queue,
        }
    }

    /// One latest-wins slot for the whole mailbox.
    ///
    /// This mode always has capacity 1; the declaration's mailbox capacity
    /// does not apply.
    ///
    /// Sending never waits for capacity. Awaited
    /// [`ActorRef::send`](crate::ActorRef::send) calls consume Tokio's
    /// cooperative task budget so tight producer loops remain fair.
    pub fn conflate() -> Self {
        Self {
            kind: MailboxKind::Conflate,
        }
    }

    /// Creates a keyed latest-wins mailbox using `key` to group messages.
    ///
    /// The mailbox stores one latest unread message per key, bounded by its
    /// configured capacity. When that capacity is already occupied by
    /// distinct keys, a message for a new key evicts the oldest unread key.
    /// Sending never waits for capacity.
    ///
    /// Each send scans at most the configured mailbox capacity and may call
    /// `key` for both the incoming and each queued message. Keep extraction
    /// cheap and prefer clone-free keys such as numeric or interned ids.
    pub fn conflate_by_key<K, F>(key: F) -> Self
    where
        K: Eq,
        F: Fn(&M) -> K + Send + Sync + 'static,
    {
        Self {
            kind: MailboxKind::ConflateByKey {
                key_matches: Arc::new(move |left, right| key(left) == key(right)),
            },
        }
    }
}

impl<M> Clone for MailboxMode<M> {
    fn clone(&self) -> Self {
        let kind = match &self.kind {
            MailboxKind::Queue => MailboxKind::Queue,
            MailboxKind::Conflate => MailboxKind::Conflate,
            MailboxKind::ConflateByKey { key_matches } => MailboxKind::ConflateByKey {
                key_matches: Arc::clone(key_matches),
            },
        };
        Self { kind }
    }
}

impl<M> fmt::Debug for MailboxMode<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            MailboxKind::Queue => f.write_str("Queue"),
            MailboxKind::Conflate => f.write_str("Conflate"),
            MailboxKind::ConflateByKey { .. } => f.write_str("ConflateByKey"),
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
    mode: &MailboxMode<M>,
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
        MailboxKind::Conflate => {
            let (sender, receiver) = conflating_channel(1, None);
            (
                MailboxSender::Conflating(sender),
                MailboxReceiver::Conflating(receiver),
            )
        }
        MailboxKind::ConflateByKey { key_matches } => {
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

pub(crate) enum GatedSendOutcome<M> {
    Accepted { conflated: u64 },
    Closed(M),
    Cancelled(M),
}

#[derive(Debug)]
pub(crate) struct SendGate {
    cancellation: CancellationToken,
    state: Mutex<SendGateState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendGateState {
    Pending,
    Accepted,
    Cancelled,
    Finished,
}

impl SendGate {
    pub(crate) fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            state: Mutex::new(SendGateState::Pending),
        }
    }

    pub(crate) async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Cancels delivery if mailbox acceptance has not already committed.
    ///
    /// Returns whether aborting the task is still safe. Once acceptance wins,
    /// the task must finish its send accounting rather than be aborted between
    /// mailbox commit and counter updates.
    pub(crate) fn cancel(&self) -> bool {
        let should_abort = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match *state {
                SendGateState::Pending => {
                    *state = SendGateState::Cancelled;
                    true
                }
                SendGateState::Cancelled => true,
                SendGateState::Accepted | SendGateState::Finished => false,
            }
        };
        if should_abort {
            self.cancellation.cancel();
        }
        should_abort
    }

    fn commit<M>(&self, message: M, send: impl FnOnce(M) -> SendOutcome<M>) -> GatedSendOutcome<M> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match *state {
            SendGateState::Pending => match send(message) {
                SendOutcome::Accepted { conflated } => {
                    *state = SendGateState::Accepted;
                    GatedSendOutcome::Accepted { conflated }
                }
                SendOutcome::Closed(message) => {
                    *state = SendGateState::Finished;
                    GatedSendOutcome::Closed(message)
                }
            },
            SendGateState::Cancelled => GatedSendOutcome::Cancelled(message),
            SendGateState::Accepted | SendGateState::Finished => {
                unreachable!("a scope-wait send gate commits at most once")
            }
        }
    }
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

    /// Sends only if `gate` has not been cancelled at the acceptance point.
    pub(crate) async fn send_retaining_gated(
        &self,
        message: M,
        gate: &SendGate,
    ) -> GatedSendOutcome<M> {
        if gate.is_cancelled() {
            return GatedSendOutcome::Cancelled(message);
        }
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return GatedSendOutcome::Closed(message);
                }
                let reserved = tokio::select! {
                    biased;
                    () = gate.cancelled() => return GatedSendOutcome::Cancelled(message),
                    reserved = sender.reserve() => reserved,
                };
                match reserved {
                    Ok(permit) => gate.commit(message, |message| {
                        if accepting_external.load(Ordering::Acquire) {
                            permit.send(message);
                            SendOutcome::Accepted { conflated: 0 }
                        } else {
                            SendOutcome::Closed(message)
                        }
                    }),
                    Err(_) if gate.is_cancelled() => GatedSendOutcome::Cancelled(message),
                    Err(_) => GatedSendOutcome::Closed(message),
                }
            }
            MailboxSender::Conflating(sender) => {
                tokio::task::coop::consume_budget().await;
                sender.send_gated(message, gate)
            }
        }
    }

    pub(crate) fn try_send(&self, message: M) -> Result<u64, TrySendError> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return Err(TrySendError::Closed {
                        actor_id: self.actor_id.to_string(),
                    });
                }
                match sender.try_reserve() {
                    Ok(permit) if accepting_external.load(Ordering::Acquire) => {
                        permit.send(message);
                        Ok(0)
                    }
                    Ok(_) | Err(mpsc::error::TrySendError::Closed(_)) => {
                        Err(TrySendError::Closed {
                            actor_id: self.actor_id.to_string(),
                        })
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => Err(TrySendError::Full {
                        actor_id: self.actor_id.to_string(),
                    }),
                }
            }
            MailboxSender::Conflating(sender) => match sender.send(message) {
                SendOutcome::Accepted { conflated } => Ok(conflated),
                SendOutcome::Closed(_) => Err(TrySendError::Closed {
                    actor_id: self.actor_id.to_string(),
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

    fn send_gated(&self, message: M, gate: &SendGate) -> GatedSendOutcome<M> {
        let mut state = self.shared.lock();
        if state.receiver_closed || !state.accepting_external {
            return GatedSendOutcome::Closed(message);
        }

        // Key extraction is user code. Run it before entering the acceptance
        // gate so a slow or panicking matcher cannot hold up cancellation.
        // The conflating-state lock keeps the computed position stable.
        let matched = state.key_matches.as_ref().and_then(|key_matches| {
            state
                .messages
                .iter()
                .position(|queued| key_matches(queued, &message))
        });
        let keyed = state.key_matches.is_some();
        let outcome = gate.commit(message, |message| {
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
            SendOutcome::Accepted { conflated }
        });
        drop(state);
        if matches!(outcome, GatedSendOutcome::Accepted { .. }) {
            self.shared.notify.notify_one();
        }
        outcome
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
    fn unbind(&self);
    fn terminate(&self);
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
    message_size: Option<Arc<MessageSizeObserver<M>>>,
    monitors: Arc<MonitorHub>,
    outbound_monitors: Arc<ActorMonitors>,
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
            stats: Arc::new(ActorStatsCounters::new(false)),
            message_size: None,
            monitors,
            outbound_monitors,
        }
    }

    pub(crate) fn with_message_size(actor_id: Arc<str>, size_hint: fn(&M) -> usize) -> Self {
        let (current, _receiver) = watch::channel(BindingState::Unbound);
        let monitors = Arc::new(MonitorHub::new(&actor_id));
        let outbound_monitors = Arc::new(ActorMonitors::new());
        let message_size = MessageSizeObserver {
            size_hint,
            metrics: MessageSizeMetrics::new(&actor_id),
        };
        Self {
            identity: Arc::new(()),
            actor_id,
            current,
            stats: Arc::new(ActorStatsCounters::new(true)),
            message_size: Some(Arc::new(message_size)),
            monitors,
            outbound_monitors,
        }
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

    pub(crate) fn message_size(&self) -> Option<Arc<MessageSizeObserver<M>>> {
        self.message_size.clone()
    }

    pub(crate) fn monitor_hub(&self) -> Arc<MonitorHub> {
        Arc::clone(&self.monitors)
    }

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

    fn bind(&self, mailbox: MailboxRef<M>) {
        self.monitors.started();
        self.current.send_replace(BindingState::Bound(mailbox));
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

    pub(crate) fn terminate(&self) {
        self.current.send_replace(BindingState::Terminated);
        self.monitors.terminated();
        self.outbound_monitors.terminate();
    }
}

impl<M: Send + 'static> BindingLifecycle for BindingCore<M> {
    fn unbind(&self) {
        BindingCore::unbind(self);
    }

    fn terminate(&self) {
        BindingCore::terminate(self);
    }

    fn stats(&self) -> ActorStats {
        BindingCore::stats(self)
    }
}

impl<M> Drop for BindingCore<M> {
    fn drop(&mut self) {
        self.monitors.terminated();
        self.outbound_monitors.terminate();
    }
}

/// Binds a mailbox on creation and clears the binding when the actor's run
/// ends.
pub(crate) struct BindingGuard<M> {
    core: Arc<BindingCore<M>>,
    observability: ScopeObservability,
    restart_policy: RestartPolicy,
}

impl<M> BindingGuard<M> {
    pub(crate) fn bind(
        core: Arc<BindingCore<M>>,
        mailbox: MailboxRef<M>,
        observability: ScopeObservability,
        restart_policy: RestartPolicy,
    ) -> Self {
        core.bind(mailbox);
        observability.emit_mailbox_bound(core.actor_id());
        Self {
            core,
            observability,
            restart_policy,
        }
    }
}

impl<M> Drop for BindingGuard<M> {
    fn drop(&mut self) {
        if matches!(self.restart_policy, RestartPolicy::Never) {
            self.core.terminate();
        } else {
            // A dropped run is a failure for restart purposes. Unknown future
            // policies remain rebindable until `run_disposition` can make the
            // final decision from the observed exit status.
            self.core.unbind();
        }
        self.observability
            .emit_mailbox_cleared(self.core.actor_id());
    }
}
