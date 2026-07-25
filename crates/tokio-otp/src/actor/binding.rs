use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::sync::{Notify, mpsc, watch};
use tokio_supervisor::RestartPolicy;
use tokio_util::sync::CancellationToken;

use crate::actor::{
    error::SendError,
    monitor::{ActorMonitors, MonitorHub},
    observability::{GraphObservability, MessageSizeMetrics},
};

/// A point-in-time snapshot of one actor's message and mailbox statistics.
///
/// Message counters accumulate for the lifetime of the actor binding and
/// therefore survive restarts. Mailbox fields describe the currently bound
/// incarnation and are zero while no mailbox is bound.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ActorStats {
    /// Actor id used to correlate these stats with supervisor snapshots.
    pub actor_id: String,
    /// Identity path of the supervisors containing this actor when sampled
    /// through [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats).
    ///
    /// A direct child of the sampled runtime has an empty path. Each nested
    /// segment includes the supervisor child's membership epoch and generation
    /// so identical actor ids and local epochs in sibling or restarted
    /// subtrees remain distinguishable. Standalone actor and graph stats have
    /// no supervisor context and report `None`.
    pub supervisor_path: Option<Vec<SupervisorPathSegment>>,
    /// Identity of the actor's current supervisor membership, when sampled
    /// through [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats).
    ///
    /// Pair this with [`actor_id`](Self::actor_id) and
    /// [`supervisor_path`](Self::supervisor_path) to distinguish a removed
    /// actor from a later actor added under the same id, including actors with
    /// identical local identities in different subtrees. Standalone actor and
    /// graph stats have no supervisor membership and report `None`.
    pub membership_epoch: Option<u64>,
    /// Messages removed from the mailbox by the actor for handling.
    ///
    /// This can be lower than [`messages_accepted`](Self::messages_accepted):
    /// accepted messages may be conflated before the actor receives them or
    /// discarded when an incarnation stops, and a queued state timeout is
    /// suppressed if its slot is replaced or cleared before receipt.
    pub messages_received: u64,
    /// Messages accepted into the mailbox by `send` or `try_send`.
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
    pub message_bytes_accepted: Option<u64>,
    /// `send` or `try_send` calls that returned an error.
    pub sends_rejected: u64,
    /// Steps currently owned by this actor incarnation.
    ///
    /// This is a gauge rather than a lifetime counter. It returns to zero
    /// when steps finish, time out, or are aborted.
    pub outstanding_steps: u64,
    /// Messages currently occupying the bound mailbox.
    pub mailbox_depth: usize,
    /// Maximum capacity of the currently bound mailbox.
    pub mailbox_capacity: usize,
}

/// Identity of one nested supervisor on an actor stats path.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SupervisorPathSegment {
    /// Child id under which the nested supervisor is mounted.
    pub id: String,
    /// Membership identity of that child in its parent supervisor.
    pub membership_epoch: u64,
    /// Current generation of the nested supervisor child.
    pub generation: u64,
}

#[derive(Debug)]
pub(crate) struct ActorStatsCounters {
    messages_received: AtomicU64,
    messages_accepted: AtomicU64,
    messages_conflated: AtomicU64,
    sends_rejected: AtomicU64,
    message_bytes_accepted: Option<AtomicU64>,
}

impl ActorStatsCounters {
    pub(crate) fn new(observe_message_size: bool) -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_accepted: AtomicU64::new(0),
            messages_conflated: AtomicU64::new(0),
            sends_rejected: AtomicU64::new(0),
            message_bytes_accepted: observe_message_size.then(|| AtomicU64::new(0)),
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

    pub(crate) fn snapshot(
        &self,
        actor_id: &str,
        outstanding_steps: u64,
        mailbox_depth: usize,
        mailbox_capacity: usize,
    ) -> ActorStats {
        ActorStats {
            actor_id: actor_id.to_owned(),
            supervisor_path: None,
            membership_epoch: None,
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_accepted: self.messages_accepted.load(Ordering::Relaxed),
            messages_conflated: self.messages_conflated.load(Ordering::Relaxed),
            message_bytes_accepted: self
                .message_bytes_accepted
                .as_ref()
                .map(|total| total.load(Ordering::Relaxed)),
            sends_rejected: self.sends_rejected.load(Ordering::Relaxed),
            outstanding_steps,
            mailbox_depth,
            mailbox_capacity,
        }
    }
}

/// Selects how an actor stores unread messages.
///
/// FIFO [`Queue`](Self::Queue) mailboxes apply backpressure at the configured
/// capacity. Conflating mailboxes never wait for capacity: they replace stale
/// unread state and are intended for idempotent snapshots, not commands.
#[derive(Default)]
#[non_exhaustive]
pub enum MailboxMode<M> {
    /// A bounded FIFO queue. This is the default.
    #[default]
    Queue,
    /// One latest-wins slot for the whole mailbox.
    ///
    /// This mode always has capacity 1; the graph's mailbox capacity setting
    /// does not apply.
    ///
    /// Sending never waits for capacity. Awaited
    /// [`ActorRef::send`](crate::ActorRef::send) calls consume Tokio's
    /// cooperative task budget so tight producer loops remain fair.
    Conflate,
    /// One latest-wins slot per key, bounded by the mailbox capacity.
    ///
    /// When the capacity is already occupied by distinct keys, a message for
    /// a new key evicts the oldest unread key. Construct this variant with
    /// [`conflate_by_key`](Self::conflate_by_key).
    ///
    /// Sending never waits for capacity. Awaited
    /// [`ActorRef::send`](crate::ActorRef::send) calls consume Tokio's
    /// cooperative task budget so tight producer loops remain fair.
    #[non_exhaustive]
    ConflateByKey {
        #[doc(hidden)]
        key_matches: MailboxKeyMatcher<M>,
    },
}

type KeyMatcherFn<M> = dyn Fn(&M, &M) -> bool + Send + Sync;

#[doc(hidden)]
pub struct MailboxKeyMatcher<M>(Arc<KeyMatcherFn<M>>);

impl<M> MailboxMode<M> {
    /// Creates a keyed latest-wins mailbox using `key` to group messages.
    ///
    /// Each send scans at most the configured mailbox capacity and may call
    /// `key` for both the incoming and each queued message. Keep extraction
    /// cheap and prefer clone-free keys such as numeric or interned ids.
    pub fn conflate_by_key<K, F>(key: F) -> Self
    where
        K: Eq,
        F: Fn(&M) -> K + Send + Sync + 'static,
    {
        Self::ConflateByKey {
            key_matches: MailboxKeyMatcher(Arc::new(move |left, right| key(left) == key(right))),
        }
    }
}

impl<M> Clone for MailboxMode<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Queue => Self::Queue,
            Self::Conflate => Self::Conflate,
            Self::ConflateByKey { key_matches } => Self::ConflateByKey {
                key_matches: MailboxKeyMatcher(Arc::clone(&key_matches.0)),
            },
        }
    }
}

impl<M> fmt::Debug for MailboxMode<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue => f.write_str("Queue"),
            Self::Conflate => f.write_str("Conflate"),
            Self::ConflateByKey { .. } => f.write_str("ConflateByKey"),
        }
    }
}

pub(crate) enum MailboxReceiver<M> {
    Queue {
        receiver: mpsc::Receiver<MailboxMessage<M>>,
        accepting_external: Arc<AtomicBool>,
    },
    Conflating(ConflatingReceiver<M>),
}

impl<M> MailboxReceiver<M> {
    pub(crate) async fn recv(&mut self) -> Option<M> {
        loop {
            let message = match self {
                Self::Queue { receiver, .. } => receiver.recv().await,
                Self::Conflating(receiver) => receiver.recv().await,
            }?;
            if let Some(message) = message.into_current() {
                return Some(message);
            }
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<M, mpsc::error::TryRecvError> {
        loop {
            let message = match self {
                Self::Queue { receiver, .. } => receiver.try_recv(),
                Self::Conflating(receiver) => receiver.try_recv(),
            }?;
            if let Some(message) = message.into_current() {
                return Ok(message);
            }
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
    match mode {
        MailboxMode::Queue => {
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
        MailboxMode::Conflate => {
            let (sender, receiver) = conflating_channel(1, None);
            (
                MailboxSender::Conflating(sender),
                MailboxReceiver::Conflating(receiver),
            )
        }
        MailboxMode::ConflateByKey { key_matches } => {
            let (sender, receiver) = conflating_channel(capacity, Some(Arc::clone(&key_matches.0)));
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

impl<M> SendOutcome<M> {
    fn map_closed<T>(self, map: impl FnOnce(M) -> T) -> SendOutcome<T> {
        match self {
            Self::Accepted { conflated } => SendOutcome::Accepted { conflated },
            Self::Closed(message) => SendOutcome::Closed(map(message)),
        }
    }
}

pub(crate) enum MailboxMessage<M> {
    Message(M),
    StateTimeout {
        message: M,
        cancellation: CancellationToken,
    },
}

impl<M> MailboxMessage<M> {
    fn message(&self) -> &M {
        match self {
            Self::Message(message) | Self::StateTimeout { message, .. } => message,
        }
    }

    fn into_message(self) -> M {
        match self {
            Self::Message(message) | Self::StateTimeout { message, .. } => message,
        }
    }

    fn into_current(self) -> Option<M> {
        match self {
            Self::Message(message) => Some(message),
            Self::StateTimeout {
                message,
                cancellation,
            } => (!cancellation.is_cancelled()).then_some(message),
        }
    }
}

/// Sender for one bound mailbox instance of an actor.
pub(crate) struct MailboxRef<M> {
    actor_id: Arc<str>,
    sender: MailboxSender<M>,
    outstanding_steps: Arc<AtomicU64>,
}

impl<M> Clone for MailboxRef<M> {
    fn clone(&self) -> Self {
        Self {
            actor_id: Arc::clone(&self.actor_id),
            sender: self.sender.clone(),
            outstanding_steps: Arc::clone(&self.outstanding_steps),
        }
    }
}

impl<M> MailboxRef<M> {
    pub(crate) fn new(
        actor_id: Arc<str>,
        sender: MailboxSender<M>,
        outstanding_steps: Arc<AtomicU64>,
    ) -> Self {
        Self {
            actor_id,
            sender,
            outstanding_steps,
        }
    }

    /// Sends, returning the message on failure so callers can retry after a
    /// rebind.
    pub(crate) async fn send_retaining(&self, message: M) -> SendOutcome<M> {
        self.send_mailbox_message_retaining(MailboxMessage::Message(message), false)
            .await
            .map_closed(MailboxMessage::into_message)
    }

    pub(crate) async fn send_state_timeout_retaining(
        &self,
        message: M,
        cancellation: CancellationToken,
    ) -> SendOutcome<M> {
        self.send_mailbox_message_retaining(
            MailboxMessage::StateTimeout {
                message,
                cancellation,
            },
            false,
        )
        .await
        .map_closed(MailboxMessage::into_message)
    }

    async fn send_mailbox_message_retaining(
        &self,
        message: MailboxMessage<M>,
        internal: bool,
    ) -> SendOutcome<MailboxMessage<M>> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !internal && !accepting_external.load(Ordering::Acquire) {
                    return SendOutcome::Closed(message);
                }
                match sender.reserve().await {
                    // This narrows the close race after reserving capacity;
                    // close_external is an intake signal, not a linearizable
                    // fence against a sender already in this operation.
                    Ok(permit) if internal || accepting_external.load(Ordering::Acquire) => {
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
                sender.send(message, internal)
            }
        }
    }

    pub(crate) async fn send_internal_retaining(&self, message: M) -> SendOutcome<M> {
        self.send_mailbox_message_retaining(MailboxMessage::Message(message), true)
            .await
            .map_closed(MailboxMessage::into_message)
    }

    pub(crate) fn try_send(&self, message: M) -> Result<u64, SendError> {
        match &self.sender {
            MailboxSender::Queue {
                sender,
                accepting_external,
            } => {
                if !accepting_external.load(Ordering::Acquire) {
                    return Err(SendError::MailboxClosed {
                        actor_id: self.actor_id.to_string(),
                    });
                }
                match sender.try_reserve() {
                    Ok(permit) if accepting_external.load(Ordering::Acquire) => {
                        permit.send(MailboxMessage::Message(message));
                        Ok(0)
                    }
                    Ok(_) | Err(mpsc::error::TrySendError::Closed(_)) => {
                        Err(SendError::MailboxClosed {
                            actor_id: self.actor_id.to_string(),
                        })
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => Err(SendError::MailboxFull {
                        actor_id: self.actor_id.to_string(),
                    }),
                }
            }
            MailboxSender::Conflating(sender) => {
                match sender.send(MailboxMessage::Message(message), false) {
                    SendOutcome::Accepted { conflated } => Ok(conflated),
                    SendOutcome::Closed(_) => Err(SendError::MailboxClosed {
                        actor_id: self.actor_id.to_string(),
                    }),
                }
            }
        }
    }

    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }

    pub(crate) fn usage(&self) -> (usize, usize) {
        self.sender.usage()
    }

    pub(crate) fn outstanding_steps(&self) -> u64 {
        self.outstanding_steps.load(Ordering::Relaxed)
    }
}

pub(crate) enum MailboxSender<M> {
    Queue {
        sender: mpsc::Sender<MailboxMessage<M>>,
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

type KeyMatcher<M> = Arc<dyn Fn(&M, &M) -> bool + Send + Sync>;

struct ConflatingState<M> {
    messages: VecDeque<MailboxMessage<M>>,
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
    fn send(&self, message: MailboxMessage<M>, internal: bool) -> SendOutcome<MailboxMessage<M>> {
        let mut state = self.shared.lock();
        if state.receiver_closed || (!internal && !state.accepting_external) {
            return SendOutcome::Closed(message);
        }

        let conflated = if let Some(key_matches) = &state.key_matches {
            if let Some(index) = state
                .messages
                .iter()
                .position(|queued| key_matches(queued.message(), message.message()))
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
    async fn recv(&mut self) -> Option<MailboxMessage<M>> {
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

    fn try_recv(&mut self) -> Result<MailboxMessage<M>, mpsc::error::TryRecvError> {
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
        let (outstanding_steps, depth, capacity) = match &*state {
            BindingState::Bound(mailbox) => {
                let (depth, capacity) = mailbox.usage();
                (mailbox.outstanding_steps(), depth, capacity)
            }
            BindingState::Unbound | BindingState::Terminated => (0, 0, 0),
        };
        self.stats
            .snapshot(&self.actor_id, outstanding_steps, depth, capacity)
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
    observability: GraphObservability,
    restart_policy: RestartPolicy,
}

impl<M> BindingGuard<M> {
    pub(crate) fn bind(
        core: Arc<BindingCore<M>>,
        mailbox: MailboxRef<M>,
        observability: GraphObservability,
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
