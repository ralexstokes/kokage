use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{oneshot, watch},
    task::{AbortHandle, Id as TaskId, JoinError, JoinSet},
    time::{Instant, MissedTickBehavior, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::RuntimeHandle;

use crate::actor::{
    binding::{
        ActorStats, ActorStatsCounters, BindingCore, BindingState, GatedSendOutcome,
        MailboxReceiver, MailboxRef, MessageSizeObserver, SendGate, SendOutcome,
    },
    cancellation::CancellationHandle,
    error::{BlockingCancelled, CallError, OffloadDeadline, SendError, TrySendError},
    handler::Actor,
    monitor::{ActorMonitors, MonitorEvent, MonitorHub},
    observability::{GraphObservability, MessageOperation, SendRejection, trace_actor_message},
};

macro_rules! ambient_context_method {
    (id, $item:item) => {
        /// Returns the actor's unique identifier within the graph.
        $item
    };
    (myself $([$note:literal])?, $item:item) => {
        /// Returns a sender targeting this actor's own mailbox.
        $(
            #[doc = ""]
            #[doc = $note]
        )?
        $item
    };
    (shutdown_token, $item:item) => {
        /// Returns the shared graph shutdown token.
        $item
    };
    (run_blocking, $item:item) => {
        /// Runs blocking work on Tokio's blocking pool.
        ///
        /// See [`ActorContext::run_blocking`].
        $item
    };
}

macro_rules! scope_context_methods {
    (actor) => {
        /// Returns this actor's enclosing scope with lifecycle waits withheld.
        ///
        /// Awaiting scope or child lifecycle progress can deadlock when that
        /// progress depends on this actor returning from its current work.
        /// During live stages, use [`LiveContext::spawn_scope_wait`] to run a
        /// lifecycle wait as incarnation-owned work and map its result back
        /// through the mailbox.
        ///
        /// Actors run directly through
        /// [`RunnableActor::run_until`](crate::host::RunnableActor::run_until),
        /// outside a supervisor, receive a terminal handle here. Its control
        /// operations return
        /// [`ControlError::Unavailable`](crate::ControlError::Unavailable)
        /// and its observation streams are closed.
        pub fn supervisor(&self) -> RestrictedScope {
            RestrictedScope::new(self.supervisor.clone())
        }

        /// Returns the actor-aware handle for this leader's declared child
        /// scope.
        ///
        /// This is `Some` exactly for the leader of an
        /// [`actor_with_scope`](crate::OrderedTree::actor_with_scope) node.
        /// The child scope starts only after its leader's `on_start` returns,
        /// so pipeline readiness waits with
        /// [`LiveContext::spawn_scope_wait`] instead of awaiting them inline.
        pub fn children(&self) -> Option<RestrictedScope> {
            self.children.clone().map(RestrictedScope::new)
        }
    };
    (start) => {
        /// Returns this actor's enclosing scope, restricted for the startup
        /// stage.
        ///
        /// See [`RestrictedScope`] for why lifecycle waits are withheld here.
        pub fn supervisor(&self) -> RestrictedScope {
            self.cx.supervisor()
        }

        /// Returns this leader's declared child scope, restricted for the
        /// startup stage.
        ///
        /// The child scope starts only after this hook returns, so awaiting its
        /// readiness inline can never succeed. See [`RestrictedScope`].
        pub fn children(&self) -> Option<RestrictedScope> {
            self.cx.children()
        }
    };
    (message) => {
        /// Returns this actor's enclosing scope, restricted to operations that
        /// cannot await actor lifecycle progress.
        ///
        /// See [`RestrictedScope`] for the withheld operations and deadlock
        /// rationale.
        pub fn supervisor(&self) -> RestrictedScope {
            self.cx.supervisor()
        }

        /// Returns this leader's declared child scope with the same
        /// restriction. See [`RestrictedScope`].
        pub fn children(&self) -> Option<RestrictedScope> {
            self.cx.children()
        }
    };
    (stop) => {
        /// Returns this actor's enclosing scope, restricted for the shutdown
        /// stage.
        ///
        /// See [`RestrictedScope`] for why lifecycle waits are withheld here
        /// and where teardown that needs one belongs instead.
        pub fn supervisor(&self) -> RestrictedScope {
            self.cx.supervisor()
        }

        /// Returns this leader's declared child scope, restricted for the
        /// shutdown stage.
        ///
        /// The child scope is torn down around this hook, so awaiting its
        /// completion inline deadlocks the same way. See [`RestrictedScope`].
        pub fn children(&self) -> Option<RestrictedScope> {
            self.cx.children()
        }
    };
}

/// Cloneable, restart-stable, typed sender for an actor mailbox.
///
/// An `ActorRef<M>` is bound to a long-lived mailbox binding rather than a
/// single actor runtime instance. When the target actor is restarted (either
/// as part of a graph rerun or via per-actor supervision), the handle
/// transparently follows the new mailbox. That binding belongs to one
/// supervisor membership: removing a dynamic actor terminates its refs, and
/// adding another actor under the same id mints a fresh binding. A stale ref
/// therefore never delivers to the replacement membership.
pub struct ActorRef<M> {
    identity: Arc<()>,
    actor_id: Arc<str>,
    binding: watch::Receiver<BindingState<M>>,
    stats: Arc<ActorStatsCounters>,
    message_size: Option<Arc<MessageSizeObserver<M>>>,
    source_actor_id: Option<Arc<str>>,
    monitors: Arc<MonitorHub>,
}

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            identity: Arc::clone(&self.identity),
            actor_id: Arc::clone(&self.actor_id),
            binding: self.binding.clone(),
            stats: Arc::clone(&self.stats),
            message_size: self.message_size.clone(),
            source_actor_id: self.source_actor_id.clone(),
            monitors: Arc::clone(&self.monitors),
        }
    }
}

impl<M> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}

impl<M> ActorRef<M> {
    pub(crate) fn from_core(core: &Arc<BindingCore<M>>, source_actor_id: Option<Arc<str>>) -> Self {
        Self::from_parts(
            Arc::clone(core.identity()),
            core.actor_id().clone(),
            core.subscribe(),
            core.stats_counters(),
            core.message_size(),
            source_actor_id,
            core.monitor_hub(),
        )
    }

    pub(crate) fn from_parts(
        identity: Arc<()>,
        actor_id: Arc<str>,
        binding: watch::Receiver<BindingState<M>>,
        stats: Arc<ActorStatsCounters>,
        message_size: Option<Arc<MessageSizeObserver<M>>>,
        source_actor_id: Option<Arc<str>>,
        monitors: Arc<MonitorHub>,
    ) -> Self {
        Self {
            identity,
            actor_id,
            binding,
            stats,
            message_size,
            source_actor_id,
            monitors,
        }
    }

    pub(crate) fn detached(actor_id: Arc<str>) -> Self {
        let core = Arc::new(BindingCore::<M>::new(actor_id));
        Self::from_core(&core, None)
    }

    pub(crate) fn detached_with_size_hint(actor_id: Arc<str>, size_hint: fn(&M) -> usize) -> Self {
        let core = Arc::new(BindingCore::<M>::with_message_size(actor_id, size_hint));
        Self::from_core(&core, None)
    }

    pub(crate) fn binding_identity(&self) -> &Arc<()> {
        &self.identity
    }

    /// Returns the target actor id.
    pub fn id(&self) -> &str {
        &self.actor_id
    }

    /// Returns a point-in-time snapshot of this actor's message counters and
    /// current mailbox usage.
    ///
    /// A ref has no enclosing runtime context, so
    /// [`ActorStats::supervisor_path`] and [`ActorStats::lineage`] are `None`.
    /// Mailbox depth and capacity are zero while the ref is unbound between
    /// incarnations or permanently terminated.
    pub fn stats(&self) -> ActorStats {
        let (depth, capacity) = match &*self.binding.borrow() {
            BindingState::Bound(mailbox) => mailbox.usage(),
            BindingState::Unbound | BindingState::Terminated => (0, 0),
        };
        self.stats.snapshot(&self.actor_id, depth, capacity)
    }

    fn current_mailbox(&self) -> Result<MailboxRef<M>, TrySendError> {
        match self.binding.borrow().clone() {
            BindingState::Bound(mailbox) => Ok(mailbox),
            BindingState::Unbound if self.binding.has_changed().is_err() => {
                Err(self.actor_try_send_terminated())
            }
            BindingState::Unbound => Err(TrySendError::NotRunning {
                actor_id: self.actor_id.to_string(),
            }),
            BindingState::Terminated => Err(self.actor_try_send_terminated()),
        }
    }

    /// Sends a message to the target actor.
    ///
    /// This waits until the actor has a bound mailbox, waits for capacity when
    /// the actor uses a FIFO queue, and rides through restart windows when the
    /// actor is expected to rebind. Conflating mailboxes replace stale unread
    /// state immediately instead of waiting for capacity. This returns an
    /// error only when the actor has terminated with no restart scheduled, or
    /// when the binding source has been dropped.
    ///
    /// Cancelling this future while it is waiting drops the message.
    ///
    /// # Delivery contract
    ///
    /// For a FIFO mailbox, messages sent sequentially by one sender are
    /// enqueued in that order. Messages from concurrent senders may interleave.
    /// Conflating mailboxes intentionally replace unread state and do not make
    /// this FIFO guarantee.
    ///
    /// Delivery is **at-most-once**. `Ok` means the message was accepted by
    /// the current incarnation's mailbox, not that it will be processed:
    /// mailboxes are incarnation-owned, so messages accepted by an
    /// incarnation that stops before reading them are lost with it. The loss
    /// windows are restart and shutdown. Stronger guarantees
    /// (acknowledgements, redelivery) are user protocol built with
    /// [`call`](Self::call) and [`Reply`], not transport features.
    pub async fn send(&self, message: M) -> Result<(), SendError> {
        self.send_to_incarnation(message).await.map(drop)
    }

    /// Sends a message and returns the incarnation mailbox that accepted it.
    ///
    /// This is used by runtime adapters that need to restore cumulative state
    /// after the target actor moves to a fresh incarnation.
    pub(crate) async fn send_to_incarnation(&self, message: M) -> Result<MailboxRef<M>, SendError> {
        let mut binding = self.binding.clone();
        let mut message = message;
        let message_size = self
            .message_size
            .as_ref()
            .map(|observer| observer.size_hint(&message));

        loop {
            let mailbox = match self.wait_for_next_mailbox(&mut binding).await {
                Ok(mailbox) => mailbox,
                Err(error) => {
                    self.observe_send(MessageOperation::Send, Some(send_rejection(&error)));
                    self.stats.record_send(false);
                    return Err(error);
                }
            };

            match mailbox.send_retaining(message).await {
                SendOutcome::Accepted { conflated } => {
                    self.observe_send(MessageOperation::Send, None);
                    self.stats.record_send(true);
                    self.stats.record_conflated(conflated);
                    self.record_message_size(message_size);
                    return Ok(mailbox);
                }
                SendOutcome::Closed(returned) => {
                    self.observe_send(MessageOperation::Send, Some(SendRejection::MailboxClosed));
                    message = returned;
                    if let Err(error) = self
                        .wait_for_rebind_or_termination(&mut binding, &mailbox)
                        .await
                    {
                        self.observe_send(MessageOperation::Send, Some(send_rejection(&error)));
                        self.stats.record_send(false);
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Sends to one captured incarnation without following a later rebind.
    async fn send_to_mailbox(&self, mailbox: MailboxRef<M>, message: M, gate: &SendGate) {
        let message_size = self
            .message_size
            .as_ref()
            .map(|observer| observer.size_hint(&message));

        match mailbox.send_retaining_gated(message, gate).await {
            GatedSendOutcome::Accepted { conflated } => {
                self.observe_send(MessageOperation::Send, None);
                self.stats.record_send(true);
                self.stats.record_conflated(conflated);
                self.record_message_size(message_size);
            }
            GatedSendOutcome::Closed(_) => {
                self.observe_send(MessageOperation::Send, Some(SendRejection::MailboxClosed));
                self.stats.record_send(false);
            }
            GatedSendOutcome::Cancelled(_) => {}
        }
    }

    /// Attempts to send a message without waiting for mailbox capacity.
    ///
    /// A full FIFO queue returns [`TrySendError::Full`]. A conflating
    /// mailbox instead accepts the message and replaces stale unread state.
    pub fn try_send(&self, message: M) -> Result<(), TrySendError> {
        let message_size = self
            .message_size
            .as_ref()
            .map(|observer| observer.size_hint(&message));
        let result = match self.current_mailbox() {
            Ok(mailbox) => mailbox.try_send(message),
            Err(error) => Err(error),
        };
        self.observe_send(
            MessageOperation::TrySend,
            result.as_ref().err().map(try_send_rejection),
        );
        self.stats.record_send(result.is_ok());
        match result {
            Ok(conflated) => {
                self.stats.record_conflated(conflated);
                self.record_message_size(message_size);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Sends a request and waits for the actor to answer through the
    /// [`Reply`] carried inside the message.
    ///
    /// The timeout bounds the entire operation, including waiting for a
    /// mailbox binding, FIFO mailbox capacity, and the reply:
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use kokage::{ActorRef, Reply};
    ///
    /// enum Msg {
    ///     Get(Reply<u64>),
    /// }
    ///
    /// # async fn get(actor: &ActorRef<Msg>) -> Result<u64, Box<dyn std::error::Error>> {
    /// let value = actor.call(Duration::from_millis(250), Msg::Get).await?;
    /// # Ok(value)
    /// # }
    /// ```
    ///
    /// This waits for the same actor binding conditions as [`send`](Self::send),
    /// including FIFO mailbox capacity and expected restart windows. If the
    /// timeout expires before the request is accepted, the request is
    /// dropped. Once the mailbox accepts it, a timeout cannot retract it: the
    /// actor may still process the request and a late reply is discarded.
    ///
    /// Consequently, a timeout after acceptance has an **unknown outcome**.
    /// In-memory queries are usually harmless, but requests with external
    /// side effects need protocol-level idempotency keys and/or reconciliation
    /// so the caller can safely retry or discover what happened. A timeout is
    /// not an actor-work cancellation signal.
    ///
    /// Do not use `call` with a conflating mailbox: a newer message may replace
    /// the request before it is handled, in which case this returns
    /// [`CallError::ReplyDropped`]. Conflating mailboxes are for state
    /// snapshots rather than request/response commands.
    ///
    /// # Head-of-line blocking inside handlers
    ///
    /// An actor processes one message at a time, so a handler that awaits
    /// `call` stops its own mailbox for the full round-trip: every queued
    /// message, however urgent or unrelated, waits behind the outstanding
    /// request for up to the call's timeout. This is the actor-model
    /// equivalent of blocking inside an Erlang `gen_server` callback, and in
    /// fan-out or routing actors it turns one slow callee into head-of-line
    /// blocking for all traffic through the intermediary. Pipeline the
    /// bounded call back into an ordinary message with
    /// [`LiveContext::offload`], or move the slow dependency behind a dedicated
    /// child actor. The book's request/reply chapter covers the pattern.
    pub async fn call<T>(
        &self,
        timeout: Duration,
        message: impl FnOnce(Reply<T>) -> M,
    ) -> Result<T, CallError> {
        tokio::time::timeout(timeout, async {
            let (sender, receiver) = oneshot::channel();
            self.send(message(Reply { sender })).await?;
            receiver.await.map_err(|_| CallError::ReplyDropped {
                actor_id: self.actor_id.to_string(),
            })
        })
        .await
        .map_err(|_| CallError::Timeout {
            actor_id: self.actor_id.to_string(),
        })?
    }

    async fn wait_for_next_mailbox(
        &self,
        binding: &mut watch::Receiver<BindingState<M>>,
    ) -> Result<MailboxRef<M>, SendError> {
        loop {
            match binding.borrow().clone() {
                BindingState::Bound(mailbox) => return Ok(mailbox),
                BindingState::Unbound => {}
                BindingState::Terminated => return Err(self.actor_terminated()),
            }

            binding
                .changed()
                .await
                .map_err(|_| self.actor_terminated())?;
        }
    }

    pub(crate) async fn wait_terminated(&self) {
        let mut binding = self.binding.clone();
        loop {
            if matches!(&*binding.borrow(), BindingState::Terminated) {
                return;
            }
            if binding.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits until the stale mailbox is unbound and a fresh one is bound.
    ///
    /// Waiting for the stale mailbox to clear first avoids busy-looping in
    /// the window where an actor's mailbox is already closed but its binding
    /// has not been cleared yet.
    async fn wait_for_rebind_or_termination(
        &self,
        binding: &mut watch::Receiver<BindingState<M>>,
        stale: &MailboxRef<M>,
    ) -> Result<(), SendError> {
        loop {
            match binding.borrow().clone() {
                BindingState::Bound(current) if !current.same_channel(stale) => return Ok(()),
                BindingState::Bound(_) | BindingState::Unbound => {}
                BindingState::Terminated => return Err(self.actor_terminated()),
            }

            binding
                .changed()
                .await
                .map_err(|_| self.actor_terminated())?;
        }
    }

    fn actor_terminated(&self) -> SendError {
        SendError {
            actor_id: self.actor_id.to_string(),
        }
    }

    fn actor_try_send_terminated(&self) -> TrySendError {
        TrySendError::Terminated {
            actor_id: self.actor_id.to_string(),
        }
    }

    fn observe_send(&self, operation: MessageOperation, rejection: Option<SendRejection>) {
        trace_actor_message(
            self.source_actor_id.as_deref(),
            &self.actor_id,
            operation,
            rejection,
        );
    }

    fn record_message_size(&self, message_size: Option<usize>) {
        if let Some(message_size) = message_size {
            self.stats.record_message_size(message_size);
            self.message_size
                .as_ref()
                .expect("message size was produced by an observer")
                .record_metrics(message_size);
        }
    }

    pub(crate) fn record_received(&self) {
        self.stats.record_received();
    }

    fn set_outstanding_offloads(&self, outstanding: usize) {
        self.stats.set_outstanding_offloads(outstanding);
    }

    fn set_outstanding_scope_waits(&self, outstanding: usize) {
        self.stats.set_outstanding_scope_waits(outstanding);
    }
}

/// One-shot reply channel carried inside a request message.
///
/// Created by [`ActorRef::call`]; the receiving actor answers with
/// [`Reply::send`]. Dropping a `Reply` without sending makes the caller's
/// `call` fail with [`CallError::ReplyDropped`].
pub struct Reply<T> {
    sender: oneshot::Sender<T>,
}

/// Handle for one actor-owned background task.
///
/// Returned by [`LiveContext::offload`] and [`LiveContext::spawn_scope_wait`].
/// Dropping the handle does not affect the task. [`abort`](Self::abort)
/// abandons its work and suppresses its mapped mailbox message when
/// cancellation wins before delivery. An already accepted scope-wait message
/// cannot be retracted, and aborting an offload cannot undo work already
/// accepted by another actor or external service. The actor aborts every
/// outstanding task when its current incarnation ends.
#[derive(Clone, Debug)]
pub struct TaskHandle {
    abort: AbortHandle,
    cancellation: TaskCancellation,
}

#[derive(Clone, Debug)]
enum TaskCancellation {
    Offload(Arc<AtomicBool>),
    ScopeWait(Arc<SendGate>),
}

impl TaskHandle {
    /// Aborts the task and suppresses its continuation message.
    pub fn abort(&self) {
        match &self.cancellation {
            TaskCancellation::Offload(cancelled) => {
                cancelled.store(true, Ordering::Release);
                self.abort.abort();
            }
            TaskCancellation::ScopeWait(gate) => {
                if gate.cancel() {
                    self.abort.abort();
                }
            }
        }
    }

    /// Returns whether the task has finished or its abort has been observed.
    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}

/// The lifecycle state visible through an actor context.
///
/// `Draining` takes precedence once a handler is replaying accepted work,
/// including when a local stop and graph shutdown overlap. Graph shutdown on
/// its own does not change this status; inspect [`ActorContext::shutdown_token`]
/// when graph-wide cancellation matters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorStatus {
    /// The callback is live and has not requested a local stop.
    Running,
    /// This handler call is replaying accepted work before stopping.
    Draining,
    /// The live callback has requested a local stop.
    Stopping,
}

impl<T> Reply<T> {
    /// Sends the reply to the caller.
    ///
    /// If the caller has gone away the value is dropped silently.
    pub fn send(self, value: T) {
        let _ = self.sender.send(value);
    }
}

impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reply").finish_non_exhaustive()
    }
}

/// Stable name for one keyed actor-local timeout slot.
///
/// Keys are static protocol vocabulary: setting the same key replaces the
/// existing timeout in that slot, while different keys remain independent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimerKey(&'static str);

impl TimerKey {
    /// Creates a timer key.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the key's static name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl From<&'static str> for TimerKey {
    fn from(name: &'static str) -> Self {
        Self::new(name)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimerWake {
    pub(crate) id: u64,
    pub(crate) deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerSlot {
    Keyed(TimerKey),
    Anonymous,
}

enum Delivery<M> {
    Mailbox(Option<M>),
    Offload(Result<OffloadCompletion<M>, JoinError>),
    ScopeWait(Result<(TaskId, ()), JoinError>),
}

pub(crate) struct OffloadCompletion<M> {
    message: M,
    cancelled: Arc<AtomicBool>,
}

struct TimerEntry<M> {
    id: u64,
    slot: TimerSlot,
    deadline: Instant,
    message: M,
    cancellation: Option<CancellationToken>,
    repeat: Option<TimerRepeat<M>>,
}

type TimerRepeat<M> = (Duration, fn(&M) -> M);

/// Far-future cap for deadlines that would otherwise overflow, mirroring the
/// horizon tokio's own timer wheel saturates to.
const FAR_FUTURE: Duration = Duration::from_secs(86400 * 365 * 30);

/// Deadline `delay` from now, saturating instead of panicking: `Instant + Duration`
/// panics on overflow, and `Duration::MAX` is a plausible "never" sentinel.
fn deadline_after(delay: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(delay).unwrap_or_else(|| now + FAR_FUTURE)
}

pub(crate) struct TimerTable<M> {
    entries: Vec<TimerEntry<M>>,
    next_id: u64,
}

impl<M> Default for TimerTable<M> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
        }
    }
}

impl<M> TimerTable<M> {
    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        id
    }

    fn insert(
        &mut self,
        slot: TimerSlot,
        message: M,
        delay: Duration,
        cancellation: Option<CancellationToken>,
        repeat: Option<TimerRepeat<M>>,
    ) {
        if slot != TimerSlot::Anonymous
            && let Some(index) = self.entries.iter().position(|entry| entry.slot == slot)
        {
            self.entries.swap_remove(index);
        }
        let id = self.next_id();
        self.entries.push(TimerEntry {
            id,
            slot,
            deadline: deadline_after(delay),
            message,
            cancellation,
            repeat,
        });
    }

    fn clear(&mut self, slot: TimerSlot) {
        if let Some(index) = self.entries.iter().position(|entry| entry.slot == slot) {
            self.entries.swap_remove(index);
        }
    }

    fn is_armed(&self, slot: TimerSlot) -> bool {
        self.entries.iter().any(|entry| entry.slot == slot)
    }

    fn next_wake(&mut self) -> Option<TimerWake> {
        self.entries.retain(|entry| {
            !entry
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        });
        self.entries
            .iter()
            .min_by_key(|entry| entry.deadline)
            .map(|entry| TimerWake {
                id: entry.id,
                deadline: entry.deadline,
            })
    }

    fn take_fired(&mut self, wake: TimerWake) -> Option<M> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.id == wake.id && entry.deadline == wake.deadline)?;
        if self.entries[index].deadline > Instant::now() {
            return None;
        }
        if self.entries[index]
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            self.entries.swap_remove(index);
            return None;
        }
        if let Some((period, clone_message)) = self.entries[index].repeat {
            let message = clone_message(&self.entries[index].message);
            self.entries[index].deadline = deadline_after(period);
            Some(message)
        } else {
            Some(self.entries.swap_remove(index).message)
        }
    }
}

fn clone_message<M: Clone>(message: &M) -> M {
    message.clone()
}

pub(crate) struct ActorLifetime(CancellationToken);

impl ActorLifetime {
    pub(crate) fn new() -> Self {
        Self(CancellationToken::new())
    }

    fn token(&self) -> CancellationToken {
        self.0.clone()
    }
}

impl Drop for ActorLifetime {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Runtime context passed to a [`RawActor`](crate::host::RawActor) each time the
/// graph is run.
///
/// This is the widest context: a `RawActor` owns its receive loop, so it gets
/// the incoming [`mailbox`](Self::recv) and explicit
/// [`mark_ready`](Self::mark_ready) alongside the ambient capabilities —
/// an [`offload`](Self::offload) primitive for bounded asynchronous work, a
/// [`shutdown_token`](Self::shutdown_token) for cooperative shutdown, and
/// [`run_blocking`](Self::run_blocking) for blocking work.
///
/// Handler-style [`Actor`](crate::Actor) implementations do not see this type.
/// The framework owns their loop and hands each lifecycle hook a narrower view
/// of the same context: [`StartContext`], [`MessageContext`], and
/// [`StopContext`]. Those views omit what the stage cannot act on, so
/// mailbox-stealing `recv` calls and no-op `continue_with` calls are compile
/// errors rather than silent misbehavior.
pub struct ActorContext<M> {
    pub(crate) id: Arc<str>,
    pub(crate) mailbox: MailboxReceiver<M>,
    pub(crate) myself: ActorRef<M>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) observability: GraphObservability,
    pub(crate) timers: TimerTable<M>,
    pub(crate) lifetime: ActorLifetime,
    pub(crate) monitors: Arc<ActorMonitors>,
    pub(crate) ready: Option<oneshot::Sender<()>>,
    pub(crate) continuations: VecDeque<M>,
    pub(crate) stop_requested: bool,
    pub(crate) offloads: JoinSet<OffloadCompletion<M>>,
    pub(crate) scope_waits: JoinSet<()>,
    pub(crate) scope_wait_gates: HashMap<TaskId, Arc<SendGate>>,
    pub(crate) supervisor: RuntimeHandle,
    pub(crate) children: Option<RuntimeHandle>,
}

impl<M: Send + 'static> ActorContext<M> {
    /// Reports that a custom [`RawActor`](crate::host::RawActor) has completed
    /// initialization.
    ///
    /// This is only needed when `RawActor::readiness_gated` is overridden to
    /// return `true`. Handler-style [`Actor`](crate::Actor) implementations
    /// report readiness automatically after `on_start` succeeds.
    pub fn mark_ready(&mut self) {
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(());
        }
    }

    pub(crate) fn take_continuation(&mut self) -> Option<M> {
        self.continuations.pop_front()
    }

    pub(crate) fn push_continuation(&mut self, message: M) {
        self.continuations.push_back(message);
    }

    pub(crate) fn request_stop(&mut self) {
        self.stop_requested = true;
    }

    pub(crate) fn is_stop_requested(&self) -> bool {
        self.stop_requested
    }

    pub(crate) fn next_timer_wake(&mut self) -> Option<TimerWake> {
        self.timers.next_wake()
    }

    pub(crate) fn take_fired_timer(&mut self, wake: TimerWake) -> Option<M> {
        self.timers.take_fired(wake)
    }

    pub(crate) fn mailbox_depth(&self) -> usize {
        self.mailbox.usage().0
    }

    pub(crate) fn record_received(&self) {
        self.myself.record_received();
        self.observability.emit_message_received(&self.id);
    }

    fn sync_offload_gauge(&self) {
        self.myself.set_outstanding_offloads(self.offloads.len());
    }

    fn sync_scope_wait_gauge(&self) {
        self.myself
            .set_outstanding_scope_waits(self.scope_waits.len());
    }

    fn joined_offload(&mut self, joined: Result<OffloadCompletion<M>, JoinError>) -> Option<M> {
        self.sync_offload_gauge();
        match joined {
            Ok(completion) if !completion.cancelled.load(Ordering::Acquire) => {
                Some(completion.message)
            }
            Ok(_) => None,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => None,
        }
    }

    fn joined_scope_wait(&mut self, joined: Result<(TaskId, ()), JoinError>) {
        let task_id = match &joined {
            Ok((task_id, ())) => *task_id,
            Err(error) => error.id(),
        };
        self.scope_wait_gates.remove(&task_id);
        self.sync_scope_wait_gauge();
        match joined {
            Ok((_task_id, ())) => {}
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => {}
        }
    }

    pub(crate) async fn next_delivery(&mut self) -> Option<M> {
        loop {
            let has_offloads = !self.offloads.is_empty();
            let has_scope_waits = !self.scope_waits.is_empty();
            let delivery = tokio::select! {
                message = self.mailbox.recv() => Delivery::Mailbox(message),
                joined = self.offloads.join_next(), if has_offloads => {
                    Delivery::Offload(joined.expect("non-empty offload set returned no task"))
                }
                joined = self.scope_waits.join_next_with_id(), if has_scope_waits => {
                    Delivery::ScopeWait(joined.expect("non-empty scope-wait set returned no task"))
                }
            };
            match delivery {
                Delivery::Mailbox(message) => return message,
                Delivery::Offload(joined) => {
                    if let Some(message) = self.joined_offload(joined) {
                        return Some(message);
                    }
                }
                Delivery::ScopeWait(joined) => self.joined_scope_wait(joined),
            }
        }
    }

    pub(crate) fn try_delivery(&mut self) -> Result<M, tokio::sync::mpsc::error::TryRecvError> {
        while let Some(joined) = self.scope_waits.try_join_next_with_id() {
            self.joined_scope_wait(joined);
        }
        while let Some(joined) = self.offloads.try_join_next() {
            if let Some(message) = self.joined_offload(joined) {
                return Ok(message);
            }
        }
        self.mailbox.try_recv()
    }

    pub(crate) async fn next_drain_delivery(&mut self) -> Option<M> {
        loop {
            if let Ok(message) = self.try_delivery() {
                return Some(message);
            }
            if self.offloads.is_empty() {
                return None;
            }

            let delivery = tokio::select! {
                message = self.mailbox.recv() => Delivery::Mailbox(message),
                joined = self.offloads.join_next() => {
                    Delivery::Offload(joined.expect("non-empty offload set returned no task"))
                }
            };
            match delivery {
                Delivery::Mailbox(Some(message)) => return Some(message),
                Delivery::Mailbox(None) => return None,
                Delivery::Offload(joined) => {
                    if let Some(message) = self.joined_offload(joined) {
                        return Some(message);
                    }
                }
                Delivery::ScopeWait(_) => {
                    unreachable!("scope waits are excluded from the drain delivery set")
                }
            }
        }
    }

    fn spawn_scope_wait<W, F, T, Map>(
        &mut self,
        scope: &RestrictedScope,
        wait: W,
        map: Map,
    ) -> TaskHandle
    where
        W: FnOnce(RuntimeHandle) -> F + Send + 'static,
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        Map: FnOnce(T) -> M + Send + 'static,
    {
        let handle = scope.handle.clone();
        let myself = self.myself();
        let mailbox = myself
            .current_mailbox()
            .expect("a live actor context must have a bound mailbox");
        let gate = Arc::new(SendGate::new());
        let task_gate = Arc::clone(&gate);
        let abort = self.scope_waits.spawn(async move {
            let output = tokio::select! {
                biased;
                () = task_gate.cancelled() => return,
                output = wait(handle) => output,
            };
            if task_gate.is_cancelled() {
                return;
            }
            let message = map(output);
            myself.send_to_mailbox(mailbox, message, &task_gate).await;
        });
        self.scope_wait_gates.insert(abort.id(), Arc::clone(&gate));
        self.sync_scope_wait_gauge();
        TaskHandle {
            abort,
            cancellation: TaskCancellation::ScopeWait(gate),
        }
    }

    /// Runs a bounded future without blocking this actor's receive loop and
    /// returns its total outcome to this actor's receive loop as an ordinary
    /// message.
    ///
    /// The continuation is total: it receives either the future's value or
    /// [`OffloadDeadline`] and must produce a message in both cases. Completion ordering relative to
    /// external messages is unspecified, and completions do not consume
    /// mailbox capacity or participate in conflation.
    ///
    /// Offloads are incarnation-owned. They are aborted when the incarnation
    /// fails, restarts, or uses [`DrainPolicy::Discard`](crate::DrainPolicy).
    /// A draining handler actor keeps processing queued messages and offload
    /// completions until both are exhausted; the required deadline bounds
    /// every offload's future during that drain.
    ///
    /// Aborting or timing out an offload is not undo. If the future sent a request
    /// before being dropped, the receiver may still perform it and the outcome
    /// is unknown. Put effects behind actors and use idempotency keys plus
    /// reconciliation; offload futures should initiate requests, not mutate
    /// untracked local state directly. Domain cancellation can still be
    /// captured explicitly in `future`.
    ///
    /// Panics in the future or continuation resume on the actor task, so
    /// supervision treats them like an ordinary actor panic.
    pub fn offload<F, T, C>(&mut self, deadline: Duration, future: F, continuation: C) -> TaskHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let abort = self.offloads.spawn(async move {
            OffloadCompletion {
                message: continuation(timeout(deadline, future).await.map_err(|_| OffloadDeadline)),
                cancelled: task_cancelled,
            }
        });
        self.sync_offload_gauge();
        TaskHandle {
            abort,
            cancellation: TaskCancellation::Offload(cancelled),
        }
    }

    pub(crate) fn close_external_intake(&mut self) {
        self.mailbox.close_external();
    }

    pub(crate) fn abort_offloads(&mut self) {
        self.offloads.abort_all();
    }

    pub(crate) fn abort_scope_waits(&mut self) {
        for gate in self.scope_wait_gates.values() {
            gate.cancel();
        }
        self.scope_wait_gates.clear();
        self.scope_waits.abort_all();
        self.scope_waits = JoinSet::new();
        self.sync_scope_wait_gauge();
    }

    /// Returns the actor's unique identifier within the graph.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the shared graph shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    scope_context_methods!(actor);

    fn live_status(&self) -> ActorStatus {
        if self.stop_requested {
            ActorStatus::Stopping
        } else {
            ActorStatus::Running
        }
    }

    /// Returns a sender targeting this actor's own mailbox.
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    /// Sends `message` to `target` after `delay` has elapsed.
    ///
    /// The timer belongs to this scheduling actor incarnation, not the target.
    /// It is cancelled if this incarnation stops or restarts. Delivery uses an
    /// ordinary awaited [`ActorRef::send`], including the target mailbox's
    /// capacity and conflation behavior.
    pub fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> CancellationHandle {
        let timer = CancellationHandle::new();
        let task_timer = timer.clone();
        let lifetime = self.lifetime.token();
        let target = target.clone();

        tokio::spawn(async move {
            tokio::select! {
                biased;
                () = task_timer.cancelled() => {}
                () = lifetime.cancelled() => task_timer.cancel(),
                () = tokio::time::sleep(delay) => {
                    tokio::select! {
                        biased;
                        () = task_timer.cancelled() => {}
                        () = lifetime.cancelled() => task_timer.cancel(),
                        _ = target.send(message) => {}
                    }
                }
            }
        });

        timer
    }

    /// Sends a clone of `message` to `target` after every `period`.
    ///
    /// Missed ticks are skipped. The timer stops when cancelled, when this
    /// scheduling incarnation ends, or when the target permanently terminates.
    /// A zero period returns an already-cancelled handle and sends no messages.
    pub fn interval_to<T: Clone + Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> CancellationHandle {
        let timer = CancellationHandle::new();
        if period.is_zero() {
            timer.cancel();
            return timer;
        }

        let task_timer = timer.clone();
        let lifetime = self.lifetime.token();
        let target = target.clone();
        tokio::spawn(async move {
            let start = deadline_after(period);
            let mut interval = tokio::time::interval_at(start, period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = task_timer.cancelled() => break,
                    () = lifetime.cancelled() => {
                        task_timer.cancel();
                        break;
                    }
                    _ = interval.tick() => {
                        let sent = tokio::select! {
                            biased;
                            () = task_timer.cancelled() => break,
                            () = lifetime.cancelled() => {
                                task_timer.cancel();
                                break;
                            }
                            sent = target.send(message.clone()) => sent,
                        };
                        if sent.is_err() {
                            task_timer.cancel();
                            break;
                        }
                    }
                }
            }
        });

        timer
    }

    /// Watches the target logical actor across restarts.
    ///
    /// Each lifecycle transition of the target is converted by `map` into
    /// this actor's message type and delivered through this actor's mailbox,
    /// in lifecycle order: [`MonitorEvent::Up`] when an incarnation starts,
    /// [`MonitorEvent::Down`] when it exits, and a final
    /// [`MonitorEvent::Terminated`] when the target is permanently gone. A
    /// target that is already running delivers an immediate `Up` for the
    /// current incarnation; a target between incarnations stays silent until
    /// the next start, so a watch never races a supervisor restart.
    ///
    /// A watch belongs to the observing and watched actor memberships, not
    /// either current incarnation. It survives restarts on both sides and is
    /// delivered to whichever observer incarnation is running next. Calling
    /// `watch` again for the same pair, even within one incarnation, returns
    /// an alias of the existing watch without replacing its original `map`
    /// closure or emitting another immediate `Up`. Cancelling any alias
    /// cancels the pair. Explicit cancellation or permanent removal of either
    /// membership ends it.
    ///
    /// A replacement observer does not receive a fresh snapshot of the
    /// target. It must durably persist any observed state that it needs after
    /// a crash. To request a fresh snapshot instead, cancel the existing watch
    /// and register a new one: a running or terminated target responds
    /// immediately, at the cost of discarding any transitions still staged on
    /// the old watch.
    ///
    /// Delivery uses the observer's ordinary mailbox policy. A conflating
    /// mailbox may replace an unread event with a later one, so use a FIFO
    /// mailbox when every transition must be observed. Undelivered events are
    /// staged in a bounded per-watch buffer, so an observer whose mailbox
    /// stays full while its target restarts in a tight loop cannot grow memory
    /// without bound. On overflow the oldest transitions are dropped and the
    /// loss surfaces as a [`MonitorEvent::Lagged`] resync marker rather than
    /// silently; the terminal `Terminated` is never dropped.
    pub fn watch<T, F>(&self, target: &ActorRef<T>, mut map: F) -> CancellationHandle
    where
        T: Send + 'static,
        F: FnMut(MonitorEvent) -> M + Send + 'static,
    {
        let (cancellation, install) = self.monitors.register(&target.monitors);
        let monitor = CancellationHandle::from_token(cancellation.clone());
        if !install {
            return monitor;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            cancellation.cancel();
            return monitor;
        };
        // The guard closes the queue on drop, so the hub stops staging events
        // whether this task exits normally or unwinds through a panicking
        // `map` closure.
        let guard = target.monitors.register_watch(cancellation.clone());
        let myself = self.myself();
        runtime.spawn(async move {
            loop {
                // Arm the wake-up before observing the queue so a push that
                // races an empty drain is not lost.
                let waiter = guard.queue().waiter();
                if let Some(event) = guard.queue().pop() {
                    let terminal = matches!(event, MonitorEvent::Terminated { .. });
                    let message = map(event);
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => break,
                        _ = myself.send(message) => {}
                    }
                    if terminal {
                        break;
                    }
                    continue;
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    _ = waiter => {}
                }
            }
        });

        monitor
    }

    /// Waits for the next mailbox message or offload completion, or `None`
    /// once shutdown has been requested or the mailbox has been closed.
    ///
    /// Shutdown is checked first: as soon as shutdown is requested this
    /// returns `None`, even when messages are still queued. Queued messages
    /// are dropped when the actor exits unless the actor drains them with
    /// [`try_recv`](Self::try_recv), or uses [`Actor`](crate::Actor)
    /// with [`DrainPolicy::Drain`](crate::DrainPolicy). Queued
    /// [`call`](ActorRef::call)s whose reply messages are dropped observe
    /// [`CallError::ReplyDropped`](crate::CallError::ReplyDropped).
    ///
    /// A panic in an [`offload`](Self::offload) future or continuation resumes
    /// here, on the actor task.
    pub async fn recv(&mut self) -> Option<M> {
        let shutdown = self.shutdown.clone();
        let message = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            message = self.next_delivery() => message,
        };

        if message.is_some() {
            self.myself.record_received();
            self.observability.emit_message_received(&self.id);
        }

        message
    }

    /// Attempts to receive a queued mailbox message or completed offload
    /// without waiting and without consulting the shutdown token.
    ///
    /// This is intended for drain-then-exit loops in hand-written
    /// [`RawActor::run`](crate::host::RawActor::run) implementations: after
    /// [`recv`](Self::recv) returns `None` because shutdown was requested,
    /// queued messages remain readable here.
    ///
    /// A returned `None` means no message is immediately available; it does
    /// not prove the mailbox is fully drained while senders hold permits. For
    /// typical actors, prefer
    /// [`Actor`](crate::Actor) with
    /// [`DrainPolicy::Drain`](crate::DrainPolicy) so the framework owns the
    /// drain loop.
    ///
    /// A panic in an [`offload`](Self::offload) future or continuation resumes
    /// here, on the actor task.
    pub fn try_recv(&mut self) -> Option<M> {
        let message = self.try_delivery().ok();
        if message.is_some() {
            self.myself.record_received();
            self.observability.emit_message_received(&self.id);
        }
        message
    }

    /// Runs blocking work on Tokio's blocking pool and waits for its result.
    ///
    /// The closure receives a child of this actor's shutdown token. The token
    /// is also cancelled if the `run_blocking` future is dropped. Cancellation
    /// is cooperative: long-running closures should check
    /// [`CancellationToken::is_cancelled`] periodically and return promptly.
    ///
    /// A panic in the closure resumes on the actor task, so supervision treats
    /// it as an ordinary actor panic. Otherwise, the closure's return value is
    /// wrapped in `Ok`; [`BlockingCancelled`] is returned if Tokio shuts down
    /// before queued blocking work can complete.
    ///
    /// The surrounding host's shutdown bound is the backstop for closures that
    /// ignore cancellation: the explicit bound passed to
    /// [`RunnableActor::run_until`](crate::host::RunnableActor::run_until), or the
    /// supervised child's [`ShutdownPolicy`](crate::ShutdownPolicy) grace.
    /// Once that bound aborts the actor task, the blocking thread continues
    /// detached because Tokio blocking tasks cannot be aborted after they start.
    ///
    /// For detached or concurrent work, clone [`myself`](Self::myself), call
    /// [`tokio::task::spawn_blocking`] directly, and send the outcome back as a
    /// message. The mailbox then acts as the completion mechanism; see the
    /// [`blocking_lifecycle` example](https://github.com/ralexstokes/kokage/blob/main/crates/kokage/examples/blocking_lifecycle.rs).
    pub fn run_blocking<F, R>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<R, BlockingCancelled>> + Send + 'static
    where
        F: FnOnce(&CancellationToken) -> R + Send + 'static,
        R: Send + 'static,
    {
        let cancellation = self.shutdown.child_token();
        async move {
            let _cancel_on_drop = cancellation.clone().drop_guard();
            let joined = tokio::task::spawn_blocking(move || f(&cancellation)).await;

            match joined {
                Ok(result) => Ok(result),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_) => Err(BlockingCancelled),
            }
        }
    }
}

impl<M> Drop for ActorContext<M> {
    fn drop(&mut self) {
        for gate in self.scope_wait_gates.values() {
            gate.cancel();
        }
        self.myself.set_outstanding_offloads(0);
        self.myself.set_outstanding_scope_waits(0);
    }
}

mod sealed {
    pub trait Sealed<M> {
        fn cx(&self) -> &super::ActorContext<M>;
        fn cx_mut(&mut self) -> &mut super::ActorContext<M>;
        fn status(&self) -> super::ActorStatus;
    }
}

macro_rules! live_context_inherent_methods {
    () => {
        /// Returns the lifecycle state of this live callback.
        ///
        /// `Draining` takes precedence over a local stop request. Graph-wide
        /// shutdown is intentionally orthogonal; inspect
        /// [`shutdown_token`](Self::shutdown_token) for that signal.
        pub fn status(&self) -> ActorStatus {
            sealed::Sealed::status(self)
        }

        /// Requests a clean stop of this actor incarnation.
        pub fn stop(&mut self) {
            self.cx.request_stop();
        }

        /// Queues follow-up work as the actor's next message.
        pub fn continue_with(&mut self, message: A::Msg) {
            self.cx.push_continuation(message);
        }

        /// Watches the target logical actor across restarts.
        pub fn watch<T, F>(&self, target: &ActorRef<T>, map: F) -> CancellationHandle
        where
            T: Send + 'static,
            F: FnMut(MonitorEvent) -> A::Msg + Send + 'static,
        {
            self.cx.watch(target, map)
        }

        /// Runs a lifecycle wait as incarnation-owned background work and maps
        /// its result into this actor's mailbox.
        pub fn spawn_scope_wait<W, F, T, Map>(
            &mut self,
            scope: &RestrictedScope,
            wait: W,
            map: Map,
        ) -> TaskHandle
        where
            W: FnOnce(RuntimeHandle) -> F + Send + 'static,
            F: Future<Output = T> + Send + 'static,
            T: Send + 'static,
            Map: FnOnce(T) -> A::Msg + Send + 'static,
        {
            self.cx.spawn_scope_wait(scope, wait, map)
        }

        /// Arms a keyed one-shot timeout, replacing the timeout at the same key.
        pub fn set_timeout(&mut self, key: TimerKey, message: A::Msg, delay: Duration) {
            self.cx
                .timers
                .insert(TimerSlot::Keyed(key), message, delay, None, None);
        }

        /// Clears the timeout at `key`, if one is armed.
        pub fn clear_timeout(&mut self, key: TimerKey) {
            self.cx.timers.clear(TimerSlot::Keyed(key));
        }

        /// Returns whether the timeout at `key` is armed.
        pub fn timeout_armed(&self, key: TimerKey) -> bool {
            self.cx.timers.is_armed(TimerSlot::Keyed(key))
        }

        /// Schedules an anonymous one-shot actor-local message.
        pub fn send_after(&mut self, message: A::Msg, delay: Duration) -> CancellationHandle {
            let timer = CancellationHandle::new();
            self.cx.timers.insert(
                TimerSlot::Anonymous,
                message,
                delay,
                Some(timer.token()),
                None,
            );
            timer
        }

        /// Schedules a periodic actor-local message.
        pub fn interval(&mut self, message: A::Msg, period: Duration) -> CancellationHandle
        where
            A::Msg: Clone,
        {
            let timer = CancellationHandle::new();
            if period.is_zero() {
                timer.cancel();
                return timer;
            }
            self.cx.timers.insert(
                TimerSlot::Anonymous,
                message,
                period,
                Some(timer.token()),
                Some((period, clone_message::<A::Msg>)),
            );
            timer
        }

        /// Sends `message` to `target` after `delay`, bound to this actor
        /// incarnation.
        pub fn send_after_to<T: Send + 'static>(
            &self,
            target: &ActorRef<T>,
            message: T,
            delay: Duration,
        ) -> CancellationHandle {
            self.cx.send_after_to(target, message, delay)
        }

        /// Periodically sends `message` to `target`, bound to this actor
        /// incarnation.
        pub fn interval_to<T: Clone + Send + 'static>(
            &self,
            target: &ActorRef<T>,
            message: T,
            period: Duration,
        ) -> CancellationHandle {
            self.cx.interval_to(target, message, period)
        }

        /// Runs a bounded future without blocking this actor's receive loop.
        pub fn offload<F, T, C>(
            &mut self,
            deadline: Duration,
            future: F,
            continuation: C,
        ) -> TaskHandle
        where
            F: Future<Output = T> + Send + 'static,
            T: Send + 'static,
            C: FnOnce(Result<T, OffloadDeadline>) -> A::Msg + Send + 'static,
        {
            self.cx.offload(deadline, future, continuation)
        }
    };
}

/// The capabilities an actor has while its incarnation is still live,
/// independent of which lifecycle stage it is in.
///
/// This combines stage-shared identity, shutdown observation, and blocking
/// work with the capabilities whose results are delivered back into this
/// incarnation. The delivery-shaped half is why the trait covers
/// [`StartContext`] and [`MessageContext`], but excludes [`StopContext`]. It is
/// the type a shared helper should take when it is called from both `on_start`
/// and `handle`:
///
/// ```no_run
/// use kokage::LiveContext;
/// use std::time::Duration;
///
/// # enum Msg { Tick }
/// const TICK: kokage::TimerKey = kokage::TimerKey::new("tick");
/// fn arm(ctx: &mut impl LiveContext<Msg>) {
///     ctx.set_timeout(TICK, Msg::Tick, Duration::from_secs(5));
/// }
/// ```
///
/// [`StopContext`] deliberately does not implement it: after the receive loop
/// exits, timers, watches, offloads, and continuations have no recipient. The
/// stage-shared identity, shutdown, and blocking methods remain available
/// there as inherent methods.
///
/// This trait is sealed. It exists to name the shared surface, not to let
/// callers substitute their own context.
pub trait LiveContext<M: Send + 'static>: sealed::Sealed<M> {
    /// Returns the actor's unique identifier within the graph.
    fn id(&self) -> &str {
        self.cx().id()
    }

    /// Returns a sender targeting this actor's own mailbox.
    fn myself(&self) -> ActorRef<M> {
        self.cx().myself()
    }

    /// Returns the shared graph shutdown token.
    fn shutdown_token(&self) -> &CancellationToken {
        self.cx().shutdown_token()
    }

    /// Runs blocking work on Tokio's blocking pool.
    fn run_blocking<F, R>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<R, BlockingCancelled>> + Send + 'static
    where
        F: FnOnce(&CancellationToken) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.cx().run_blocking(f)
    }

    /// Returns the lifecycle state of this live callback.
    fn status(&self) -> ActorStatus {
        sealed::Sealed::status(self)
    }

    /// Requests a clean stop of this actor incarnation.
    fn stop(&mut self) {
        self.cx_mut().request_stop();
    }

    /// Queues follow-up work as the actor's next message.
    fn continue_with(&mut self, message: M) {
        self.cx_mut().push_continuation(message);
    }

    /// Watches the target logical actor across restarts.
    fn watch<T, F>(&self, target: &ActorRef<T>, map: F) -> CancellationHandle
    where
        T: Send + 'static,
        F: FnMut(MonitorEvent) -> M + Send + 'static,
    {
        self.cx().watch(target, map)
    }

    /// Runs a lifecycle wait as incarnation-owned background work.
    fn spawn_scope_wait<W, F, T, Map>(
        &mut self,
        scope: &RestrictedScope,
        wait: W,
        map: Map,
    ) -> TaskHandle
    where
        W: FnOnce(RuntimeHandle) -> F + Send + 'static,
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        Map: FnOnce(T) -> M + Send + 'static,
    {
        self.cx_mut().spawn_scope_wait(scope, wait, map)
    }

    /// Arms a keyed one-shot timeout, replacing the timeout at the same key.
    fn set_timeout(&mut self, key: TimerKey, message: M, delay: Duration) {
        self.cx_mut()
            .timers
            .insert(TimerSlot::Keyed(key), message, delay, None, None);
    }

    /// Clears the timeout at `key`, if one is armed.
    fn clear_timeout(&mut self, key: TimerKey) {
        self.cx_mut().timers.clear(TimerSlot::Keyed(key));
    }

    /// Returns whether the timeout at `key` is armed.
    fn timeout_armed(&self, key: TimerKey) -> bool {
        self.cx().timers.is_armed(TimerSlot::Keyed(key))
    }

    /// Schedules an anonymous one-shot actor-local message.
    fn send_after(&mut self, message: M, delay: Duration) -> CancellationHandle {
        let timer = CancellationHandle::new();
        self.cx_mut().timers.insert(
            TimerSlot::Anonymous,
            message,
            delay,
            Some(timer.token()),
            None,
        );
        timer
    }

    /// Schedules a periodic actor-local message.
    fn interval(&mut self, message: M, period: Duration) -> CancellationHandle
    where
        M: Clone,
    {
        let timer = CancellationHandle::new();
        if period.is_zero() {
            timer.cancel();
            return timer;
        }
        self.cx_mut().timers.insert(
            TimerSlot::Anonymous,
            message,
            period,
            Some(timer.token()),
            Some((period, clone_message::<M>)),
        );
        timer
    }

    /// Sends `message` to `target` after `delay`, bound to this incarnation.
    fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> CancellationHandle {
        self.cx().send_after_to(target, message, delay)
    }

    /// Periodically sends `message` to `target`, bound to this incarnation.
    fn interval_to<T: Clone + Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> CancellationHandle {
        self.cx().interval_to(target, message, period)
    }

    /// Runs a bounded future without blocking this actor's receive loop.
    fn offload<F, T, C>(&mut self, deadline: Duration, future: F, continuation: C) -> TaskHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        self.cx_mut().offload(deadline, future, continuation)
    }
}

macro_rules! live_context_trait_impl {
    ($view:ident) => {
        impl<A: Actor + ?Sized> LiveContext<A::Msg> for $view<'_, A> {
            fn id(&self) -> &str {
                $view::id(self)
            }

            fn myself(&self) -> ActorRef<A::Msg> {
                $view::myself(self)
            }

            fn shutdown_token(&self) -> &CancellationToken {
                $view::shutdown_token(self)
            }

            fn run_blocking<F, R>(
                &self,
                f: F,
            ) -> impl Future<Output = Result<R, BlockingCancelled>> + Send + 'static
            where
                F: FnOnce(&CancellationToken) -> R + Send + 'static,
                R: Send + 'static,
            {
                $view::run_blocking(self, f)
            }

            fn status(&self) -> ActorStatus {
                $view::status(self)
            }

            fn stop(&mut self) {
                $view::stop(self);
            }

            fn continue_with(&mut self, message: A::Msg) {
                $view::continue_with(self, message);
            }

            fn watch<T, F>(&self, target: &ActorRef<T>, map: F) -> CancellationHandle
            where
                T: Send + 'static,
                F: FnMut(MonitorEvent) -> A::Msg + Send + 'static,
            {
                $view::watch(self, target, map)
            }

            fn spawn_scope_wait<W, F, T, Map>(
                &mut self,
                scope: &RestrictedScope,
                wait: W,
                map: Map,
            ) -> TaskHandle
            where
                W: FnOnce(RuntimeHandle) -> F + Send + 'static,
                F: Future<Output = T> + Send + 'static,
                T: Send + 'static,
                Map: FnOnce(T) -> A::Msg + Send + 'static,
            {
                $view::spawn_scope_wait(self, scope, wait, map)
            }

            fn set_timeout(&mut self, key: TimerKey, message: A::Msg, delay: Duration) {
                $view::set_timeout(self, key, message, delay);
            }

            fn clear_timeout(&mut self, key: TimerKey) {
                $view::clear_timeout(self, key);
            }

            fn timeout_armed(&self, key: TimerKey) -> bool {
                $view::timeout_armed(self, key)
            }

            fn send_after(&mut self, message: A::Msg, delay: Duration) -> CancellationHandle {
                $view::send_after(self, message, delay)
            }

            fn interval(&mut self, message: A::Msg, period: Duration) -> CancellationHandle
            where
                A::Msg: Clone,
            {
                $view::interval(self, message, period)
            }

            fn send_after_to<T: Send + 'static>(
                &self,
                target: &ActorRef<T>,
                message: T,
                delay: Duration,
            ) -> CancellationHandle {
                $view::send_after_to(self, target, message, delay)
            }

            fn interval_to<T: Clone + Send + 'static>(
                &self,
                target: &ActorRef<T>,
                message: T,
                period: Duration,
            ) -> CancellationHandle {
                $view::interval_to(self, target, message, period)
            }

            fn offload<F, T, C>(
                &mut self,
                deadline: Duration,
                future: F,
                continuation: C,
            ) -> TaskHandle
            where
                F: Future<Output = T> + Send + 'static,
                T: Send + 'static,
                C: FnOnce(Result<T, OffloadDeadline>) -> A::Msg + Send + 'static,
            {
                $view::offload(self, deadline, future, continuation)
            }
        }
    };
}

macro_rules! live_context {
    ($view:ident) => {
        impl<A: Actor + ?Sized> sealed::Sealed<A::Msg> for $view<'_, A> {
            fn cx(&self) -> &ActorContext<A::Msg> {
                self.cx
            }

            fn cx_mut(&mut self) -> &mut ActorContext<A::Msg> {
                self.cx
            }

            fn status(&self) -> ActorStatus {
                self.cx.live_status()
            }
        }

        live_context_trait_impl!($view);

        impl<A: Actor + ?Sized> $view<'_, A> {
            live_context_inherent_methods!();
        }
    };
    ($view:ident, draining) => {
        impl<A: Actor + ?Sized> sealed::Sealed<A::Msg> for $view<'_, A> {
            fn cx(&self) -> &ActorContext<A::Msg> {
                self.cx
            }

            fn cx_mut(&mut self) -> &mut ActorContext<A::Msg> {
                self.cx
            }

            fn status(&self) -> ActorStatus {
                if self.draining {
                    ActorStatus::Draining
                } else {
                    self.cx.live_status()
                }
            }
        }

        live_context_trait_impl!($view);

        impl<A: Actor + ?Sized> $view<'_, A> {
            live_context_inherent_methods!();
        }
    };
}

macro_rules! stage_context_methods {
    ($view:ident, $scope_stage:ident $(, $myself_note:literal)?) => {
        impl<A: Actor + ?Sized> $view<'_, A> {
            ambient_context_method! {
                id,
                pub fn id(&self) -> &str {
                    self.cx.id()
                }
            }

            ambient_context_method! {
                myself $([$myself_note])?,
                pub fn myself(&self) -> ActorRef<A::Msg> {
                    self.cx.myself()
                }
            }

            ambient_context_method! {
                shutdown_token,
                pub fn shutdown_token(&self) -> &CancellationToken {
                    self.cx.shutdown_token()
                }
            }

            ambient_context_method! {
                run_blocking,
                pub fn run_blocking<F, R>(
                    &self,
                    f: F,
                ) -> impl Future<Output = Result<R, BlockingCancelled>> + Send + 'static
                where
                    F: FnOnce(&CancellationToken) -> R + Send + 'static,
                    R: Send + 'static,
                {
                    self.cx.run_blocking(f)
                }
            }

            scope_context_methods!($scope_stage);
        }
    };
}

/// A lifecycle-restricted scope handle as seen from
/// every [`Actor`] lifecycle stage and directly from [`RawActor`](crate::host::RawActor)
/// code.
///
/// This is a [`RuntimeHandle`] with the lifecycle-awaiting operations withheld.
/// An actor cannot report ready until its `on_start` returns, so awaiting any
/// operation that blocks on another child's lifecycle — the scope starting, a
/// child completing, the scope shutting down — deadlocks the actor against
/// itself. Those methods are absent here rather than documented as forbidden.
///
/// The restriction is closed under navigation: [`subtree`](Self::subtree)
/// hands back another `RestrictedScope`. During startup, a sibling scope
/// declared after this actor starts after it reports ready. During shutdown, a
/// nested scope's shutdown is sequenced with this one's. No method on
/// `RestrictedScope` exposes the full [`RuntimeHandle`] directly.
/// [`LiveContext::spawn_scope_wait`] is the explicit escape hatch: its closure
/// receives a full handle, but runs in a separate incarnation-owned task rather
/// than inside the actor callback. Code in that closure can still export the
/// handle if it deliberately chooses to do so.
///
/// During ordinary message handling or a raw receive loop, lifecycle waits can
/// likewise depend on the current actor draining work or returning from the
/// operation that holds up the target child.
///
/// The shutdown-stage restriction has a different cause but the same shape. A
/// stopping child is still attached to its supervisor: cooperative removal
/// waits for `on_stop` to return before the child is detached and its exit
/// recorded. Anything awaited here that blocks on the scope's membership
/// settling — the scope finishing its shutdown, a child completing, this
/// actor's own removal — therefore waits on a detach that waits on this hook.
/// The cycle resolves only when the shutdown grace period runs out and aborts
/// the actor, turning a clean stop into a timed-out one.
///
/// Fire-and-forget control is kept: [`shutdown`](Self::shutdown) requests and
/// returns, and insertion schedules rather than waits. Live actor stages can
/// run a lifecycle wait safely with [`LiveContext::spawn_scope_wait`]; the
/// shutdown stage cannot start future work.
#[derive(Clone, Debug)]
pub struct RestrictedScope {
    handle: RuntimeHandle,
}

macro_rules! restricted_scope_forwards {
    () => {
        /// Returns a point-in-time snapshot of the scope.
        pub fn snapshot(&self) -> crate::observe::SupervisorSnapshot {
            self.handle.snapshot()
        }

        /// Returns per-actor message counters for this scope.
        pub fn actor_stats(&self) -> Vec<crate::observe::ActorStats> {
            self.handle.actor_stats()
        }

        /// Subscribes to scope snapshots.
        pub fn subscribe_snapshots(&self) -> watch::Receiver<crate::observe::SupervisorSnapshot> {
            self.handle.subscribe_snapshots()
        }

        /// Returns a handle to a nested subtree by id, restricted the same way
        /// as this one.
        pub fn subtree(&self, id: &str) -> Option<Self> {
            self.handle.subtree(id).map(Self::new)
        }

        /// Inserts an actor with default options into this scope.
        ///
        /// This is safe to await from an actor callback: success means the
        /// membership was inserted and startup was scheduled, not that the
        /// actor reported ready. See [`RuntimeHandle::add_actor`].
        pub async fn add_actor<F>(
            &self,
            label: impl Into<String>,
            factory: F,
        ) -> Result<ActorRef<<F::Actor as crate::host::RawActor>::Msg>, crate::ControlError>
        where
            F: crate::ActorFactory,
        {
            self.handle.add_actor(label, factory).await
        }

        /// Inserts an actor with explicit options into this scope.
        ///
        /// This is safe to await from an actor callback because insertion only
        /// schedules startup. See [`RuntimeHandle::add_actor_with`].
        pub async fn add_actor_with<F>(
            &self,
            label: impl Into<String>,
            factory: F,
            options: crate::DynamicActorOptions<<F::Actor as crate::host::RawActor>::Msg>,
        ) -> Result<ActorRef<<F::Actor as crate::host::RawActor>::Msg>, crate::ControlError>
        where
            F: crate::ActorFactory,
        {
            self.handle.add_actor_with(label, factory, options).await
        }

        /// Inserts an arbitrary supervised task child into this scope.
        ///
        /// This is safe to await from an actor callback because insertion only
        /// schedules startup. See [`RuntimeHandle::add_child`].
        pub async fn add_child(
            &self,
            child: crate::host::ChildSpec,
        ) -> Result<u64, crate::ControlError> {
            self.handle.add_child(child).await
        }

        /// Inserts an identity-owning subtree into this scope.
        ///
        /// This is safe to await from an actor callback because insertion only
        /// schedules startup. Unlike [`RuntimeHandle::add_subtree`], this
        /// restricted form returns `()` so a full handle cannot escape. Keep
        /// the id and call [`subtree`](Self::subtree) after successful insertion
        /// when a restricted handle is needed.
        pub async fn add_subtree(
            &self,
            id: impl Into<String>,
            tree: impl Into<crate::TreeNode>,
        ) -> Result<(), crate::ControlError> {
            self.handle.add_subtree(id, tree).await.map(drop)
        }

        /// Observes lifecycle transitions of this scope's direct children.
        pub fn watch_lifecycle(&self) -> crate::observe::ChildLifecycleWatch {
            self.handle.watch_lifecycle()
        }

        /// Observes lifecycle transitions of this scope and everything beneath
        /// it.
        pub fn watch_lifecycle_recursive(&self) -> crate::observe::LifecycleWatch {
            self.handle.watch_lifecycle_recursive()
        }

        /// Pumps direct-child lifecycle events into `target` using its ordinary
        /// mailbox policy.
        ///
        /// The pump runs in a detached task, so starting it from a lifecycle
        /// hook does not block that hook. Retain the returned guard for as long
        /// as delivery is wanted; dropping or cancelling it stops the pump.
        /// See [`RuntimeHandle::watch_lifecycle_to`].
        pub fn watch_lifecycle_to<M, F>(
            &self,
            target: &ActorRef<M>,
            map: F,
        ) -> crate::observe::LifecycleWatchGuard
        where
            M: Send + 'static,
            F: FnMut(crate::observe::ChildLifecycleEvent) -> M + Send + 'static,
        {
            self.handle.watch_lifecycle_to(target, map)
        }

        /// Shuts this scope down once every named child has completed.
        ///
        /// The returned guard must be retained; dropping it cancels the watch
        /// and leaves the scope running. See
        /// [`RuntimeHandle::shutdown_on_completion`] for child-id and runtime
        /// requirements.
        pub fn shutdown_on_completion<I, S>(&self, ids: I) -> crate::observe::CompletionGuard
        where
            I: IntoIterator<Item = S>,
            S: Into<String>,
        {
            self.handle.shutdown_on_completion(ids)
        }

        /// Requests shutdown of this scope without waiting for it.
        pub fn shutdown(&self) {
            self.handle.shutdown()
        }
    };
}

impl RestrictedScope {
    fn new(handle: RuntimeHandle) -> Self {
        Self { handle }
    }

    restricted_scope_forwards!();
}

/// Context handed to [`Actor::on_start`](crate::Actor::on_start).
///
/// Adds [`continue_with`](LiveContext::continue_with) and
/// [`stop`](LiveContext::stop) to the ambient capabilities and exposes scope
/// handles as [`RestrictedScope`], which withholds the lifecycle waits that
/// would deadlock an actor that has not reported ready.
///
/// The mailbox is deliberately absent: the provided receive loop owns it, and
/// readiness is reported by the framework once this hook returns.
///
/// The parameter is the actor, not its message: a hook signature writes
/// `&mut StartContext<'_, Self>` and the message type is projected from
/// [`Actor::Msg`](crate::Actor::Msg).
pub struct StartContext<'a, A: Actor + ?Sized> {
    cx: &'a mut ActorContext<A::Msg>,
}

/// Context handed to [`Actor::handle`](crate::Actor::handle) — the context in
/// which one message is handled.
///
/// The ambient capabilities plus [`continue_with`](LiveContext::continue_with),
/// [`stop`](LiveContext::stop), and restricted scope handles. The mailbox is
/// absent because the provided receive loop owns it; a handler that reads it
/// directly would bypass drain accounting and the continuation queue.
///
/// This is the only hook the provided loop calls from two different phases, so
/// it is also the only one whose [`status`](Self::status) can be `Draining`.
///
/// The parameter is the actor, not its message: a hook signature writes
/// `&mut MessageContext<'_, Self>` and the message type is projected from
/// [`Actor::Msg`](crate::Actor::Msg). A helper shared across actors should
/// take [`LiveContext`] rather than name this type with a concrete message.
pub struct MessageContext<'a, A: Actor + ?Sized> {
    cx: &'a mut ActorContext<A::Msg>,
    draining: bool,
}

/// Context handed to [`Actor::on_stop`](crate::Actor::on_stop).
///
/// A deliberately narrow surface. The hook runs after the receive loop has
/// exited and the mailbox has been drained or discarded, so anything that
/// queues future work for this incarnation — timers, intervals, watches,
/// offloads, continuations — has no one left to deliver to and
/// is withheld. What remains is identity, the shutdown token, the scope
/// handles, and [`run_blocking`](StopContext::run_blocking) for synchronous
/// teardown. These common operations are inherent methods on every stage
/// context, so calling them requires no context-trait import.
///
/// The scope handles are narrowed to [`RestrictedScope`], which withholds the
/// lifecycle waits that would block on a detach this hook is itself holding up.
///
/// The parameter is the actor, not its message: a hook signature writes
/// `&mut StopContext<'_, Self>` and the message type is projected from
/// [`Actor::Msg`](crate::Actor::Msg).
pub struct StopContext<'a, A: Actor + ?Sized> {
    cx: &'a mut ActorContext<A::Msg>,
}

live_context!(StartContext);
live_context!(MessageContext, draining);
stage_context_methods!(StartContext, start);
stage_context_methods!(MessageContext, message);
stage_context_methods!(
    StopContext,
    stop,
    "In `StopContext` the mailbox is no longer read by this incarnation. Teardown can pass the ref elsewhere, but should not post work to itself."
);

impl<'a, A: Actor + ?Sized> StartContext<'a, A> {
    pub(crate) fn new(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self { cx }
    }
}

impl<'a, A: Actor + ?Sized> MessageContext<'a, A> {
    pub(crate) fn new(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self {
            cx,
            draining: false,
        }
    }

    pub(crate) fn draining(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self { cx, draining: true }
    }
}

impl<'a, A: Actor + ?Sized> StopContext<'a, A> {
    pub(crate) fn new(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self { cx }
    }
}

fn send_rejection(_: &SendError) -> SendRejection {
    SendRejection::ActorTerminated
}

fn try_send_rejection(error: &TrySendError) -> SendRejection {
    match error {
        TrySendError::NotRunning { .. } => SendRejection::NotRunning,
        TrySendError::Terminated { .. } => SendRejection::ActorTerminated,
        TrySendError::Full { .. } => SendRejection::MailboxFull,
        TrySendError::Closed { .. } => SendRejection::MailboxClosed,
    }
}
