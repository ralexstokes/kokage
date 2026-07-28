use std::{
    collections::VecDeque,
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
    task::{AbortHandle, JoinError, JoinSet},
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::RuntimeHandle;

use crate::actor::{
    binding::{
        ActorStats, ActorStatsCounters, BindingCore, BindingState, MailboxReceiver, MailboxRef,
        MessageSizeObserver, SendOutcome,
    },
    cancellation::{CancellationHandle, Lifetime},
    error::{BlockingCancelled, CallError, OffloadDeadline, SendError, TryRecvError},
    handler::Actor,
    monitor::{ActorMonitors, MonitorEvent, MonitorHub},
    observability::{GraphObservability, MessageOperation, SendRejection, trace_actor_message},
};

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
            core.actor_id().clone(),
            core.subscribe(),
            core.stats_counters(),
            core.message_size(),
            source_actor_id,
            core.monitor_hub(),
        )
    }

    pub(crate) fn from_parts(
        actor_id: Arc<str>,
        binding: watch::Receiver<BindingState<M>>,
        stats: Arc<ActorStatsCounters>,
        message_size: Option<Arc<MessageSizeObserver<M>>>,
        source_actor_id: Option<Arc<str>>,
        monitors: Arc<MonitorHub>,
    ) -> Self {
        Self {
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

    /// Returns the target actor id.
    pub fn id(&self) -> &str {
        &self.actor_id
    }

    /// Returns a point-in-time snapshot of this actor's message counters and
    /// current mailbox usage.
    pub fn stats(&self) -> ActorStats {
        let (depth, capacity) = match &*self.binding.borrow() {
            BindingState::Bound(mailbox) => mailbox.usage(),
            BindingState::Unbound | BindingState::Terminated => (0, 0),
        };
        self.stats.snapshot(&self.actor_id, depth, capacity)
    }

    fn current_mailbox(&self) -> Result<MailboxRef<M>, SendError> {
        match self.binding.borrow().clone() {
            BindingState::Bound(mailbox) => Ok(mailbox),
            BindingState::Unbound if self.binding.has_changed().is_err() => {
                Err(self.actor_terminated())
            }
            BindingState::Unbound => Err(SendError::ActorNotRunning {
                actor_id: self.actor_id.to_string(),
            }),
            BindingState::Terminated => Err(self.actor_terminated()),
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

    /// Attempts to send a message without waiting for mailbox capacity.
    ///
    /// A full FIFO queue returns [`SendError::MailboxFull`]. A conflating
    /// mailbox instead accepts the message and replaces stale unread state.
    pub fn try_send(&self, message: M) -> Result<(), SendError> {
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
            result.as_ref().err().map(send_rejection),
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
    /// use tokio_otp::{ActorRef, Reply};
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
        SendError::ActorTerminated {
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
}

/// One-shot reply channel carried inside a request message.
///
/// Created by [`ActorRef::call`]; the receiving actor answers with
/// [`Reply::send`]. Dropping a `Reply` without sending makes the caller's
/// `call` fail with [`CallError::ReplyDropped`].
pub struct Reply<T> {
    sender: oneshot::Sender<T>,
}

/// Handle for one bounded future started by [`ActorContext::offload`].
///
/// Dropping the handle does not affect the offload. [`abort`](Self::abort)
/// abandons the future and prevents its continuation message from being
/// delivered. Aborting a request only abandons the local wait: it cannot retract
/// work that another actor or external service already accepted.
#[derive(Clone, Debug)]
pub struct OffloadHandle {
    abort: AbortHandle,
    cancelled: Arc<AtomicBool>,
}

impl OffloadHandle {
    /// Aborts the offload and suppresses its continuation message.
    pub fn abort(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.abort.abort();
    }

    /// Returns whether the offload has finished or its abort has been observed.
    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
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
pub(crate) fn deadline_after(delay: Duration) -> Instant {
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

    fn observe(&self) -> Lifetime {
        Lifetime::from_token(self.0.clone())
    }
}

impl Drop for ActorLifetime {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Runtime context passed to a [`RawActor`](crate::RawActor) each time the
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
    pub(crate) offloads: JoinSet<OffloadCompletion<M>>,
    pub(crate) supervisor: RuntimeHandle,
    pub(crate) children: Option<RuntimeHandle>,
}

impl<M: Send + 'static> ActorContext<M> {
    /// Reports that a custom [`RawActor`](crate::RawActor) has completed
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

    pub(crate) async fn next_delivery(&mut self) -> Option<M> {
        loop {
            let has_offloads = !self.offloads.is_empty();
            let delivery = tokio::select! {
                message = self.mailbox.recv() => Delivery::Mailbox(message),
                joined = self.offloads.join_next(), if has_offloads => {
                    Delivery::Offload(joined.expect("non-empty offload set returned no task"))
                }
            };
            match delivery {
                Delivery::Mailbox(message) => return message,
                Delivery::Offload(joined) => {
                    if let Some(message) = self.joined_offload(joined) {
                        return Some(message);
                    }
                }
            }
        }
    }

    pub(crate) fn try_delivery(&mut self) -> Result<M, tokio::sync::mpsc::error::TryRecvError> {
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
            }
        }
    }

    /// Runs a bounded future and substitutes `fallback` when its deadline
    /// expires, then returns the resulting value to the receive loop as an
    /// ordinary message.
    ///
    /// This is the usual way to pipeline bounded work from an actor. A timed
    /// out offload may already have initiated an external effect, so `fallback`
    /// should represent an unknown outcome that the actor can reconcile.
    /// Use [`Self::offload`] when the continuation needs to distinguish a
    /// deadline from a value returned by the future.
    pub fn offload_or<F, T, C>(
        &mut self,
        deadline: Duration,
        future: F,
        fallback: T,
        continuation: C,
    ) -> OffloadHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(T) -> M + Send + 'static,
    {
        self.offload(deadline, future, move |outcome| {
            continuation(outcome.unwrap_or(fallback))
        })
    }

    /// Runs a bounded future without blocking this actor's receive loop and
    /// returns its total outcome to this actor's receive loop as an ordinary
    /// message.
    ///
    /// This is the lower-level form of [`Self::offload_or`]. The continuation is
    /// total: it receives either the future's value or [`OffloadDeadline`] and
    /// must produce a message in both cases. Completion ordering relative to
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
    pub fn offload<F, T, C>(
        &mut self,
        deadline: Duration,
        future: F,
        continuation: C,
    ) -> OffloadHandle
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
        OffloadHandle { abort, cancelled }
    }

    pub(crate) fn close_external_intake(&mut self) {
        self.mailbox.close_external();
    }

    pub(crate) fn abort_offloads(&mut self) {
        self.offloads.abort_all();
    }

    /// Returns the actor's unique identifier within the graph.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the shared graph shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// Returns the actor-aware handle for this actor's enclosing scope.
    ///
    /// Awaiting control operations on the enclosing scope is safe. The
    /// remaining self-deadlock is awaiting removal of a sibling whose drain
    /// depends on this actor draining its own mailbox; pipeline that operation
    /// with [`offload`](Self::offload) instead.
    ///
    /// Do not await this scope's `wait_started()` from
    /// [`Actor::on_start`](crate::Actor::on_start): this actor cannot report
    /// ready until `on_start` returns, so the wait depends on itself. Pipeline
    /// the wait and consume its result after startup instead.
    ///
    /// Actors run directly through
    /// [`RunnableActor::run_until`](crate::RunnableActor::run_until), outside a
    /// supervisor, receive a terminal handle here. Its control operations
    /// return [`ControlError::Unavailable`](crate::ControlError::Unavailable)
    /// and its observation streams are closed.
    pub fn supervisor(&self) -> RuntimeHandle {
        self.supervisor.clone()
    }

    /// Returns the actor-aware handle for this leader's declared child scope.
    ///
    /// This is `Some` exactly for the leader of an
    /// [`ActorWithScope`](crate::SupervisionTree::ActorWithScope) node. Other
    /// actor shapes use ordinary builder-handle plumbing when they need a
    /// pre-spawn scope handle.
    ///
    /// The child scope starts only after its leader's `on_start` returns. A
    /// leader must therefore not await `children().wait_started()` inline from
    /// `on_start`; launch that wait as pipelined work, return from `on_start`,
    /// and consume the result after the child scope binds.
    pub fn children(&self) -> Option<RuntimeHandle> {
        self.children.clone()
    }

    /// Returns `true` if graph shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Returns an observe-only view of this actor incarnation's lifetime.
    pub fn lifetime(&self) -> Lifetime {
        self.lifetime.observe()
    }

    /// Returns a sender targeting this actor's own mailbox.
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
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
    /// [`RawActor::run`](crate::RawActor::run) implementations: after
    /// [`recv`](Self::recv) returns `None` because shutdown was requested,
    /// queued messages remain readable here.
    ///
    /// A returned [`TryRecvError::Empty`] means no message is immediately
    /// available; it does not prove the mailbox is fully drained while senders
    /// hold permits. For typical actors, prefer
    /// [`Actor`](crate::Actor) with
    /// [`DrainPolicy::Drain`](crate::DrainPolicy) so the framework owns the
    /// drain loop.
    ///
    /// A panic in an [`offload`](Self::offload) future or continuation resumes
    /// here, on the actor task.
    pub fn try_recv(&mut self) -> Result<M, TryRecvError> {
        let message = self.try_delivery().map_err(|error| match error {
            tokio::sync::mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            tokio::sync::mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
        });
        if message.is_ok() {
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
    /// [`RunnableActor::run_until`](crate::RunnableActor::run_until), or the
    /// supervised child's [`ShutdownPolicy`](crate::ShutdownPolicy) grace.
    /// Once that bound aborts the actor task, the blocking thread continues
    /// detached because Tokio blocking tasks cannot be aborted after they start.
    ///
    /// For detached or concurrent work, clone [`myself`](Self::myself), call
    /// [`tokio::task::spawn_blocking`] directly, and send the outcome back as a
    /// message. The mailbox then acts as the completion mechanism; see the
    /// [`blocking_lifecycle` example](https://github.com/ralexstokes/tokio-otp/blob/main/crates/tokio-otp/examples/blocking_lifecycle.rs).
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
        self.myself.set_outstanding_offloads(0);
    }
}

mod sealed {
    pub trait Sealed<M> {
        fn cx(&self) -> &super::ActorContext<M>;
        fn cx_mut(&mut self) -> &mut super::ActorContext<M>;
    }
}

/// The capabilities an actor has while its incarnation is still live,
/// independent of which lifecycle stage it is in.
///
/// Live is the whole condition: every capability here ends in a delivery to
/// this incarnation, so the trait covers exactly the stages that still have
/// someone to deliver to. Implemented by [`StartContext`] and
/// [`MessageContext`]. It is the type a shared helper should take when it is
/// called from both `on_start` and `handle`:
///
/// ```no_run
/// use tokio_otp::LiveContext;
/// use std::time::Duration;
///
/// # enum Msg { Tick }
/// const TICK: tokio_otp::TimerKey = tokio_otp::TimerKey::new("tick");
/// fn arm(ctx: &mut impl LiveContext<Msg>) {
///     ctx.set_timeout(TICK, Msg::Tick, Duration::from_secs(5));
/// }
/// ```
///
/// [`StopContext`] deliberately does not implement it: after the receive loop
/// has exited, nothing here has anyone left to deliver to.
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

    /// Returns `true` if graph shutdown has been requested.
    fn is_shutting_down(&self) -> bool {
        self.cx().is_shutting_down()
    }

    /// Returns an observe-only view of this actor incarnation's lifetime.
    fn lifetime(&self) -> Lifetime {
        self.cx().lifetime()
    }

    /// Queues follow-up work as the actor's next message.
    ///
    /// Continuations are taken ahead of the mailbox on every iteration of the
    /// provided receive loop, so they are a priority self-send that does not
    /// consume mailbox capacity. Calls made from
    /// [`Actor::on_start`](crate::Actor::on_start) are processed after startup
    /// readiness is reported and before ordinary mailbox messages, which keeps
    /// expensive warm-up work out of the readiness-critical initialization
    /// path.
    ///
    /// Continuations count as received messages in
    /// [`ActorStats`](crate::ActorStats), but not as externally accepted
    /// mailbox messages. They are abandoned once the actor begins stopping,
    /// which is why [`StopContext`] is outside this trait.
    ///
    /// Two stopping paths still reach this method, because they run in a
    /// context that can queue work at other times: a handler called on the
    /// drain path, and an [`on_start`](crate::Actor::on_start) that returns
    /// [`Flow::Stop`](crate::Flow). Continuations queued there are dropped
    /// with the incarnation. The provided receive loop cannot refuse them at
    /// compile time, so it emits a `WARN` naming the actor and the number
    /// dropped before `on_stop` runs.
    ///
    /// A handler that wants to avoid queueing work the drain will throw away
    /// can ask [`MessageContext::is_draining`] first — the drain path is the
    /// one of the two that is not visible from the type.
    fn continue_with(&mut self, message: M) {
        self.cx_mut().push_continuation(message);
    }

    /// Watches the target logical actor across restarts.
    ///
    /// See [`ActorContext::watch`].
    fn watch<T, F>(&self, target: &ActorRef<T>, map: F) -> CancellationHandle
    where
        T: Send + 'static,
        F: FnMut(MonitorEvent) -> M + Send + 'static,
    {
        self.cx().watch(target, map)
    }

    /// Arms a keyed one-shot timeout, replacing the timeout at the same key.
    ///
    /// The timeout is owned by the actor loop rather than sent through its
    /// mailbox. Replacement and [`clear_timeout`](Self::clear_timeout) are
    /// therefore exact until delivery. Timeouts at other keys are unchanged.
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

    /// Schedules an anonymous one-shot message and returns its exact
    /// cancellation handle.
    ///
    /// Self-timer delivery bypasses mailbox capacity and conflation, counts as
    /// a received message, and is cancelled structurally on restart.
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
    ///
    /// The first message is delivered after one full period. Each delivery
    /// arms the next one, so missed ticks never pile up. A zero period returns
    /// an already-cancelled handle and sends no messages.
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

    /// Runs a bounded future and substitutes `fallback` when its deadline
    /// expires.
    ///
    /// See [`ActorContext::offload_or`].
    fn offload_or<F, T, C>(
        &mut self,
        deadline: Duration,
        future: F,
        fallback: T,
        continuation: C,
    ) -> OffloadHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(T) -> M + Send + 'static,
    {
        self.cx_mut()
            .offload_or(deadline, future, fallback, continuation)
    }

    /// Runs a bounded future without blocking this actor's receive loop.
    ///
    /// See [`ActorContext::offload`].
    fn offload<F, T, C>(&mut self, deadline: Duration, future: F, continuation: C) -> OffloadHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        self.cx_mut().offload(deadline, future, continuation)
    }

    /// Runs blocking work on Tokio's blocking pool.
    ///
    /// See [`ActorContext::run_blocking`].
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
        }

        impl<A: Actor + ?Sized> LiveContext<A::Msg> for $view<'_, A> {}
    };
}

/// A lifecycle-restricted scope handle as seen from
/// [`Actor::on_start`](crate::Actor::on_start) and
/// [`Actor::on_stop`](crate::Actor::on_stop).
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
/// nested scope's shutdown is sequenced with this one's. The raw
/// `SupervisorHandle` — one method call away from the same waits — is not
/// reachable from here at all.
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
/// returns, and insertion schedules rather than waits. When a lifecycle wait
/// must happen, take the full handle with [`release`](Self::release) and move
/// it into work that runs after startup or outlives the stopping incarnation.
#[derive(Clone, Debug)]
pub struct RestrictedScope {
    handle: RuntimeHandle,
}

impl RestrictedScope {
    fn new(handle: RuntimeHandle) -> Self {
        Self { handle }
    }

    /// Returns a point-in-time snapshot of the scope.
    pub fn snapshot(&self) -> tokio_supervisor::SupervisorSnapshot {
        self.handle.snapshot()
    }

    /// Returns per-actor message counters for this scope.
    pub fn actor_stats(&self) -> Vec<ActorStats> {
        self.handle.actor_stats()
    }

    /// Subscribes to scope snapshots.
    pub fn subscribe_snapshots(&self) -> watch::Receiver<tokio_supervisor::SupervisorSnapshot> {
        self.handle.subscribe_snapshots()
    }

    /// Returns a handle to a nested subtree by id, restricted the same way as
    /// this one.
    pub fn subtree(&self, id: &str) -> Option<Self> {
        self.handle.subtree(id).map(Self::new)
    }

    /// Inserts an actor into this scope.
    ///
    /// Safe to await here: insertion schedules startup rather than waiting for
    /// it, so it does not block on another child's lifecycle. See
    /// [`RuntimeHandle::add_actor`].
    pub async fn add_actor<F>(
        &self,
        label: impl Into<String>,
        factory: F,
        options: crate::DynamicActorOptions<<F::Actor as crate::RawActor>::Msg>,
    ) -> Result<ActorRef<<F::Actor as crate::RawActor>::Msg>, tokio_supervisor::ControlError>
    where
        F: crate::ActorFactory,
    {
        self.handle.add_actor(label, factory, options).await
    }

    /// Inserts a subtree into this scope.
    ///
    /// The subtree must already be reserved because reservation is fallible
    /// and carries any handles taken before insertion. Call
    /// [`SupervisionTree::reserve`](crate::SupervisionTree::reserve) explicitly
    /// for a plain declaration.
    ///
    /// Safe to await here for the same reason as [`add_actor`](Self::add_actor).
    /// See [`RuntimeHandle::add_subtree`].
    pub async fn add_subtree(
        &self,
        id: impl Into<String>,
        tree: impl Into<crate::ReservedSupervisionTree>,
    ) -> Result<RuntimeHandle, crate::AddSubtreeError> {
        self.handle.add_subtree(id, tree).await
    }

    /// Observes lifecycle transitions of this scope's direct children.
    pub fn watch_lifecycle(&self) -> tokio_supervisor::LifecycleWatch {
        self.handle.watch_lifecycle()
    }

    /// Observes lifecycle transitions of this scope and everything beneath it.
    pub fn watch_lifecycle_recursive(&self) -> tokio_supervisor::LifecycleWatch {
        self.handle.watch_lifecycle_recursive()
    }

    /// Pumps direct-child lifecycle events into `target` using its ordinary
    /// mailbox policy.
    ///
    /// The detached pump is safe to start from a lifecycle hook: it does not
    /// wait for this scope or any child to transition. See
    /// [`RuntimeHandle::watch_lifecycle_to`].
    pub fn watch_lifecycle_to<M, F>(
        &self,
        target: &ActorRef<M>,
        map: F,
    ) -> crate::LifecycleWatchGuard
    where
        M: Send + 'static,
        F: FnMut(tokio_supervisor::LifecycleEvent) -> M + Send + 'static,
    {
        self.handle.watch_lifecycle_to(target, map)
    }

    /// Requests shutdown of this scope without waiting for it.
    pub fn shutdown(&self) {
        self.handle.shutdown()
    }

    /// Releases the full [`RuntimeHandle`] for work outside the current
    /// lifecycle hook.
    ///
    /// During `on_start`, move the returned handle into a spawned or offloaded
    /// future that runs after startup. During `on_stop`, move it into work that
    /// outlives the incarnation. Awaiting lifecycle operations inline is the
    /// deadlock this type exists to make explicit.
    pub fn release(self) -> RuntimeHandle {
        self.handle
    }
}

/// Context handed to [`Actor::on_start`](crate::Actor::on_start).
///
/// Adds [`continue_with`](LiveContext::continue_with) to the ambient
/// capabilities and narrows the scope handles to [`RestrictedScope`], which
/// withholds the lifecycle waits that would deadlock an actor that has not
/// reported ready.
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
/// The ambient capabilities plus [`continue_with`](LiveContext::continue_with)
/// and full scope handles. The mailbox is absent because the provided receive
/// loop owns it; a handler that reads it directly would bypass drain accounting
/// and the continuation queue.
///
/// This is the only hook the provided loop calls from two different phases, so
/// it is also the only one that has to say which: see
/// [`is_draining`](Self::is_draining).
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
/// handles, and [`run_blocking`](Self::run_blocking) for synchronous teardown.
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
live_context!(MessageContext);

impl<'a, A: Actor + ?Sized> StartContext<'a, A> {
    pub(crate) fn new(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self { cx }
    }

    /// Returns this actor's enclosing scope, restricted for the startup stage.
    ///
    /// See [`RestrictedScope`] for why the lifecycle waits are withheld here
    /// and how to pipeline one that must happen.
    pub fn supervisor(&self) -> RestrictedScope {
        RestrictedScope::new(self.cx.supervisor())
    }

    /// Returns this leader's declared child scope, restricted for the startup
    /// stage.
    ///
    /// The child scope starts only after this hook returns, so awaiting its
    /// readiness inline can never succeed. See [`RestrictedScope`].
    pub fn children(&self) -> Option<RestrictedScope> {
        self.cx.children().map(RestrictedScope::new)
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

    /// Returns `true` when this `handle` call comes from the drain phase
    /// rather than from ordinary message handling.
    ///
    /// The provided receive loop calls `handle` from two phases. Ordinarily it
    /// is pulling from a live mailbox and the actor keeps running afterwards.
    /// Once the loop has exited and external intake is closed,
    /// [`DrainPolicy::Drain`](crate::DrainPolicy) replays what is already
    /// queued — mailbox messages and offload completions — and this returns
    /// `true` for every one of those calls. Nothing follows the drain except
    /// [`on_stop`](crate::Actor::on_stop), so work a handler defers here is
    /// work that will not happen: continuations are dropped, new timers and
    /// intervals never fire, and a fresh
    /// [`offload`](LiveContext::offload) is racing the shutdown budget.
    ///
    /// This is not [`is_shutting_down`](LiveContext::is_shutting_down), and
    /// the difference is the reason it exists. A drain also follows the
    /// actor's own [`Flow::Stop`](crate::Flow), where the graph is not
    /// shutting down at all and `is_shutting_down` is `false` throughout.
    /// Conversely a handler can observe `is_shutting_down` as `true` while
    /// still on the ordinary path, when shutdown is requested during an
    /// in-flight call. Ask this when the question is "will anything I queue be
    /// run"; ask `is_shutting_down` when the question is about the graph.
    pub fn is_draining(&self) -> bool {
        self.draining
    }

    /// Returns the actor-aware handle for this actor's enclosing scope.
    ///
    /// See [`ActorContext::supervisor`].
    pub fn supervisor(&self) -> RuntimeHandle {
        self.cx.supervisor()
    }

    /// Returns the actor-aware handle for this leader's declared child scope.
    ///
    /// See [`ActorContext::children`].
    pub fn children(&self) -> Option<RuntimeHandle> {
        self.cx.children()
    }
}

impl<'a, A: Actor + ?Sized> StopContext<'a, A> {
    pub(crate) fn new(cx: &'a mut ActorContext<A::Msg>) -> Self {
        Self { cx }
    }

    /// Returns the actor's unique identifier within the graph.
    pub fn id(&self) -> &str {
        self.cx.id()
    }

    /// Returns a sender targeting this actor's own mailbox.
    ///
    /// The mailbox is no longer being read by this incarnation. This is here
    /// so teardown can hand the ref to something else, not so the actor can
    /// post to itself.
    pub fn myself(&self) -> ActorRef<A::Msg> {
        self.cx.myself()
    }

    /// Returns the shared graph shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        self.cx.shutdown_token()
    }

    /// Returns `true` if graph shutdown has been requested.
    ///
    /// This is `false` when the actor is stopping on its own
    /// [`Flow::Stop`](crate::Flow) rather than on graph shutdown.
    pub fn is_shutting_down(&self) -> bool {
        self.cx.is_shutting_down()
    }

    /// Returns this actor's enclosing scope, restricted for the shutdown stage.
    ///
    /// See [`RestrictedScope`] for why the lifecycle waits are withheld here
    /// and where teardown that needs one belongs instead.
    pub fn supervisor(&self) -> RestrictedScope {
        RestrictedScope::new(self.cx.supervisor())
    }

    /// Returns this leader's declared child scope, restricted for the shutdown
    /// stage.
    ///
    /// The child scope is torn down around this hook, so awaiting its
    /// completion inline deadlocks the same way. See [`RestrictedScope`].
    pub fn children(&self) -> Option<RestrictedScope> {
        self.cx.children().map(RestrictedScope::new)
    }

    /// Runs blocking teardown work on Tokio's blocking pool.
    ///
    /// See [`ActorContext::run_blocking`].
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

fn send_rejection(error: &SendError) -> SendRejection {
    match error {
        SendError::ActorNotRunning { .. } => SendRejection::NotRunning,
        SendError::ActorTerminated { .. } => SendRejection::ActorTerminated,
        SendError::MailboxFull { .. } => SendRejection::MailboxFull,
        SendError::MailboxClosed { .. } => SendRejection::MailboxClosed,
    }
}
