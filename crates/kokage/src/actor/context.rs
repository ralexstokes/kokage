use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{
    __private::{guard_from_tokens, guard_from_tokens_with_cancel},
    CancelOnDrop, CancellationToken, CompletionOnDrop, Guard,
};
use tokio::{
    sync::{oneshot, watch},
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior, sleep_until, timeout},
};

use crate::ScopeRef;

use crate::actor::{
    binding::{
        ActorStats, ActorStatsCounters, BindingCore, BindingState, MailboxReceiver, MailboxRef,
        MessageSizeObserver, SendOutcome, TimedSendOutcome,
    },
    error::{BlockingCancelled, CallError, OffloadDeadline, SendError, SendErrorKind},
    handler::Actor,
    monitor::{ActorMonitors, MonitorEvent, MonitorHub},
    observability::{MessageOperation, MessageRejection, ScopeObservability, trace_actor_message},
};

/// Cloneable, restart-stable, typed sender for an actor mailbox.
///
/// An `ActorRef<M>` is bound to a long-lived mailbox binding rather than a
/// single actor runtime instance. When the target actor is restarted (either
/// as part of a group restart or via per-actor supervision), the handle
/// transparently follows the new mailbox. That binding belongs to one
/// supervisor membership: removing a dynamic actor terminates its refs, and
/// adding another actor under the same id mints a fresh binding. A stale ref
/// therefore never delivers to the replacement membership.
pub struct ActorRef<M> {
    identity: Arc<()>,
    actor_id: Arc<str>,
    binding: watch::Receiver<BindingState<M>>,
    stats: Arc<ActorStatsCounters>,
    message_size: Arc<OnceLock<MessageSizeObserver<M>>>,
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
            message_size: Arc::clone(&self.message_size),
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
        message_size: Arc<OnceLock<MessageSizeObserver<M>>>,
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

    /// Returns the target actor id.
    pub fn id(&self) -> &str {
        &self.actor_id
    }

    /// Returns a point-in-time snapshot of this actor's message counters and
    /// current mailbox usage.
    ///
    /// A ref has no enclosing runtime context, so
    /// [`ActorStats::scope_path`] and [`ActorStats::lineage`] are `None`.
    /// Mailbox depth and capacity are zero while the ref is unbound between
    /// incarnations or permanently terminated.
    pub fn stats(&self) -> ActorStats {
        let (depth, capacity) = match &*self.binding.borrow() {
            BindingState::Bound(mailbox) => mailbox.usage(),
            BindingState::Unbound | BindingState::Terminated => (0, 0),
        };
        self.stats.snapshot(&self.actor_id, depth, capacity)
    }

    /// Sends a message to the target actor.
    ///
    /// This waits until the actor has a bound mailbox, waits for capacity when
    /// the actor uses a FIFO queue, and rides through restart windows when the
    /// actor is expected to rebind. Conflating mailboxes replace stale unread
    /// state immediately instead of waiting for capacity. This returns an
    /// error only when the actor has terminated with no restart scheduled, or
    /// when the binding source has been dropped. This method therefore returns
    /// only [`SendErrorKind::Terminated`], with the unaccepted message available
    /// through [`SendError::into_message`].
    ///
    /// Cancelling this future while it is waiting drops the message. Use
    /// [`send_timeout`](Self::send_timeout) when a bounded wait must return an
    /// unaccepted message.
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
    pub async fn send(&self, message: M) -> Result<(), SendError<M>> {
        self.send_to_incarnation(message).await.map(drop)
    }

    /// Sends a message with a bounded asynchronous wait for mailbox acceptance.
    ///
    /// Like [`send`](Self::send), this waits through restart windows and FIFO
    /// mailbox capacity pressure. If the bound expires first, the message is
    /// returned with [`SendErrorKind::TimedOut`]. If the target membership
    /// terminates first, it is returned with [`SendErrorKind::Terminated`]. An
    /// `Ok` result has the same at-most-once acceptance contract as `send`.
    ///
    /// The deadline is checked before the first acceptance attempt, so a zero
    /// bound always returns [`SendErrorKind::TimedOut`]; use
    /// [`try_send`](Self::try_send) for one immediate attempt. A nonzero bound
    /// shorter than the runtime's timer tick can expire on that same tick and
    /// likewise should not be relied on to permit an attempt, particularly
    /// when passing a computed remaining budget. The bound covers asynchronous
    /// waits for binding and capacity. It cannot preempt synchronous user code
    /// such as a keyed-conflation matcher, but the deadline is rechecked after
    /// that code and the message is not accepted late.
    ///
    /// This is a delivery primitive rather than a convenience wrapper around
    /// [`tokio::time::timeout`]. Cancelling a `send` future drops the message
    /// it owns, so `timeout(actor.send(message))` cannot recover the message
    /// when its bound expires. Cancelling this `send_timeout` future also drops
    /// its message; ownership is recovered only when the future completes with
    /// an error.
    pub async fn send_timeout(&self, message: M, bound: Duration) -> Result<(), SendError<M>> {
        let deadline = deadline_after(bound);
        let mut binding = self.binding.clone();
        let mut message = message;

        loop {
            let mailbox = tokio::select! {
                biased;
                () = sleep_until(deadline) => {
                    self.observe_send(
                        MessageOperation::SendTimeout,
                        Some(MessageRejection::Timeout),
                    );
                    self.stats.record_send(false);
                    return Err(self.send_error(message, SendErrorKind::TimedOut));
                }
                mailbox = self.wait_for_next_mailbox(&mut binding) => match mailbox {
                    Ok(mailbox) => mailbox,
                    Err(()) => {
                        self.observe_send(
                            MessageOperation::SendTimeout,
                            Some(MessageRejection::ActorTerminated),
                        );
                        self.stats.record_send(false);
                        return Err(self.send_error(message, SendErrorKind::Terminated));
                    }
                }
            };
            // Materialization installs the observer before binding the first
            // mailbox. Read it only after resolving a live binding so a send
            // polled while the declaration is configurable cannot miss sizing.
            let message_size = self
                .message_size
                .get()
                .map(|observer| observer.size_hint(&message));

            match mailbox.send_retaining_until(message, deadline).await {
                TimedSendOutcome::Accepted { conflated } => {
                    self.observe_send(MessageOperation::SendTimeout, None);
                    self.stats.record_send(true);
                    self.stats.record_conflated(conflated);
                    self.record_message_size(message_size);
                    return Ok(());
                }
                TimedSendOutcome::Closed(returned) => {
                    self.observe_send(
                        MessageOperation::SendTimeout,
                        Some(MessageRejection::MailboxClosed),
                    );
                    message = returned;
                    let rebound = tokio::select! {
                        biased;
                        () = sleep_until(deadline) => {
                            self.observe_send(
                                MessageOperation::SendTimeout,
                                Some(MessageRejection::Timeout),
                            );
                            self.stats.record_send(false);
                            return Err(self.send_error(message, SendErrorKind::TimedOut));
                        }
                        rebound = self.wait_for_rebind_or_termination(&mut binding, &mailbox) => {
                            rebound
                        }
                    };
                    if rebound.is_err() {
                        self.observe_send(
                            MessageOperation::SendTimeout,
                            Some(MessageRejection::ActorTerminated),
                        );
                        self.stats.record_send(false);
                        return Err(self.send_error(message, SendErrorKind::Terminated));
                    }
                }
                TimedSendOutcome::Timeout(returned) => {
                    self.observe_send(
                        MessageOperation::SendTimeout,
                        Some(MessageRejection::Timeout),
                    );
                    self.stats.record_send(false);
                    return Err(self.send_error(returned, SendErrorKind::TimedOut));
                }
            }
        }
    }

    /// Sends a message and returns the incarnation mailbox that accepted it.
    ///
    /// This is used by runtime adapters that need to restore cumulative state
    /// after the target actor moves to a fresh incarnation.
    pub(crate) async fn send_to_incarnation(
        &self,
        message: M,
    ) -> Result<MailboxRef<M>, SendError<M>> {
        let mut binding = self.binding.clone();
        let mut message = message;

        loop {
            let mailbox = match self.wait_for_next_mailbox(&mut binding).await {
                Ok(mailbox) => mailbox,
                Err(()) => {
                    self.observe_send(
                        MessageOperation::Send,
                        Some(MessageRejection::ActorTerminated),
                    );
                    self.stats.record_send(false);
                    return Err(self.send_error(message, SendErrorKind::Terminated));
                }
            };
            // Materialization installs the observer before binding the first
            // mailbox. Read it only after that bind so a send polled while the
            // declaration is still configurable cannot miss late sizing.
            let message_size = self
                .message_size
                .get()
                .map(|observer| observer.size_hint(&message));

            match mailbox.send_retaining(message).await {
                SendOutcome::Accepted { conflated } => {
                    self.observe_send(MessageOperation::Send, None);
                    self.stats.record_send(true);
                    self.stats.record_conflated(conflated);
                    self.record_message_size(message_size);
                    return Ok(mailbox);
                }
                SendOutcome::Closed(returned) => {
                    self.observe_send(
                        MessageOperation::Send,
                        Some(MessageRejection::MailboxClosed),
                    );
                    message = returned;
                    if let Err(()) = self
                        .wait_for_rebind_or_termination(&mut binding, &mailbox)
                        .await
                    {
                        self.observe_send(
                            MessageOperation::Send,
                            Some(MessageRejection::ActorTerminated),
                        );
                        self.stats.record_send(false);
                        return Err(self.send_error(message, SendErrorKind::Terminated));
                    }
                }
            }
        }
    }

    /// Attempts to send a message without waiting for mailbox capacity.
    ///
    /// An actor without a live incarnation returns
    /// [`SendErrorKind::NotRunning`], a full FIFO queue returns
    /// [`SendErrorKind::Full`], and terminal membership returns
    /// [`SendErrorKind::Terminated`]. A conflating mailbox instead accepts the
    /// message and replaces stale unread state. This operation never returns
    /// [`SendErrorKind::TimedOut`]. Every rejection returns the message through
    /// [`SendError::into_message`].
    pub fn try_send(&self, message: M) -> Result<(), SendError<M>> {
        // Clone the state separately so the watch read guard is dropped before
        // tracing or statistics code runs on a rejection path.
        let binding = self.binding.borrow().clone();
        let mailbox = match binding {
            BindingState::Bound(mailbox) => mailbox,
            BindingState::Unbound if self.binding.has_changed().is_err() => {
                let error = self.send_error(message, SendErrorKind::Terminated);
                self.observe_send(
                    MessageOperation::TrySend,
                    Some(send_error_rejection(error.kind)),
                );
                self.stats.record_send(false);
                return Err(error);
            }
            BindingState::Unbound => {
                let error = self.send_error(message, SendErrorKind::NotRunning);
                self.observe_send(
                    MessageOperation::TrySend,
                    Some(send_error_rejection(error.kind)),
                );
                self.stats.record_send(false);
                return Err(error);
            }
            BindingState::Terminated => {
                let error = self.send_error(message, SendErrorKind::Terminated);
                self.observe_send(
                    MessageOperation::TrySend,
                    Some(send_error_rejection(error.kind)),
                );
                self.stats.record_send(false);
                return Err(error);
            }
        };
        // Materialization installs the observer before binding the first
        // mailbox. Resolve the mailbox first so a pre-materialization ref
        // cannot race that transition and miss sizing for an accepted send.
        let message_size = self
            .message_size
            .get()
            .map(|observer| observer.size_hint(&message));
        let result = mailbox.try_send(message);
        self.observe_send(
            MessageOperation::TrySend,
            result.as_ref().err().map(|failure| failure.rejection),
        );
        self.stats.record_send(result.is_ok());
        match result {
            Ok(conflated) => {
                self.stats.record_conflated(conflated);
                self.record_message_size(message_size);
                Ok(())
            }
            Err(failure) => Err(failure.error),
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
    /// [`Context::offload`], or move the slow dependency behind a dedicated
    /// child actor. The book's request/reply chapter covers the pattern.
    pub async fn call<T>(
        &self,
        timeout: Duration,
        message: impl FnOnce(Reply<T>) -> M,
    ) -> Result<T, CallError> {
        tokio::time::timeout(timeout, async {
            let (sender, receiver) = oneshot::channel();
            self.send(message(Reply { sender }))
                .await
                .map_err(|error| CallError::Terminated {
                    actor_id: error.actor_id,
                })?;
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
    ) -> Result<MailboxRef<M>, ()> {
        loop {
            match binding.borrow().clone() {
                BindingState::Bound(mailbox) => return Ok(mailbox),
                BindingState::Unbound => {}
                BindingState::Terminated => return Err(()),
            }

            binding.changed().await.map_err(drop)?;
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
    ) -> Result<(), ()> {
        loop {
            match binding.borrow().clone() {
                BindingState::Bound(current) if !current.same_channel(stale) => return Ok(()),
                BindingState::Bound(_) | BindingState::Unbound => {}
                BindingState::Terminated => return Err(()),
            }

            binding.changed().await.map_err(drop)?;
        }
    }

    fn send_error(&self, message: M, kind: SendErrorKind) -> SendError<M> {
        SendError {
            actor_id: self.actor_id.to_string(),
            message,
            kind,
        }
    }

    fn observe_send(&self, operation: MessageOperation, rejection: Option<MessageRejection>) {
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
                .get()
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

/// The lifecycle state visible through an actor context.
///
/// `Draining` takes precedence once a handler is replaying accepted work,
/// including when a local stop and runtime shutdown overlap. Runtime shutdown on
/// its own does not change this status; inspect [`Context::shutdown_token`]
/// when execution-wide cancellation matters.
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
    key: TimerKey,
    deadline: Instant,
    message: M,
}

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

    fn insert(&mut self, key: TimerKey, message: M, delay: Duration) {
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            self.entries.swap_remove(index);
        }
        let id = self.next_id();
        self.entries.push(TimerEntry {
            id,
            key,
            deadline: deadline_after(delay),
            message,
        });
    }

    fn clear(&mut self, key: TimerKey) {
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            self.entries.swap_remove(index);
        }
    }

    fn next_wake(&mut self) -> Option<TimerWake> {
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
        Some(self.entries.swap_remove(index).message)
    }
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

/// Runtime context passed to a [`RawActor`](crate::raw::RawActor) each time the
/// actor is run.
///
/// This is the widest context: a `RawActor` owns its receive loop, so it gets
/// the incoming [`mailbox`](Self::recv) and explicit
/// [`mark_ready`](Self::mark_ready) alongside the ambient capabilities —
/// an [`offload`](Self::offload) primitive for bounded asynchronous work, a
/// [`shutdown_token`](Self::shutdown_token) for cooperative shutdown, and
/// [`run_blocking`](Self::run_blocking) for blocking work.
///
/// Handler-style [`Actor`](crate::Actor) implementations do not see this type.
/// The framework owns their loop and hands live hooks a [`Context`] and the
/// shutdown hook a [`StopContext`]. Those views omit what the stage cannot act
/// on, so mailbox-stealing `recv` calls and no-op `continue_with` calls are
/// compile errors rather than silent misbehavior.
pub struct RawContext<M> {
    pub(crate) id: Arc<str>,
    pub(crate) mailbox: MailboxReceiver<M>,
    pub(crate) myself: ActorRef<M>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) drain_messages: bool,
    pub(crate) observability: ScopeObservability,
    pub(crate) timers: TimerTable<M>,
    pub(crate) lifetime: ActorLifetime,
    pub(crate) monitors: Arc<ActorMonitors>,
    pub(crate) ready: Option<oneshot::Sender<()>>,
    pub(crate) continuations: VecDeque<M>,
    pub(crate) stop_requested: bool,
    pub(crate) offloads: JoinSet<OffloadCompletion<M>>,
    pub(crate) supervisor: ScopeRef,
}

impl<M: Send + 'static> RawContext<M> {
    /// Reports that a custom [`RawActor`](crate::raw::RawActor) has completed
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
    /// fails, restarts, or uses
    /// [`Shutdown::discard_after_current`](crate::Shutdown::discard_after_current).
    /// A draining handler actor keeps processing queued messages and offload
    /// completions until both are exhausted; the required deadline bounds
    /// every offload's future during that drain.
    ///
    /// Cancelling or timing out an offload is not undo. If the future sent a request
    /// before being dropped, the receiver may still perform it and the outcome
    /// is unknown. Put effects behind actors and use idempotency keys plus
    /// reconciliation; offload futures should initiate requests, not mutate
    /// untracked local state directly. Domain cancellation can still be
    /// captured explicitly in `future`.
    ///
    /// Panics in the future or continuation resume on the actor task, so
    /// supervision treats them like an ordinary actor panic.
    ///
    /// Dropping the returned [`Guard`] cancels the offload and suppresses its
    /// continuation message. Call [`Guard::detach`] for explicit
    /// fire-and-forget ownership.
    pub fn offload<F, T, C>(&mut self, deadline: Duration, future: F, continuation: C) -> Guard
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let (finished, finished_on_drop) = CompletionOnDrop::armed();
        let abort = self.offloads.spawn(async move {
            let _finished_on_drop = finished_on_drop;
            OffloadCompletion {
                message: continuation(timeout(deadline, future).await.map_err(|_| OffloadDeadline)),
                cancelled: task_cancelled,
            }
        });
        self.sync_offload_gauge();
        let cancellation = CancellationToken::new();
        let guard_abort = abort.clone();
        guard_from_tokens_with_cancel(cancellation, finished, move || {
            cancelled.store(true, Ordering::Release);
            guard_abort.abort();
        })
    }

    pub(crate) fn close_external_intake(&mut self) {
        self.mailbox.close_external();
    }

    pub(crate) fn abort_offloads(&mut self) {
        self.offloads.abort_all();
    }

    /// Returns the actor's identifier within its scope.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the shared execution shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// Returns this actor's enclosing scope.
    ///
    /// Awaiting scope or child lifecycle progress can deadlock when that
    /// progress depends on this actor returning from its current work.
    /// Actors run directly through
    /// [`ActorHost::run_once`](crate::raw::ActorHost::run_once) or
    /// [`ActorHost::run_incarnation`](crate::raw::ActorHost::run_incarnation),
    /// outside a supervisor, receive a terminal handle here. Its control
    /// operations return
    /// [`ControlError::Unavailable`](crate::ControlError::Unavailable) and its
    /// observation streams are closed.
    pub fn scope(&self) -> ScopeRef {
        self.supervisor.clone()
    }

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

    /// Sends `message` to this actor after `delay` has elapsed.
    ///
    /// Unlike handler actors' [`Context::set_timeout`] facility, which raw actors
    /// do not have, this uses ordinary mailbox delivery: mailbox capacity and
    /// conflation apply, and successful delivery increments accepted-message
    /// statistics. The timer is independently owned by its returned [`Guard`];
    /// it has no key for exact replacement or retraction.
    ///
    /// The timer belongs to this actor incarnation and ends if the incarnation
    /// stops or restarts. Dropping the returned [`Guard`] cancels delivery; call
    /// [`Guard::detach`] to leave it running.
    pub fn send_after(&self, message: M, delay: Duration) -> Guard {
        self.send_after_to(&self.myself, message, delay)
    }

    /// Sends `message` to `target` after `delay` has elapsed.
    ///
    /// The timer belongs to this scheduling actor incarnation, not the target.
    /// It ends if this incarnation stops or restarts. Delivery uses an
    /// ordinary awaited [`ActorRef::send`], including the target mailbox's
    /// capacity and conflation behavior.
    ///
    /// Dropping the returned [`Guard`] cancels delivery; call
    /// [`Guard::detach`] to leave it running.
    pub fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> Guard {
        let cancellation = CancellationToken::new();
        let (finished, finished_on_drop) = CompletionOnDrop::armed();
        let task_cancellation = cancellation.clone();
        let lifetime = self.lifetime.token();
        let target = target.clone();

        let task = tokio::spawn(async move {
            let _finished_on_drop = finished_on_drop;
            tokio::select! {
                biased;
                () = task_cancellation.cancelled() => {}
                () = lifetime.cancelled() => {}
                () = tokio::time::sleep(delay) => {
                    tokio::select! {
                        biased;
                        () = task_cancellation.cancelled() => {}
                        () = lifetime.cancelled() => {}
                        _ = target.send(message) => {}
                    }
                }
            }
        });

        std::mem::drop(task);
        guard_from_tokens(cancellation, finished)
    }

    /// Sends a clone of `message` to this actor after every `period`.
    ///
    /// Delivery takes the ordinary mailbox path, so capacity, conflation, and
    /// accepted-message statistics apply. Missed ticks are skipped. The timer
    /// stops when cancelled or when this actor incarnation ends, including a
    /// restart or permanent termination. Dropping the returned [`Guard`] cancels
    /// it; call [`Guard::detach`] to leave it running. A zero period returns an
    /// already-finished guard and sends no messages.
    pub fn interval(&self, message: M, period: Duration) -> Guard
    where
        M: Clone,
    {
        self.interval_to(&self.myself, message, period)
    }

    /// Sends a clone of `message` to `target` after every `period`.
    ///
    /// Missed ticks are skipped. The timer stops when cancelled, when this
    /// scheduling incarnation ends, or when the target permanently terminates.
    /// Dropping the returned [`Guard`] cancels it; call [`Guard::detach`] to
    /// leave it running. A zero period returns an already-finished guard and
    /// sends no messages.
    pub fn interval_to<T: Clone + Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> Guard {
        let cancellation = CancellationToken::new();
        if period.is_zero() {
            let finished = CancellationToken::new();
            finished.cancel();
            return guard_from_tokens(cancellation, finished);
        }

        let task_cancellation = cancellation.clone();
        let (finished, finished_on_drop) = CompletionOnDrop::armed();
        let lifetime = self.lifetime.token();
        let target = target.clone();
        let task = tokio::spawn(async move {
            let _finished_on_drop = finished_on_drop;
            let start = deadline_after(period);
            let mut interval = tokio::time::interval_at(start, period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => break,
                    () = lifetime.cancelled() => break,
                    _ = interval.tick() => {
                        let sent = tokio::select! {
                            biased;
                            () = task_cancellation.cancelled() => break,
                            () = lifetime.cancelled() => break,
                            sent = target.send(message.clone()) => sent,
                        };
                        if sent.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        std::mem::drop(task);
        guard_from_tokens(cancellation, finished)
    }

    /// Watches the target logical actor across restarts.
    ///
    /// Each lifecycle transition of the target is converted by `map` into
    /// this actor's message type and delivered through this actor's mailbox,
    /// in lifecycle order: [`MonitorEvent::Started`] when an incarnation starts,
    /// [`MonitorEvent::Exited`] when it exits, and a final
    /// [`MonitorEvent::Removed`] when the target is permanently gone. A
    /// target that is already running delivers an immediate `Started` for the
    /// current incarnation; a target between incarnations stays silent until
    /// the next start, so a watch never races a supervisor restart.
    ///
    /// A watch belongs to the observing and watched actor memberships, not
    /// either current incarnation. It survives restarts on both sides and is
    /// delivered to whichever observer incarnation is running next. Calling
    /// `watch` again for the same pair, even within one incarnation, returns
    /// an alias of the existing watch without replacing its original `map`
    /// closure or emitting another immediate `Started`. Cancelling any alias
    /// cancels the pair. Explicit cancellation or permanent removal of either
    /// membership ends it.
    ///
    /// A replacement observer does not receive a fresh snapshot of the
    /// target. It must durably persist any observed state that it needs after
    /// a crash. To request a fresh snapshot instead, cancel the existing watch
    /// and register a new one: a running target delivers an immediate
    /// [`MonitorEvent::Started`], an already removed target delivers an
    /// immediate [`MonitorEvent::Removed`], and a target between incarnations
    /// stays silent until its next `Started`. Re-registering discards any
    /// transitions still staged on the old watch.
    ///
    /// Delivery uses the observer's ordinary mailbox policy. A conflating
    /// mailbox may replace an unread event with a later one, so use a FIFO
    /// mailbox when every transition must be observed. Undelivered events are
    /// staged in a bounded per-watch buffer, so an observer whose mailbox
    /// stays full while its target restarts in a tight loop cannot grow memory
    /// without bound. On overflow the oldest transitions are dropped and the
    /// loss surfaces as a [`MonitorEvent::Lagged`] resync marker rather than
    /// silently; the terminal `Removed` is never dropped.
    ///
    /// Dropping the returned [`Guard`] cancels the watch; call
    /// [`Guard::detach`] when membership ownership should keep it alive.
    /// Delivery of the terminal event finishes the guard without marking it
    /// cancelled.
    pub fn watch<T, F>(&self, target: &ActorRef<T>, mut map: F) -> Guard
    where
        T: Send + 'static,
        F: FnMut(MonitorEvent) -> M + Send + 'static,
    {
        let (cancellation, stop, finished, install) = self.monitors.register(&target.monitors);
        if !install {
            return guard_from_tokens(cancellation, finished.token());
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            finished.signal();
            return guard_from_tokens(cancellation, finished.token());
        };
        // The guard closes the queue on drop, so the hub stops staging events
        // whether this task exits normally or unwinds through a panicking
        // `map` closure.
        let guard =
            target
                .monitors
                .register_watch(cancellation.clone(), stop.clone(), finished.clone());
        let myself = self.myself();
        let task_cancellation = cancellation.clone();
        let task_stop = stop;
        runtime.spawn(async move {
            loop {
                // Registration can stage an immediate event before this task
                // is first polled. Observe a guard dropped in that window
                // before invoking the user mapper.
                if task_cancellation.is_cancelled() || task_stop.is_cancelled() {
                    break;
                }
                // Arm the wake-up before observing the queue so a push that
                // races an empty drain is not lost.
                let waiter = guard.queue().waiter();
                if let Some(event) = guard.queue().pop() {
                    let terminal = matches!(event, MonitorEvent::Removed { .. });
                    let message = map(event);
                    tokio::select! {
                        biased;
                        () = task_cancellation.cancelled() => break,
                        () = task_stop.cancelled() => break,
                        _ = myself.send(message) => {}
                    }
                    if terminal {
                        break;
                    }
                    continue;
                }
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => break,
                    () = task_stop.cancelled() => break,
                    _ = waiter => {}
                }
            }
        });

        guard_from_tokens(cancellation, finished.token())
    }

    /// Waits for the next mailbox message or offload completion, or `None`
    /// once shutdown has been requested or the mailbox has been closed.
    ///
    /// Shutdown is checked first: as soon as shutdown is requested this
    /// returns `None`, even when messages are still queued. Queued messages
    /// are dropped when the actor exits unless the actor drains them with
    /// [`try_recv`](Self::try_recv), or uses [`Actor`](crate::Actor)
    /// with [`Shutdown::drain_for`](crate::Shutdown::drain_for). Queued
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
    /// [`RawActor::run`](crate::raw::RawActor::run) implementations: after
    /// [`recv`](Self::recv) returns `None` because shutdown was requested,
    /// queued messages remain readable here.
    ///
    /// A returned `None` means no message is immediately available; it does
    /// not prove the mailbox is fully drained while senders hold permits. For
    /// typical actors, prefer
    /// [`Actor`](crate::Actor) with
    /// [`Shutdown::drain_for`](crate::Shutdown::drain_for) so the framework owns the
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
    /// [`ActorHost::run_once`](crate::raw::ActorHost::run_once),
    /// [`ActorHost::run_incarnation`](crate::raw::ActorHost::run_incarnation),
    /// or the
    /// supervised child's [`Shutdown`](crate::Shutdown) grace.
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
            let _cancel_on_drop = CancelOnDrop::new(cancellation.clone());
            let joined = tokio::task::spawn_blocking(move || f(&cancellation)).await;

            match joined {
                Ok(result) => Ok(result),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_) => Err(BlockingCancelled),
            }
        }
    }
}

impl<M> Drop for RawContext<M> {
    fn drop(&mut self) {
        self.myself.set_outstanding_offloads(0);
    }
}

/// Context handed to both [`Actor::on_start`](crate::Actor::on_start) and
/// [`Actor::handle`](crate::Actor::handle).
///
/// It exposes the live incarnation capabilities as inherent methods, including
/// timers, continuations, watches, and offloads. The mailbox is absent because
/// the provided receive loop owns it; reading it directly would bypass drain
/// accounting and the continuation queue.
///
/// [`status`](Context::status) reports `Running` during startup and ordinary
/// message handling, `Stopping` after a local stop request, and `Draining`
/// only while the framework replays accepted work during shutdown.
///
/// The parameter is the actor, not its message: a hook signature writes
/// `&mut Context<'_, Self>` and the message type is projected from
/// [`Actor::Msg`](crate::Actor::Msg).
///
/// A helper shared across actor types names that actor generically:
///
/// ```no_run
/// use kokage::{Actor, Context, TimerKey};
/// use std::time::Duration;
///
/// # enum Msg { Tick }
/// const TICK: TimerKey = TimerKey::new("tick");
/// fn arm<A: Actor<Msg = Msg> + ?Sized>(ctx: &mut Context<'_, A>) {
///     ctx.set_timeout(TICK, Msg::Tick, Duration::from_secs(5));
/// }
/// ```
pub struct Context<'a, A: Actor + ?Sized> {
    cx: &'a mut RawContext<A::Msg>,
    draining: bool,
}

impl<'a, A: Actor + ?Sized> Context<'a, A> {
    pub(crate) fn new(cx: &'a mut RawContext<A::Msg>) -> Self {
        Self {
            cx,
            draining: false,
        }
    }

    pub(crate) fn draining(cx: &'a mut RawContext<A::Msg>) -> Self {
        Self { cx, draining: true }
    }

    /// Returns the actor's identifier within its scope.
    pub fn id(&self) -> &str {
        self.cx.id()
    }

    /// Returns a sender targeting this actor's own mailbox.
    pub fn myself(&self) -> ActorRef<A::Msg> {
        self.cx.myself()
    }

    /// Returns the shared execution shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        self.cx.shutdown_token()
    }

    /// Returns this actor's enclosing scope.
    ///
    /// Awaiting scope or child lifecycle progress directly from a callback can
    /// deadlock when that progress depends on the callback returning; see
    /// [`ScopeRef::wait_started`](crate::ScopeRef::wait_started). Pass the
    /// wait to [`offload`](Self::offload) to run it outside the callback and
    /// receive its result as an ordinary message.
    pub fn scope(&self) -> ScopeRef {
        self.cx.scope()
    }

    /// Runs blocking work on Tokio's blocking pool.
    ///
    /// See [`RawContext::run_blocking`].
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

    /// Returns the lifecycle state of this live callback.
    ///
    /// The provided receive loop calls [`Actor::handle`](crate::Actor::handle)
    /// from two phases. Ordinary calls report [`ActorStatus::Running`] until
    /// this callback requests a local stop. Once the receive loop exits,
    /// [`Shutdown::drain_for`](crate::Shutdown::drain_for) replays already accepted
    /// mailbox messages and offload completions as [`ActorStatus::Draining`].
    /// Nothing follows the drain except
    /// [`on_stop`](crate::Actor::on_stop), so work deferred from that phase
    /// will not run: continuations are dropped, new timers and intervals never
    /// fire, and a fresh [`offload`](Self::offload) races the shutdown budget.
    /// A `Context` passed to `on_start` never reports `Draining`.
    ///
    /// This status is deliberately distinct from runtime shutdown. A local
    /// [`stop`](Self::stop) can lead to `Draining` while
    /// [`shutdown_token`](Self::shutdown_token) remains live; conversely,
    /// runtime shutdown requested during an in-flight ordinary callback cancels
    /// that token while this method still reports `Running`. Ask `status` when
    /// the question is whether work queued by this callback can run, and
    /// inspect the token when the question is about the runtime. `Draining`
    /// takes precedence when local stop and runtime shutdown overlap.
    pub fn status(&self) -> ActorStatus {
        if self.draining {
            ActorStatus::Draining
        } else {
            self.cx.live_status()
        }
    }

    /// Requests a clean stop of this actor incarnation.
    ///
    /// The request takes effect after the current `on_start` or `handle` call
    /// returns successfully. The provided receive loop then applies the
    /// actor's [`Shutdown`](crate::Shutdown), runs
    /// [`on_stop`](crate::Actor::on_stop), and reports a normal exit to
    /// monitoring and supervision. Returning an error from the same callback
    /// still fails the actor; the error takes precedence over this request.
    ///
    /// A startup request skips the ordinary receive loop but still reports
    /// readiness before clean shutdown, preserving the lifecycle boundary for
    /// ordered supervision. A handler whose [`status`](Self::status) is
    /// [`ActorStatus::Draining`] is already on the stop path, so another
    /// request there has no additional effect. Repeated calls are harmless.
    pub fn stop(&mut self) {
        self.cx.request_stop();
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
    /// [`ActorStats`](crate::observe::ActorStats), but not as externally
    /// accepted mailbox messages. They are abandoned once the actor begins
    /// stopping, which is why [`StopContext`] does not expose this method.
    ///
    /// Two stopping paths still reach this method: a handler called during
    /// [`ActorStatus::Draining`] and an `on_start` callback that also calls
    /// [`stop`](Self::stop). Continuations queued there are dropped with the
    /// incarnation. The provided receive loop emits a `WARN` naming the actor
    /// and the number dropped before `on_stop` runs.
    pub fn continue_with(&mut self, message: A::Msg) {
        self.cx.push_continuation(message);
    }

    /// Watches the target logical actor across restarts.
    ///
    /// See [`RawContext::watch`] for the full contract.
    pub fn watch<T, F>(&self, target: &ActorRef<T>, map: F) -> Guard
    where
        T: Send + 'static,
        F: FnMut(MonitorEvent) -> A::Msg + Send + 'static,
    {
        self.cx.watch(target, map)
    }

    /// Arms a keyed one-shot self timeout, replacing the timeout at the same key.
    ///
    /// The timeout is owned by the actor loop and never transits the mailbox.
    /// Mailbox capacity and conflation do not apply; delivery increments
    /// received-message statistics but not accepted-message statistics. Reusing
    /// `key` exactly replaces the pending entry, and
    /// [`clear_timeout`](Self::clear_timeout) exactly retracts it until delivery.
    /// Timeouts at other keys are unchanged.
    ///
    /// The timer table belongs to this actor incarnation: a stop or restart
    /// drops every pending entry, and an elapsed timeout is not delivered once
    /// the loop begins stopping, including during shutdown drain.
    pub fn set_timeout(&mut self, key: TimerKey, message: A::Msg, delay: Duration) {
        self.cx.timers.insert(key, message, delay);
    }

    /// Clears the timeout at `key`, if one is armed.
    pub fn clear_timeout(&mut self, key: TimerKey) {
        self.cx.timers.clear(key);
    }

    /// Sends `message` to `target` after `delay`, bound to this incarnation.
    ///
    /// See [`RawContext::send_after_to`] for the full contract.
    pub fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> Guard {
        self.cx.send_after_to(target, message, delay)
    }

    /// Periodically sends `message` to this actor, bound to this incarnation.
    ///
    /// See [`RawContext::interval`] for the full contract.
    pub fn interval(&self, message: A::Msg, period: Duration) -> Guard
    where
        A::Msg: Clone,
    {
        self.cx.interval(message, period)
    }

    /// Periodically sends `message` to `target`, bound to this incarnation.
    ///
    /// See [`RawContext::interval_to`] for the full contract.
    pub fn interval_to<T: Clone + Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> Guard {
        self.cx.interval_to(target, message, period)
    }

    /// Runs a bounded future without blocking this actor's receive loop.
    ///
    /// See [`RawContext::offload`] for the full contract.
    pub fn offload<F, T, C>(&mut self, deadline: Duration, future: F, continuation: C) -> Guard
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> A::Msg + Send + 'static,
    {
        self.cx.offload(deadline, future, continuation)
    }
}

/// Context handed to [`Actor::on_stop`](crate::Actor::on_stop).
///
/// A deliberately narrow surface. The hook runs after the receive loop has
/// exited and the mailbox has been drained or discarded, so anything that
/// queues future work for this incarnation — timers, intervals, watches,
/// offloads, continuations — has no one left to deliver to and
/// is withheld. What remains is identity, the shutdown token, the scope
/// handle, and [`run_blocking`](StopContext::run_blocking) for synchronous
/// teardown.
///
/// The parameter is the actor, not its message: a hook signature writes
/// `&mut StopContext<'_, Self>` and the message type is projected from
/// [`Actor::Msg`](crate::Actor::Msg).
pub struct StopContext<'a, A: Actor + ?Sized> {
    cx: &'a mut RawContext<A::Msg>,
}

impl<'a, A: Actor + ?Sized> StopContext<'a, A> {
    pub(crate) fn new(cx: &'a mut RawContext<A::Msg>) -> Self {
        Self { cx }
    }

    /// Returns the actor's identifier within its scope.
    pub fn id(&self) -> &str {
        self.cx.id()
    }

    /// Returns a sender targeting this actor's own mailbox.
    ///
    /// The mailbox is no longer read by this incarnation. Teardown can pass
    /// the ref elsewhere, but should not post work to itself.
    pub fn myself(&self) -> ActorRef<A::Msg> {
        self.cx.myself()
    }

    /// Returns the shared execution shutdown token.
    pub fn shutdown_token(&self) -> &CancellationToken {
        self.cx.shutdown_token()
    }

    /// Runs blocking work on Tokio's blocking pool.
    ///
    /// See [`RawContext::run_blocking`].
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

    /// Returns this actor's enclosing scope.
    ///
    /// Cooperative removal detaches this child only after this hook returns,
    /// so awaiting anything that depends on that detach — scope termination,
    /// this child's completion, its own removal — resolves only once the
    /// shutdown grace period expires. Request fire-and-forget control here and
    /// observe the outcome from outside the scope.
    pub fn scope(&self) -> ScopeRef {
        self.cx.scope()
    }
}

fn send_error_rejection(kind: SendErrorKind) -> MessageRejection {
    match kind {
        SendErrorKind::NotRunning => MessageRejection::NotRunning,
        SendErrorKind::Terminated => MessageRejection::ActorTerminated,
        SendErrorKind::Full => MessageRejection::MailboxFull,
        SendErrorKind::TimedOut => MessageRejection::Timeout,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex, PoisonError},
    };

    use tracing::Level;
    use tracing_subscriber::{
        fmt::{self, MakeWriter},
        prelude::*,
    };

    use super::*;
    use crate::{
        MailboxMode, Restart,
        actor::binding::{BindingGuard, mailbox},
    };

    #[test]
    fn actor_ref_try_send_traces_closed_mailbox_reason() {
        let actor_id: Arc<str> = Arc::from("worker");
        let core = Arc::new(BindingCore::new(Arc::clone(&actor_id)));
        let actor = ActorRef::from_core(&core, None);
        let (sender, mut receiver) = mailbox(&MailboxMode::conflate(), 1);
        let _binding = BindingGuard::bind(
            Arc::clone(&core),
            MailboxRef::new(actor_id, sender),
            ScopeObservability::new(),
            Restart::never(),
        );
        receiver.close_external();

        let output = capture_tracing_output(|| {
            assert!(matches!(
                actor.try_send(()),
                Err(SendError {
                    actor_id,
                    kind: SendErrorKind::NotRunning,
                    ..
                }) if actor_id == "worker"
            ));
        });
        for expected in [
            r#""actor_id":"worker""#,
            r#""operation":"try_send""#,
            r#""reason":"mailbox_closed""#,
        ] {
            assert!(
                output.contains(expected),
                "expected tracing output to contain `{expected}`, got: {output}"
            );
        }
    }

    fn capture_tracing_output(f: impl FnOnce()) -> String {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .json()
                .with_writer(buffer.clone())
                .with_current_span(false)
                .with_span_list(false)
                .without_time()
                .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
                    Level::TRACE,
                )),
        );

        tracing::subscriber::with_default(subscriber, f);
        buffer.to_string_output()
    }

    #[derive(Clone, Default)]
    struct SharedBuffer {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedBuffer {
        fn to_string_output(&self) -> String {
            String::from_utf8(
                self.inner
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone(),
            )
            .expect("tracing output should be utf-8")
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedWriter {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    struct SharedWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
