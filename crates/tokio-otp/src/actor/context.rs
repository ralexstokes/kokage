use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, oneshot, watch},
    time::{Instant, MissedTickBehavior, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::RuntimeHandle;

use crate::actor::{
    binding::{
        ActorStats, ActorStatsCounters, BindingCore, BindingState, MailboxReceiver, MailboxRef,
        MessageSizeObserver, SendOutcome,
    },
    cancellation::CancellationHandle,
    error::{BlockingCancelled, CallError, OffloadDeadline, SendError, TryRecvError},
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
        let (outstanding_offloads, depth, capacity) = match &*self.binding.borrow() {
            BindingState::Bound(mailbox) => {
                let (depth, capacity) = mailbox.usage();
                (mailbox.outstanding_offloads(), depth, capacity)
            }
            BindingState::Unbound | BindingState::Terminated => (0, 0, 0),
        };
        self.stats
            .snapshot(&self.actor_id, outstanding_offloads, depth, capacity)
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

    async fn post_to_incarnation(&self, mailbox: MailboxRef<M>, message: M) {
        let message_size = self
            .message_size
            .as_ref()
            .map(|observer| observer.size_hint(&message));
        if let SendOutcome::Accepted { conflated } = mailbox.send_internal_retaining(message).await
        {
            self.observe_send(MessageOperation::Send, None);
            self.stats.record_send(true);
            self.stats.record_conflated(conflated);
            self.record_message_size(message_size);
        }
    }

    async fn post_state_timeout_to_incarnation(
        &self,
        mailbox: MailboxRef<M>,
        message: M,
        cancellation: CancellationToken,
    ) {
        let message_size = self
            .message_size
            .as_ref()
            .map(|observer| observer.size_hint(&message));
        if let SendOutcome::Accepted { conflated } = mailbox
            .send_state_timeout_retaining(message, cancellation)
            .await
        {
            self.observe_send(MessageOperation::Send, None);
            self.stats.record_send(true);
            self.stats.record_conflated(conflated);
            self.record_message_size(message_size);
        }
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
/// posted. Aborting a request only abandons the local wait: it cannot retract
/// work that another actor or external service already accepted.
#[derive(Clone, Debug)]
pub struct OffloadHandle {
    cancellation: CancellationToken,
    finished: Arc<AtomicBool>,
}

impl OffloadHandle {
    /// Aborts the offload and suppresses its continuation message.
    pub fn abort(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether the offload has finished or its abort has been observed.
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
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

/// Runtime context passed to a [`RawActor`](crate::RawActor) each time the
/// graph is run.
///
/// This is the widest context: a `RawActor` owns its receive loop, so it gets
/// the incoming [`mailbox`](Self::recv) and explicit
/// [`mark_ready`](Self::mark_ready) alongside the ambient capabilities —
/// message [`timers`](Self::send_after), a bounded
/// [`offload`](Self::offload) primitive for asynchronous postbacks, a
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
    pub(crate) incarnation: MailboxRef<M>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) observability: GraphObservability,
    pub(crate) timers: ActorTimers,
    pub(crate) monitors: Arc<ActorMonitors>,
    pub(crate) ready: Option<oneshot::Sender<()>>,
    pub(crate) continuations: VecDeque<M>,
    pub(crate) offloads: ActorOffloads,
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

    /// Runs a bounded future and substitutes `fallback` when its deadline
    /// expires, then posts the resulting value back as an ordinary message.
    ///
    /// This is the usual way to pipeline bounded work from an actor. A timed
    /// out offload may already have initiated an external effect, so `fallback`
    /// should represent an unknown outcome that the actor can reconcile.
    /// Use [`Self::offload`] when the continuation needs to distinguish a
    /// deadline from a value returned by the future.
    pub fn offload_or<F, T, C>(
        &self,
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
    /// posts its total outcome back as an ordinary message.
    ///
    /// This is the lower-level form of [`Self::offload_or`]. The continuation is
    /// total: it receives either the future's value or [`OffloadDeadline`] and
    /// must produce a message in both cases. Delivery uses this actor's normal
    /// mailbox policy and FIFO backpressure. It is stamped to this exact
    /// incarnation, so a completion racing a restart is silently dropped
    /// rather than delivered to fresh state.
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
    /// `offload` lives on the shared context type, but its drain integration is
    /// provided by the framework-owned [`Actor`](crate::Actor) loop. A custom
    /// [`RawActor`](crate::RawActor) remains responsible for its own shutdown
    /// and draining protocol.
    pub fn offload<F, T, C>(&self, deadline: Duration, future: F, continuation: C) -> OffloadHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        let (cancellation, finished) = self.offloads.start();
        let handle = OffloadHandle {
            cancellation: cancellation.clone(),
            finished: Arc::clone(&finished),
        };
        let offloads = self.offloads.inner();
        let myself = self.myself.clone();
        let incarnation = self.incarnation.clone();
        let guard = OffloadGuard { offloads, finished };
        tokio::spawn(async move {
            // Constructed before spawning so dropping an unpolled task still
            // decrements the outstanding count and marks its handle finished.
            let _guard = guard;
            let outcome = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                outcome = timeout(deadline, future) => outcome.map_err(|_| OffloadDeadline),
            };
            let message = continuation(outcome);
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {}
                () = myself.post_to_incarnation(incarnation, message) => {}
            }
        });
        handle
    }

    pub(crate) fn close_external_intake(&mut self) {
        self.mailbox.close_external();
    }

    pub(crate) fn abort_offloads(&self) {
        self.offloads.abort_all();
    }

    pub(crate) fn outstanding_offloads(&self) -> usize {
        self.offloads.outstanding()
    }

    pub(crate) fn offload_change_notify(&self) -> Arc<Notify> {
        self.offloads.change_notify()
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
        let monitor = CancellationHandle::new(cancellation.clone());
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

    /// Sends `message` to this actor after `delay` has elapsed.
    ///
    /// Delivery uses the actor's ordinary mailbox policy: FIFO queues wait for
    /// capacity, while conflating mailboxes replace stale unread state. The
    /// timer is cancelled automatically if this actor incarnation stops or
    /// restarts. To schedule delayed delivery to another actor, use
    /// [`send_after_to`](Self::send_after_to).
    pub fn send_after(&self, message: M, delay: Duration) -> CancellationHandle {
        self.send_after_to(&self.myself, message, delay)
    }

    /// Sends `message` to `target` after `delay` has elapsed.
    ///
    /// The timer is bound to the lifecycle of the *scheduling* actor, exactly
    /// like [`send_after`](Self::send_after): it is cancelled automatically
    /// when this actor incarnation stops or restarts. It is not bound to the
    /// target's lifecycle — if the target restarts before the timer fires,
    /// the message is delivered to whichever target incarnation is running at
    /// fire time, so messages should carry enough context (a key or
    /// generation) for the handler to reject ones it no longer expects.
    /// Delivery uses the target's ordinary mailbox policy: FIFO queues wait
    /// for capacity, while conflating mailboxes replace stale unread state.
    pub fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> CancellationHandle {
        let cancellation = self.timers.child_token();
        let timer = CancellationHandle::new(cancellation.clone());
        spawn_delayed_send(target.clone(), message, delay, cancellation);

        timer
    }

    /// Sends `message` to this actor after `delay`, retractably.
    ///
    /// This differs from [`send_after`](Self::send_after) only in when
    /// cancellation stops working. An ordinary delayed send can be cancelled
    /// until it reaches the mailbox; cancelling the handle returned here also
    /// discards the message *after* the mailbox accepted it, as long as the
    /// actor has not yet received it. The suppression is a mailbox-level
    /// filter, so it works for both [`Actor`](crate::Actor) and
    /// [`RawActor`](crate::RawActor) receive loops.
    ///
    /// Like other timers, the send is cancelled automatically if this actor
    /// incarnation stops or restarts.
    ///
    /// This is the primitive under [`StateTimeoutSlot`], which adds the
    /// one-at-a-time replace/clear bookkeeping of a `gen_statem`-style state
    /// timeout. Reach for the slot unless you are building different
    /// bookkeeping on top.
    pub fn send_after_retractable(&self, message: M, delay: Duration) -> CancellationHandle {
        let cancellation = self.timers.child_token();
        let timer = CancellationHandle::new(cancellation.clone());
        spawn_state_timeout_send(
            self.myself(),
            self.incarnation.clone(),
            message,
            delay,
            cancellation,
        );

        timer
    }

    /// Sends a clone of `message` to this actor after every `period`.
    ///
    /// The first message is sent after one full period. FIFO delivery waits
    /// for mailbox capacity; conflating delivery replaces stale unread state.
    /// Missed ticks are skipped rather than accumulated. The timer stops on
    /// cancellation, delivery failure, or when this actor incarnation stops or
    /// restarts. To schedule periodic delivery to another actor, use
    /// [`interval_to`](Self::interval_to).
    ///
    /// A zero period creates an already-cancelled timer and sends no messages.
    pub fn interval(&self, message: M, period: Duration) -> CancellationHandle
    where
        M: Clone,
    {
        self.interval_to(&self.myself, message, period)
    }

    /// Sends a clone of `message` to `target` after every `period`.
    ///
    /// The first message is sent after one full period. Delivery uses the
    /// target's ordinary mailbox policy — FIFO queues wait for capacity,
    /// conflating mailboxes replace stale unread state — and missed ticks are
    /// skipped rather than accumulated. Like
    /// [`send_after_to`](Self::send_after_to), the timer is bound to the
    /// lifecycle of the *scheduling* actor, not the target's: it stops on
    /// cancellation, when this actor incarnation stops or restarts, or on
    /// delivery failure (the target has permanently terminated). A target
    /// that merely restarts does not stop the timer; later ticks are
    /// delivered to its next incarnation.
    ///
    /// A zero period creates an already-cancelled timer and sends no messages.
    pub fn interval_to<T>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> CancellationHandle
    where
        T: Clone + Send + 'static,
    {
        let cancellation = self.timers.child_token();
        let timer = CancellationHandle::new(cancellation.clone());
        if period.is_zero() {
            timer.cancel();
            return timer;
        }
        let target = target.clone();

        tokio::spawn(async move {
            let start = Instant::now() + period;
            let mut interval = tokio::time::interval_at(start, period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let sent = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => break,
                            sent = target.send(message.clone()) => sent,
                        };
                        if sent.is_err() {
                            cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        });

        timer
    }

    /// Waits for the next mailbox message, or `None` once shutdown has been
    /// requested or the mailbox has been closed.
    ///
    /// Shutdown is checked first: as soon as shutdown is requested this
    /// returns `None`, even when messages are still queued. Queued messages
    /// are dropped when the actor exits unless the actor drains them with
    /// [`try_recv`](Self::try_recv), or uses [`Actor`](crate::Actor)
    /// with [`DrainPolicy::Drain`](crate::DrainPolicy). Queued
    /// [`call`](ActorRef::call)s whose reply messages are dropped observe
    /// [`CallError::ReplyDropped`](crate::CallError::ReplyDropped).
    pub async fn recv(&mut self) -> Option<M> {
        let message = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => None,
            message = self.mailbox.recv() => message,
        };

        if message.is_some() {
            self.myself.record_received();
            self.observability.emit_message_received(&self.id);
        }

        message
    }

    /// Attempts to receive a queued message without waiting and without
    /// consulting the shutdown token.
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
    pub fn try_recv(&mut self) -> Result<M, TryRecvError> {
        let message = self.mailbox.try_recv().map_err(|error| match error {
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
/// use tokio_otp::{LiveContext, StateTimeoutSlot};
/// use std::time::Duration;
///
/// # enum Msg { Tick }
/// fn arm(ctx: &impl LiveContext<Msg>, idle: &mut StateTimeoutSlot) {
///     idle.set(ctx.send_after_retractable(Msg::Tick, Duration::from_secs(5)));
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

    /// Sends `message` to this actor after `delay` has elapsed.
    ///
    /// See [`ActorContext::send_after`].
    fn send_after(&self, message: M, delay: Duration) -> CancellationHandle {
        self.cx().send_after(message, delay)
    }

    /// Sends `message` to `target` after `delay` has elapsed.
    ///
    /// See [`ActorContext::send_after_to`].
    fn send_after_to<T: Send + 'static>(
        &self,
        target: &ActorRef<T>,
        message: T,
        delay: Duration,
    ) -> CancellationHandle {
        self.cx().send_after_to(target, message, delay)
    }

    /// Sends `message` to this actor after `delay`, retractably.
    ///
    /// See [`ActorContext::send_after_retractable`] and [`StateTimeoutSlot`].
    fn send_after_retractable(&self, message: M, delay: Duration) -> CancellationHandle {
        self.cx().send_after_retractable(message, delay)
    }

    /// Sends a clone of `message` to this actor after every `period`.
    ///
    /// See [`ActorContext::interval`].
    fn interval(&self, message: M, period: Duration) -> CancellationHandle
    where
        M: Clone,
    {
        self.cx().interval(message, period)
    }

    /// Sends a clone of `message` to `target` after every `period`.
    ///
    /// See [`ActorContext::interval_to`].
    fn interval_to<T>(
        &self,
        target: &ActorRef<T>,
        message: T,
        period: Duration,
    ) -> CancellationHandle
    where
        T: Clone + Send + 'static,
    {
        self.cx().interval_to(target, message, period)
    }

    /// Runs a bounded future and substitutes `fallback` when its deadline
    /// expires.
    ///
    /// See [`ActorContext::offload_or`].
    fn offload_or<F, T, C>(
        &self,
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
        self.cx()
            .offload_or(deadline, future, fallback, continuation)
    }

    /// Runs a bounded future without blocking this actor's receive loop.
    ///
    /// See [`ActorContext::offload`].
    fn offload<F, T, C>(&self, deadline: Duration, future: F, continuation: C) -> OffloadHandle
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, OffloadDeadline>) -> M + Send + 'static,
    {
        self.cx().offload(deadline, future, continuation)
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
        impl<M> sealed::Sealed<M> for $view<'_, M> {
            fn cx(&self) -> &ActorContext<M> {
                self.cx
            }

            fn cx_mut(&mut self) -> &mut ActorContext<M> {
                self.cx
            }
        }

        impl<M: Send + 'static> LiveContext<M> for $view<'_, M> {}
    };
}

/// The enclosing scope handle as seen from
/// [`Actor::on_start`](crate::Actor::on_start).
///
/// This is a [`RuntimeHandle`] with the lifecycle-awaiting operations withheld.
/// An actor cannot report ready until its `on_start` returns, so awaiting any
/// operation that blocks on another child's lifecycle — the scope starting, a
/// child completing, the scope shutting down — deadlocks the actor against
/// itself. Those methods are absent here rather than documented as forbidden.
///
/// The restriction is closed under navigation: [`subtree`](Self::subtree)
/// hands back another `StartingScope`, because a sibling scope declared after
/// this actor starts after it reports ready, and the raw
/// `SupervisorHandle` — one method call away from the same waits — is not
/// reachable from here at all.
///
/// The pattern for a wait that must happen is to launch it as pipelined work
/// and consume the result after startup. Take the full handle with
/// [`after_start`](Self::after_start) and move it into the spawned future;
/// that is also the way to reach
/// [`RuntimeHandle::supervisor_handle`] and the rest of the full surface.
#[derive(Clone, Debug)]
pub struct StartingScope {
    handle: RuntimeHandle,
}

/// The enclosing scope handle as seen from
/// [`Actor::on_stop`](crate::Actor::on_stop).
///
/// The same [`RuntimeHandle`] narrowing as [`StartingScope`], for the opposite
/// end of the lifecycle and a different reason. A stopping child is still
/// attached to its supervisor: cooperative removal waits for `on_stop` to
/// return before the child is detached and its exit recorded. Anything awaited
/// here that blocks on the scope's membership settling — the scope finishing
/// its shutdown, a child completing, this actor's own removal — therefore
/// waits on a detach that waits on this hook. The cycle resolves only when the
/// shutdown grace period runs out and aborts the actor, turning a clean stop
/// into a timed-out one.
///
/// Fire-and-forget control is kept: [`shutdown`](Self::shutdown) requests and
/// returns, and insertion schedules rather than waits.
/// [`subtree`](Self::subtree) hands back another `StoppingScope`, since a
/// nested scope's shutdown is sequenced with this one's.
///
/// Teardown that genuinely has to observe another child belongs in work that
/// outlives this incarnation: take the full handle with
/// [`after_stop`](Self::after_stop) and move it into a
/// [`tokio::spawn`]ed future rather than awaiting it inline.
#[derive(Clone, Debug)]
pub struct StoppingScope {
    handle: RuntimeHandle,
}

/// Generates the delegation shared by the stage-restricted scope handles.
///
/// Both types are the same restriction of [`RuntimeHandle`] — everything that
/// cannot block on another child's lifecycle — differing only in the stage
/// they belong to and in the name of their escape hatch. The rationale for
/// each restriction lives on the type, not here.
macro_rules! restricted_scope {
    ($scope:ident) => {
        impl $scope {
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
            pub fn subscribe_snapshots(
                &self,
            ) -> watch::Receiver<tokio_supervisor::SupervisorSnapshot> {
                self.handle.subscribe_snapshots()
            }

            /// Returns a handle to a nested subtree by id, restricted the same
            /// way as this one.
            pub fn subtree(&self, id: &str) -> Option<Self> {
                self.handle.subtree(id).map(Self::new)
            }

            /// Inserts an actor into this scope.
            ///
            /// Safe to await here: insertion schedules startup rather than
            /// waiting for it, so it does not block on another child's
            /// lifecycle. See [`RuntimeHandle::add_actor`].
            pub async fn add_actor<F>(
                &self,
                label: impl Into<String>,
                factory: F,
                options: crate::DynamicActorOptions<<F::Actor as crate::RawActor>::Msg>,
            ) -> Result<
                ActorRef<<F::Actor as crate::RawActor>::Msg>,
                tokio_supervisor::ControlError,
            >
            where
                F: crate::ActorFactory,
            {
                self.handle.add_actor(label, factory, options).await
            }

            /// Inserts a subtree into this scope.
            ///
            /// Safe to await here for the same reason as
            /// [`add_actor`](Self::add_actor). See
            /// [`RuntimeHandle::add_subtree`].
            pub async fn add_subtree(
                &self,
                id: impl Into<String>,
                builder: impl Into<crate::SupervisionTree>,
            ) -> Result<RuntimeHandle, crate::AddSubtreeError> {
                self.handle.add_subtree(id, builder).await
            }

            /// Observes lifecycle transitions of this scope's direct children.
            pub fn watch_lifecycle(&self) -> tokio_supervisor::LifecycleWatch {
                self.handle.watch_lifecycle()
            }

            /// Observes lifecycle transitions of this scope and everything
            /// beneath it.
            pub fn watch_lifecycle_recursive(&self) -> tokio_supervisor::RecursiveLifecycleWatch {
                self.handle.watch_lifecycle_recursive()
            }

            /// Requests shutdown of this scope without waiting for it.
            pub fn shutdown(&self) {
                self.handle.shutdown()
            }
        }
    };
}

restricted_scope!(StartingScope);
restricted_scope!(StoppingScope);

impl StartingScope {
    /// Releases the full [`RuntimeHandle`] for use after `on_start` returns.
    ///
    /// Move the returned handle into a [`tokio::spawn`] or an
    /// [`offload`](LiveContext::offload) continuation. Awaiting its lifecycle
    /// operations inline, before `on_start` returns, is the deadlock this type
    /// exists to make explicit.
    pub fn after_start(self) -> RuntimeHandle {
        self.handle
    }
}

impl StoppingScope {
    /// Releases the full [`RuntimeHandle`] for teardown that outlives this
    /// incarnation.
    ///
    /// Move the returned handle into a [`tokio::spawn`]ed future — something
    /// the supervisor is not waiting on — and let it observe the scope after
    /// this child has detached. Awaiting its lifecycle operations inline, from
    /// `on_stop`, waits on a detach that is waiting on `on_stop`.
    pub fn after_stop(self) -> RuntimeHandle {
        self.handle
    }
}

/// Context handed to [`Actor::on_start`](crate::Actor::on_start).
///
/// Adds [`continue_with`](Self::continue_with) to the ambient capabilities and
/// narrows the scope handles to [`StartingScope`], which withholds the
/// lifecycle waits that would deadlock an actor that has not reported ready.
///
/// The mailbox is deliberately absent: the provided receive loop owns it, and
/// readiness is reported by the framework once this hook returns.
pub struct StartContext<'a, M> {
    cx: &'a mut ActorContext<M>,
}

/// Context handed to [`Actor::handle`](crate::Actor::handle) — the context in
/// which one message is handled.
///
/// The ambient capabilities plus [`continue_with`](Self::continue_with) and
/// full scope handles. The mailbox is absent because the provided receive loop
/// owns it; a handler that reads it directly would bypass drain accounting and
/// the continuation queue.
///
/// This is the only hook the provided loop calls from two different phases, so
/// it is also the only one that has to say which: see
/// [`is_draining`](Self::is_draining).
pub struct MessageContext<'a, M> {
    cx: &'a mut ActorContext<M>,
    draining: bool,
}

/// Context handed to [`Actor::on_stop`](crate::Actor::on_stop).
///
/// A deliberately narrow surface. The hook runs after the receive loop has
/// exited and the mailbox has been drained or discarded, so anything that
/// queues future work for this incarnation — timers, intervals, watches,
/// offloads, state timeouts, continuations — has no one left to deliver to and
/// is withheld. What remains is identity, the shutdown token, the scope
/// handles, and [`run_blocking`](Self::run_blocking) for synchronous teardown.
///
/// The scope handles are narrowed to [`StoppingScope`], which withholds the
/// lifecycle waits that would block on a detach this hook is itself holding up.
pub struct StopContext<'a, M> {
    cx: &'a mut ActorContext<M>,
}

live_context!(StartContext);
live_context!(MessageContext);

impl<'a, M: Send + 'static> StartContext<'a, M> {
    pub(crate) fn new(cx: &'a mut ActorContext<M>) -> Self {
        Self { cx }
    }

    /// Returns this actor's enclosing scope, restricted for the startup stage.
    ///
    /// See [`StartingScope`] for why the lifecycle waits are withheld here and
    /// how to pipeline one that must happen.
    pub fn supervisor(&self) -> StartingScope {
        StartingScope::new(self.cx.supervisor())
    }

    /// Returns this leader's declared child scope, restricted for the startup
    /// stage.
    ///
    /// The child scope starts only after this hook returns, so awaiting its
    /// readiness inline can never succeed. See [`StartingScope`].
    pub fn children(&self) -> Option<StartingScope> {
        self.cx.children().map(StartingScope::new)
    }
}

impl<'a, M: Send + 'static> MessageContext<'a, M> {
    pub(crate) fn new(cx: &'a mut ActorContext<M>) -> Self {
        Self {
            cx,
            draining: false,
        }
    }

    pub(crate) fn draining(cx: &'a mut ActorContext<M>) -> Self {
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

impl<'a, M: Send + 'static> StopContext<'a, M> {
    pub(crate) fn new(cx: &'a mut ActorContext<M>) -> Self {
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
    pub fn myself(&self) -> ActorRef<M> {
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
    /// See [`StoppingScope`] for why the lifecycle waits are withheld here and
    /// where teardown that needs one belongs instead.
    pub fn supervisor(&self) -> StoppingScope {
        StoppingScope::new(self.cx.supervisor())
    }

    /// Returns this leader's declared child scope, restricted for the shutdown
    /// stage.
    ///
    /// The child scope is torn down around this hook, so awaiting its
    /// completion inline deadlocks the same way. See [`StoppingScope`].
    pub fn children(&self) -> Option<StoppingScope> {
        self.cx.children().map(StoppingScope::new)
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

/// One-at-a-time state timeout, held in actor state.
///
/// This is the `gen_statem` state-timeout pattern: entering a timed state arms
/// a timeout, entering a different state replaces or clears it, and a timeout
/// belonging to a state the actor has already left must not be acted on.
///
/// The slot is bookkeeping over
/// [`send_after_retractable`](LiveContext::send_after_retractable), which is
/// what makes the last part work — [`set`](Self::set) and [`clear`](Self::clear)
/// suppress a stale timeout that already reached the mailbox but has not been
/// received yet. Without that primitive this pattern cannot be built outside
/// the framework: recognizing a stale timeout in user code would require
/// tagging it with a generation, and the actor's message type belongs to its
/// senders, not to the wrapper.
///
/// Nothing here is per-actor state the runtime must carry, so an actor that
/// does not model states pays nothing for it.
///
/// ```no_run
/// use std::time::Duration;
/// use tokio_otp::{LiveContext, StateTimeoutSlot};
///
/// # enum Msg { Idle, Work }
/// const IDLE: Duration = Duration::from_secs(30);
///
/// struct Session {
///     idle: StateTimeoutSlot,
/// }
///
/// impl Session {
///     fn go_idle(&mut self, ctx: &impl LiveContext<Msg>) {
///         self.idle.set(ctx.send_after_retractable(Msg::Idle, IDLE));
///     }
///
///     fn go_busy(&mut self) {
///         self.idle.clear();
///     }
/// }
/// ```
#[derive(Debug, Default)]
pub struct StateTimeoutSlot {
    armed: Option<CancellationHandle>,
}

impl StateTimeoutSlot {
    /// Returns an empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms `timer`, cancelling and discarding whatever the slot held.
    ///
    /// Pass the handle from
    /// [`send_after_retractable`](LiveContext::send_after_retractable). The
    /// previous timeout is cancelled even if it has already been accepted by
    /// the mailbox, so a stale timeout is never received. The returned handle
    /// is an alias of the newly armed one, for callers that want to cancel it
    /// independently.
    pub fn set(&mut self, timer: CancellationHandle) -> CancellationHandle {
        if let Some(previous) = self.armed.replace(timer.clone()) {
            previous.cancel();
        }
        timer
    }

    /// Cancels and clears the armed timeout, if any.
    ///
    /// Call this when entering a state that has no timeout. A timeout already
    /// accepted by the mailbox is discarded if it has not yet been received.
    pub fn clear(&mut self) {
        if let Some(armed) = self.armed.take() {
            armed.cancel();
        }
    }

    /// Returns whether a timeout is currently armed and uncancelled.
    pub fn is_armed(&self) -> bool {
        self.armed
            .as_ref()
            .is_some_and(|armed| !armed.is_cancelled())
    }
}

pub(crate) struct ActorOffloads {
    inner: Arc<ActorOffloadsInner>,
    cancellation: CancellationToken,
}

struct ActorOffloadsInner {
    changed: Arc<Notify>,
    outstanding: Arc<AtomicU64>,
}

impl ActorOffloads {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ActorOffloadsInner {
                changed: Arc::new(Notify::new()),
                outstanding: Arc::new(AtomicU64::new(0)),
            }),
            // Unlike timers, this token is independent from graph shutdown:
            // Drain actors keep their offloads until their own deadlines.
            cancellation: CancellationToken::new(),
        }
    }

    fn start(&self) -> (CancellationToken, Arc<AtomicBool>) {
        let cancellation = self.cancellation.child_token();
        self.inner.outstanding.fetch_add(1, Ordering::Relaxed);
        (cancellation, Arc::new(AtomicBool::new(false)))
    }

    fn inner(&self) -> Arc<ActorOffloadsInner> {
        Arc::clone(&self.inner)
    }

    pub(crate) fn gauge(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.inner.outstanding)
    }

    fn abort_all(&self) {
        self.cancellation.cancel();
    }

    fn outstanding(&self) -> usize {
        self.inner.outstanding.load(Ordering::Acquire) as usize
    }

    fn change_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.changed)
    }
}

impl Drop for ActorOffloads {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct OffloadGuard {
    offloads: Arc<ActorOffloadsInner>,
    finished: Arc<AtomicBool>,
}

impl Drop for OffloadGuard {
    fn drop(&mut self) {
        self.offloads.outstanding.fetch_sub(1, Ordering::Release);
        self.finished.store(true, Ordering::Release);
        self.offloads.changed.notify_one();
    }
}

fn spawn_delayed_send<T: Send + 'static>(
    target: ActorRef<T>,
    message: T,
    delay: Duration,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {}
            () = tokio::time::sleep(delay) => {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {}
                    _ = target.send(message) => {}
                }
            }
        }
    });
}

fn spawn_state_timeout_send<T: Send + 'static>(
    target: ActorRef<T>,
    incarnation: MailboxRef<T>,
    message: T,
    delay: Duration,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {}
            () = tokio::time::sleep(delay) => {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {}
                    _ = target.post_state_timeout_to_incarnation(
                        incarnation,
                        message,
                        cancellation.clone(),
                    ) => {}
                }
            }
        }
    });
}

pub(crate) struct ActorTimers(CancellationToken);

impl ActorTimers {
    pub(crate) fn new(shutdown: &CancellationToken) -> Self {
        Self(shutdown.child_token())
    }

    fn child_token(&self) -> CancellationToken {
        self.0.child_token()
    }
}

impl Drop for ActorTimers {
    fn drop(&mut self) {
        self.0.cancel();
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
