use std::future::Future;

use crate::actor::{
    context::{ActorContext, MessageContext, StartContext, StopContext, TimerWake},
    raw::{ActorResult, BoxError, RawActor},
};

enum LoopEvent<M> {
    Message(Option<M>),
    Timer(TimerWake),
}

/// How the provided [`Actor`] receive loop treats messages still
/// queued when shutdown is requested.
///
/// # Choosing a policy
///
/// The default is [`Drain`](Self::Drain): an actor that says nothing finishes
/// the work it already accepted. This is the safer default for the common case
/// where a queued message represents work no one else will redo, and losing it
/// silently is worse than taking longer to stop.
///
/// Keep [`Drain`](Self::Drain) when a dropped message would lose work that no
/// one else will redo: writes not yet flushed, positions not yet reported,
/// [`call`](crate::ActorRef::call)s whose caller is still waiting for a reply.
/// Drain is not free — see the shutdown budget it spends, below — and it
/// requires a handler that stays correct while its peers are stopping. An
/// actor whose `handle` sends to a sibling on the drain path must tolerate
/// that sibling already being gone, per the ordering rules below.
///
/// Choose [`Discard`](Self::Discard) when queued work is already replaceable:
/// messages that a later run will recompute, a conflating mailbox whose value
/// is a snapshot, ticks and polls, or anything the sender retries. Discard also
/// keeps shutdown bounded by the handler currently in flight rather than by the
/// depth of the queue behind it, and it aborts outstanding
/// [offloads](crate::ActorContext::offload) instead of waiting for them — so it
/// is the right answer for an actor holding long-running work that shutdown
/// should cut short rather than see through.
/// Actor-owned [scope waits](crate::LiveContext::spawn_scope_wait) are always
/// aborted before either policy is applied; lifecycle waits are not accepted
/// actor work for `Drain` to finish.
///
/// Neither policy is a delivery guarantee. A message dropped by `Discard` and a
/// message handled by `Drain` are equally invisible to the sender, which
/// observes only [`SendError`](crate::SendError) or
/// [`ReplyDropped`](crate::CallError::ReplyDropped). End-to-end delivery
/// ownership belongs in an application acknowledgement and replay protocol.
///
/// # Startup and shutdown ordering
///
/// Actors in an ordered runtime initialize in declaration order: each actor's
/// [`on_start`](Actor::on_start) must finish before the next actor is spawned.
/// Ordered siblings stop in reverse declaration order, one complete child at
/// a time. A draining actor can therefore
/// observe a [`SendError`](crate::SendError) from a later sibling that has already
/// stopped; drain handlers must tolerate that (skip or log the failed send)
/// rather than propagate it, or the error fails the draining actor itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DrainPolicy {
    /// Stop immediately.
    ///
    /// Queued messages are dropped and queued [`call`](crate::ActorRef::call)s
    /// observe [`ReplyDropped`](crate::CallError::ReplyDropped). Outstanding
    /// [offloads](crate::ActorContext::offload) are aborted rather than awaited.
    /// Actor-owned [scope waits](crate::LiveContext::spawn_scope_wait) are
    /// aborted under both policies. This matches the behavior of a hand-written
    /// `while let Some(message) = ctx.recv().await` loop.
    Discard,
    /// Close the mailbox to new sends, handle every message already queued,
    /// then stop.
    ///
    /// A send racing with a shutdown request can be accepted before the actor
    /// observes cancellation and closes intake; that message is part of the
    /// queued prefix drained here. Once intake is closed, `try_send` reports a
    /// closed mailbox and an awaited `send` waits for the binding's final
    /// lifecycle state. There is no separate sender-visible `Draining` state.
    ///
    /// The handler itself can see the phase:
    /// [`MessageContext::is_draining`](crate::MessageContext::is_draining) is
    /// `true` for exactly the calls made here. Use it to skip work whose only
    /// effect would be to queue something the drain will drop.
    ///
    /// # Shutdown budget
    ///
    /// The drain has no clock of its own. It spends the surrounding host's
    /// shutdown budget, which is set in a different place from this policy:
    /// the explicit bound passed to
    /// [`RunnableActor::run_until`](crate::RunnableActor::run_until) for a
    /// standalone actor, or the child [`ShutdownPolicy`](crate::ShutdownPolicy)
    /// under a runtime. The two are not checked against each other. Setting
    /// `Drain` does not extend the budget, and nothing warns when the budget is
    /// too small for the queue.
    ///
    /// When the budget runs out mid-drain, the drain is cut where it stands:
    /// remaining queued messages are dropped, [`on_stop`](Actor::on_stop) can be
    /// skipped, and the actor is aborted. There is no per-message signal for the
    /// messages that were lost — what surfaces is a timed-out exit
    /// ([`ActorRunError::ShutdownTimedOut`](crate::ActorRunError::ShutdownTimedOut)
    /// standalone, [`ExitStatusView::Aborted { after_grace: true }`](crate::ExitStatusView::Aborted)
    /// under supervision). A `Drain` actor under a too-short grace period
    /// therefore behaves like a slower `Discard`, which is the failure mode to
    /// watch for. The enclosing shutdown also reports the timeout. In particular,
    /// [`ShutdownPolicy::abort`](crate::ShutdownPolicy::abort) has a zero grace
    /// period and leaves effectively no drain window at all.
    ///
    /// Size the budget for the whole queued prefix, not one message: roughly
    /// mailbox depth times worst-case handler latency, plus room for
    /// [`on_stop`](Actor::on_stop). Ordered siblings stop one at a time, so
    /// sibling drains add up rather than overlap. Where the drain must finish
    /// for correctness, handle the timeout reported by cooperative shutdown.
    #[default]
    Drain,
}

/// Handler-style actor interface with a framework-owned receive loop.
///
/// Implement this trait when an actor only needs one method per message. The
/// blanket [`RawActor`] implementation receives messages in mailbox order, runs
/// lifecycle hooks, and applies [`DrainPolicy`] at shutdown.
///
/// Hand-writing [`RawActor::run`] remains the escape hatch for actors that need
/// custom loop control.
///
/// # Capability contract
///
/// Handler actors do not receive the mailbox-owning [`ActorContext`]. The
/// framework owns `recv`, `try_recv`, and readiness reporting, and hands each
/// hook a stage-specific view instead. In return, the live startup and message
/// stages implement [`LiveContext`](crate::LiveContext), which provides
/// loop-owned timers, continuations, and actor-owned scope waits that a custom
/// raw loop must express directly. Watches and offloads remain available on
/// [`ActorContext`].
///
/// A [`LiveContext::stop`](crate::LiveContext::stop) exit is normal for
/// monitoring and supervision. An
/// [`Always`](tokio_supervisor::RestartPolicy::Always) child restarts after it;
/// [`OnFailure`](tokio_supervisor::RestartPolicy::OnFailure) and
/// [`Never`](tokio_supervisor::RestartPolicy::Never) children do not.
///
/// # Incarnation construction
///
/// Registration takes a reusable [`ActorFactory`](crate::ActorFactory), whose
/// output is fresh incarnation-local state and need not implement [`Clone`].
/// An incarnation is moved into one task and need not implement [`Sync`]; the
/// reusable factory remains `Send + Sync` because supervision may share it
/// across restarts.
/// Acquire fallible or asynchronous per-incarnation resources (connections,
/// files, sessions) in [`on_start`](Self::on_start).
pub trait Actor: Send + 'static {
    /// The message type this handler receives.
    type Msg: Send + 'static;

    /// Handles one received message.
    ///
    /// Returning `Ok(())` receives the next message unless
    /// [`ctx.stop()`](crate::LiveContext::stop) was called. A stop request is
    /// clean: the actor's [`DrainPolicy`] is applied to the queued mailbox
    /// before [`on_stop`](Self::on_stop) runs. Returning `Err` fails the actor
    /// exactly like [`RawActor::run`] returning `Err`.
    fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> impl Future<Output = ActorResult> + Send;

    /// Runs once before the first message of each actor run.
    ///
    /// This is the place to acquire per-incarnation resources. Calling
    /// [`ctx.stop()`](crate::LiveContext::stop) requests a clean stop before
    /// the ordinary receive loop.
    /// [`DrainPolicy::Discard`] drops messages queued during startup, while
    /// [`DrainPolicy::Drain`] handles the externally accepted mailbox queue;
    /// actor-local continuations are dropped under either policy, and their
    /// loss is reported as a `WARN` before [`on_stop`](Self::on_stop). An error
    /// here fails the run like a [`handle`](Self::handle) error, so under
    /// supervision it is an ordinary restartable failure.
    fn on_start(
        &mut self,
        _ctx: &mut StartContext<'_, Self>,
    ) -> impl Future<Output = ActorResult> + Send {
        async { Ok(()) }
    }

    /// Runs once after the receive loop exits cleanly.
    ///
    /// This hook also runs after a drain and cannot change the stop decision.
    /// During cooperative supervisor removal, the supervisor waits for the
    /// hook before detaching the child and completing
    /// [`RuntimeHandle::remove_child`](crate::RuntimeHandle::remove_child).
    /// Immediate abort, or expiry of the cooperative shutdown grace period,
    /// can abort this hook and detach the child without waiting for it. The
    /// hook's scope handles are
    /// [`RestrictedScope`](crate::RestrictedScope), which withholds the
    /// operations that would wait on that detach.
    /// It is not called when
    /// [`handle`](Self::handle) or [`on_start`](Self::on_start) returns an
    /// error.
    fn on_stop(
        &mut self,
        _ctx: &mut StopContext<'_, Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Returns this handler's shutdown drain policy.
    ///
    /// Defaults to [`DrainPolicy::Drain`], so a handler that says nothing
    /// finishes its queued mailbox before stopping. Override with
    /// [`DrainPolicy::Discard`] for an actor whose queued work is replaceable,
    /// or one holding long-running [offloads](crate::ActorContext::offload) that
    /// shutdown should cut short rather than await.
    ///
    /// # Evaluation point
    ///
    /// This is called exactly once per run, and late: after the receive loop
    /// has already exited and external intake has been closed, but before the
    /// queued mailbox is drained or discarded and before
    /// [`on_stop`](Self::on_stop) runs. It is not consulted at registration, at
    /// [`on_start`](Self::on_start), or on any path through
    /// [`handle`](Self::handle).
    ///
    /// Because the receiver is `&self` and the call happens after the last
    /// `handle`, the answer may depend on state the run accumulated — an actor
    /// can return [`Drain`](DrainPolicy::Drain) only when it is holding
    /// unflushed work and [`Discard`](DrainPolicy::Discard) otherwise. Reading
    /// state here is supported; the value is used immediately and never cached
    /// across runs. The usual implementation is still a constant, or a field
    /// carried from the [`ActorFactory`](crate::ActorFactory) so the policy is
    /// configured once and survives restarts.
    ///
    /// The method must not block or panic: it runs on the shutdown path, where
    /// the budget is already committed and a panic fails a run that was
    /// otherwise stopping cleanly.
    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

impl<H: Actor> RawActor for H {
    type Msg = H::Msg;

    fn readiness_gated(&self) -> bool {
        true
    }

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        self.on_start(&mut StartContext::new(&mut ctx)).await?;
        ctx.mark_ready();

        let mut stopping = ctx.is_stop_requested();
        'receive: while !stopping {
            // External shutdown has priority over actor-local continuations.
            // In particular, a continuation queued by an in-flight handler
            // must not run after shutdown was requested.
            if ctx.shutdown.is_cancelled() {
                break;
            }
            let event = if let Some(message) = ctx.take_continuation() {
                LoopEvent::Message(Some(message))
            } else if let Some(timer) = ctx.next_timer_wake() {
                let shutdown = ctx.shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    () = tokio::time::sleep_until(timer.deadline) => LoopEvent::Timer(timer),
                    message = ctx.next_delivery() => LoopEvent::Message(message),
                }
            } else {
                let shutdown = ctx.shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        break;
                    }
                    message = ctx.next_delivery() => LoopEvent::Message(message),
                }
            };

            match event {
                LoopEvent::Message(Some(message)) => {
                    ctx.record_received();
                    self.handle(message, &mut MessageContext::new(&mut ctx))
                        .await?;
                    stopping = ctx.is_stop_requested();
                }
                LoopEvent::Message(None) => break,
                LoopEvent::Timer(timer) => {
                    // Preserve mailbox arrival order at fire time: messages
                    // already queued get one bounded turn to retract or
                    // replace the elapsed timer. Continuations retain their
                    // priority and do not consume the bounded mailbox prefix.
                    let mut queued_before_fire = ctx.mailbox_depth();
                    loop {
                        if ctx.shutdown.is_cancelled() {
                            break 'receive;
                        }
                        let message = if let Some(message) = ctx.take_continuation() {
                            Some(message)
                        } else if queued_before_fire > 0 {
                            queued_before_fire -= 1;
                            ctx.mailbox.try_recv().ok()
                        } else {
                            None
                        };
                        let Some(message) = message else { break };
                        ctx.record_received();
                        self.handle(message, &mut MessageContext::new(&mut ctx))
                            .await?;
                        if ctx.is_stop_requested() {
                            stopping = true;
                            break;
                        }
                    }
                    if !stopping && let Some(message) = ctx.take_fired_timer(timer) {
                        ctx.record_received();
                        self.handle(message, &mut MessageContext::new(&mut ctx))
                            .await?;
                        stopping = ctx.is_stop_requested();
                    }
                }
            }
        }

        // Detached scope waits are incarnation-owned but are not actor work to
        // drain. Stop them before applying the mailbox/offload drain policy.
        ctx.abort_scope_waits();
        ctx.close_external_intake();
        if self.drain_policy() == DrainPolicy::Drain {
            // Completions and the mailbox are independent loop-owned sources.
            // Drain whichever is ready until the JoinSet is empty and the
            // closed external mailbox has no accepted message left.
            while let Some(message) = ctx.next_drain_delivery().await {
                ctx.record_received();
                // Once stopping begins, later stop requests do not change the
                // drain decision. Continuations queued by drain handlers are
                // left for the context to drop with the incarnation, and
                // reported below. A handler that cares can ask `is_draining`.
                self.handle(message, &mut MessageContext::draining(&mut ctx))
                    .await?;
            }
        } else {
            ctx.abort_offloads();
        }

        // Only the receive loop above takes continuations, so anything still
        // queued here is dropped with the incarnation: pushed by a drain
        // handler, or by an `on_start` that also requested a stop. Both reach
        // `continue_with` through a context type that is legitimately able to
        // queue work at other times, so neither is expressible as a compile
        // error the way `on_stop` and `RawActor` are. Report it.
        if !ctx.continuations.is_empty() {
            ctx.observability
                .emit_continuations_dropped(&ctx.id, ctx.continuations.len());
        }

        self.on_stop(&mut StopContext::new(&mut ctx)).await?;
        Ok(())
    }
}
