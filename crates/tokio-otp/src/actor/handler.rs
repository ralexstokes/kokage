use std::future::Future;

use crate::actor::{
    context::ActorContext,
    raw::{ActorResult, BoxError, Flow, RawActor},
};

/// How the provided [`Actor`] receive loop treats messages still
/// queued when shutdown is requested.
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
#[non_exhaustive]
pub enum DrainPolicy {
    /// Stop immediately.
    ///
    /// Queued messages are dropped and queued [`call`](crate::ActorRef::call)s
    /// observe [`ReplyDropped`](crate::CallError::ReplyDropped). This matches
    /// the behavior of a hand-written `while let Some(message) =
    /// ctx.recv().await` loop.
    #[default]
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
    /// The drain is not separately time-bounded; the surrounding host's
    /// shutdown backstop applies: the explicit bound passed to
    /// [`RunnableActor::run_until`](crate::RunnableActor::run_until) for a
    /// standalone actor, or the child [`ShutdownPolicy`](crate::ShutdownPolicy)
    /// under a runtime.
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
/// A [`Flow::Stop`] exit is normal for monitoring and supervision. An
/// [`Always`](tokio_supervisor::RestartPolicy::Always) child restarts after it;
/// [`OnFailure`](tokio_supervisor::RestartPolicy::OnFailure) and
/// [`Never`](tokio_supervisor::RestartPolicy::Never) children do not.
///
/// # Incarnation construction
///
/// Registration takes a reusable [`ActorFactory`](crate::ActorFactory), whose
/// output is fresh incarnation-local state and need not implement [`Clone`].
/// Acquire fallible or asynchronous per-incarnation resources (connections,
/// files, sessions) in [`on_start`](Self::on_start).
pub trait Actor: Send + Sync + 'static {
    /// The message type this handler receives.
    type Msg: Send + 'static;

    /// Handles one received message.
    ///
    /// Returning [`Flow::Continue`] receives the next message. Returning
    /// [`Flow::Stop`] requests a clean stop; the actor's [`DrainPolicy`] is
    /// applied to the queued mailbox before [`on_stop`](Self::on_stop) runs.
    /// Returning `Err` fails the actor exactly like [`RawActor::run`] returning
    /// `Err`.
    fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut ActorContext<Self::Msg>,
    ) -> impl Future<Output = ActorResult> + Send;

    /// Runs once before the first message of each actor run.
    ///
    /// This is the place to acquire per-incarnation resources. Returning
    /// [`Flow::Stop`] requests a clean stop before the ordinary receive loop.
    /// [`DrainPolicy::Discard`] drops messages queued during startup, while
    /// [`DrainPolicy::Drain`] handles the externally accepted mailbox queue;
    /// actor-local continuations are dropped under either policy. An error
    /// here fails the run like a [`handle`](Self::handle) error, so under
    /// supervision it is an ordinary restartable failure.
    fn on_start(
        &mut self,
        _ctx: &mut ActorContext<Self::Msg>,
    ) -> impl Future<Output = ActorResult> + Send {
        async { Ok(Flow::Continue) }
    }

    /// Runs once after the receive loop exits cleanly.
    ///
    /// This hook also runs after a drain and cannot change the flow decision.
    /// During cooperative supervisor removal, the supervisor waits for the
    /// hook before detaching the child and completing
    /// [`RuntimeHandle::remove_child`](crate::RuntimeHandle::remove_child).
    /// Immediate abort, or expiry of the cooperative shutdown grace period,
    /// can abort this hook and detach the child without waiting for it.
    /// It is not called when
    /// [`handle`](Self::handle) or [`on_start`](Self::on_start) returns an
    /// error.
    fn on_stop(
        &mut self,
        _ctx: &mut ActorContext<Self::Msg>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Returns this handler's shutdown drain policy.
    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Discard
    }
}

impl<H: Actor> RawActor for H {
    type Msg = H::Msg;

    fn readiness_gated(&self) -> bool {
        true
    }

    async fn run(&mut self, mut ctx: ActorContext<Self::Msg>) -> ActorResult {
        let start_flow = self.on_start(&mut ctx).await?;
        ctx.mark_ready();

        let mut stopping = start_flow == Flow::Stop;
        while !stopping {
            // External shutdown has priority over actor-local continuations.
            // In particular, a continuation queued by an in-flight handler
            // must not run after shutdown was requested.
            if ctx.shutdown.is_cancelled() {
                break;
            }
            let message = if let Some(message) = ctx.take_continuation() {
                Some(message)
            } else {
                tokio::select! {
                    biased;
                    _ = ctx.shutdown.cancelled() => {
                        break;
                    }
                    message = ctx.mailbox.recv() => message,
                }
            };

            let Some(message) = message else {
                break;
            };
            ctx.myself.record_received();
            ctx.observability.emit_message_received(&ctx.id);
            stopping = self.handle(message, &mut ctx).await? == Flow::Stop;
        }

        ctx.close_external_intake();
        if self.drain_policy() == DrainPolicy::Drain {
            // Waiting for every step before receiving would deadlock when a
            // full FIFO mailbox backpressures a completion. Drain externally
            // accepted messages and incarnation-local step completions
            // together until both sources are quiescent. Continuations are
            // deliberately ignored once stopping begins.
            loop {
                let message = match ctx.mailbox.try_recv() {
                    Ok(message) => Some(message),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                        if ctx.outstanding_steps() == 0 =>
                    {
                        // A final step can enqueue its postback after the
                        // first poll and then decrement the gauge before this
                        // check. The step's Release decrement synchronizes
                        // with the Acquire load that observed zero, so re-poll
                        // before declaring the mailbox quiescent.
                        ctx.mailbox.try_recv().ok()
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        let changed = ctx.step_change_notify();
                        tokio::select! {
                            message = ctx.mailbox.recv() => message,
                            () = changed.notified() => continue,
                        }
                    }
                };
                let Some(message) = message else { break };
                ctx.myself.record_received();
                ctx.observability.emit_message_received(&ctx.id);
                // Once stopping begins, flow values do not change the drain
                // decision. Continuations queued by drain handlers are left
                // for the context to drop with the incarnation.
                let _ = self.handle(message, &mut ctx).await?;
            }
        } else {
            ctx.abort_steps();
        }

        self.on_stop(&mut ctx).await?;
        Ok(Flow::Stop)
    }
}
