use std::future::Future;

use crate::actor::{
    context::{Context, RawContext, StopContext, TimerWake},
    raw::{BoxError, ExitResult, RawActor},
};

enum LoopEvent<M> {
    Message(Option<M>),
    Timer(TimerWake),
}

/// Handler-style actor interface with a framework-owned receive loop.
///
/// Implement this trait when an actor only needs one method per message. The
/// blanket [`RawActor`] implementation receives messages in mailbox order and
/// applies the declaration's [`Shutdown`](crate::Shutdown) behavior.
///
/// Hand-writing [`RawActor::run`] remains the escape hatch for actors that need
/// custom loop control.
///
/// # Capability contract
///
/// Handler actors do not receive the mailbox-owning [`RawContext`]. The
/// framework owns `recv`, `try_recv`, and readiness reporting, and hands each
/// hook a [`Context`] instead. It provides loop-owned timers, continuations,
/// and actor-owned scope waits that a custom raw loop must express directly.
/// Watches and offloads remain available on [`RawContext`].
///
/// A [`Context::stop`] exit is normal for monitoring and supervision. A
/// [`Restart::always`](crate::Restart::always) child restarts after it;
/// [`Restart::on_failure`](crate::Restart::on_failure) and
/// [`Restart::never`](crate::Restart::never) children do not.
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
    /// [`ctx.stop()`](Context::stop) was called. A stop request is
    /// clean: the actor's declared shutdown behavior is applied to the queued mailbox
    /// before [`on_stop`](Self::on_stop) runs. Returning `Err` fails the actor
    /// exactly like [`RawActor::run`] returning `Err`.
    fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut Context<'_, Self>,
    ) -> impl Future<Output = ExitResult> + Send;

    /// Runs once before the first message of each actor run.
    ///
    /// This is the place to acquire per-incarnation resources. The framework
    /// reports readiness after this hook returns successfully. Calling
    /// [`ctx.stop()`](Context::stop) requests a clean stop before the ordinary
    /// receive loop.
    /// [`Shutdown::discard_after_current`](crate::Shutdown::discard_after_current)
    /// drops messages queued during startup, while
    /// [`Shutdown::drain_for`](crate::Shutdown::drain_for) handles the accepted queue;
    /// actor-local continuations are dropped under either policy, and their
    /// loss is reported as a `WARN` before [`on_stop`](Self::on_stop). An error
    /// here fails the run like a [`handle`](Self::handle) error, so under
    /// supervision it is an ordinary restartable failure.
    fn on_start(
        &mut self,
        _ctx: &mut Context<'_, Self>,
    ) -> impl Future<Output = ExitResult> + Send {
        async { Ok(()) }
    }

    /// Runs once after the receive loop exits cleanly.
    ///
    /// This hook also runs after a drain and cannot change the stop decision.
    /// During cooperative supervisor removal, the supervisor waits for the
    /// hook before detaching the child and completing
    /// [`ScopeRef::remove_child`](crate::ScopeRef::remove_child).
    /// Immediate abort, or expiry of the cooperative shutdown grace period,
    /// can abort this hook and detach the child without waiting for it. The
    /// hook's scope references are
    /// [`RestrictedScopeRef`](crate::RestrictedScopeRef), which withholds the
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
}

impl<H: Actor> RawActor for H {
    type Msg = H::Msg;

    fn readiness_gated(&self) -> bool {
        true
    }

    async fn run(&mut self, mut ctx: RawContext<Self::Msg>) -> ExitResult {
        self.on_start(&mut Context::new(&mut ctx)).await?;
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
                    self.handle(message, &mut Context::new(&mut ctx)).await?;
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
                        self.handle(message, &mut Context::new(&mut ctx)).await?;
                        if ctx.is_stop_requested() {
                            stopping = true;
                            break;
                        }
                    }
                    if !stopping && let Some(message) = ctx.take_fired_timer(timer) {
                        ctx.record_received();
                        self.handle(message, &mut Context::new(&mut ctx)).await?;
                        stopping = ctx.is_stop_requested();
                    }
                }
            }
        }

        // Detached scope waits are incarnation-owned but are not actor work to
        // drain. Stop them before applying the mailbox/offload drain policy.
        ctx.abort_scope_waits();
        ctx.close_external_intake();
        if ctx.drain_messages {
            // Completions and the mailbox are independent loop-owned sources.
            // Drain whichever is ready until the JoinSet is empty and the
            // closed external mailbox has no accepted message left.
            while let Some(message) = ctx.next_drain_delivery().await {
                ctx.record_received();
                // Once stopping begins, later stop requests do not change the
                // drain decision. Continuations queued by drain handlers are
                // left for the context to drop with the incarnation, and
                // reported below. A handler that cares can inspect `status`.
                self.handle(message, &mut Context::draining(&mut ctx))
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
