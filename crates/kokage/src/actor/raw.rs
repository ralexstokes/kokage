use std::{future::Future, time::Duration};

use crate::actor::context::RawContext;
pub(crate) use crate::supervisor::BoxError;

/// The result type returned by actor callbacks and supervised task factories.
///
/// `Ok(())` keeps a handler-style actor running after startup or one handled
/// message. Call [`Context::stop`](crate::Context::stop) before
/// returning successfully to request a clean self-stop. A custom [`RawActor`]
/// owns its receive loop, so returning `Ok(())` simply completes that actor
/// cleanly. A [`TaskSpec`](crate::TaskSpec) factory uses the same result:
/// `Ok(())` is a clean task exit, while `Err` is a supervised failure.
pub type ExitResult = Result<(), BoxError>;

/// Async actor interface with a typed mailbox.
///
/// [`Actor`](crate::Actor) is the recommended starting
/// point for ordinary actors: it provides the receive loop, lifecycle hooks,
/// and shutdown drain policy. Implement `RawActor` directly when an actor needs
/// custom loop control.
///
/// # Capability contract
///
/// A raw actor borrows the mailbox-owning [`RawContext`], including
/// `recv`, `try_recv`, and `mark_ready`. Loop-owned timers and continuations
/// depend on the framework-owned handler loop, so a raw actor expresses those
/// branches directly with Tokio futures beside `recv`, while watches,
/// offloads, blocking work, identity, and scope access remain available on
/// `RawContext` itself.
///
/// Implementors can use
/// `async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult` in
/// their trait impls. Registration takes a reusable
/// [`ActorFactory`](crate::ActorFactory), so each run owns fresh
/// incarnation-local state, including non-[`Clone`] fields. Custom raw actors
/// need not implement [`Sync`] because each incarnation moves into one task;
/// the reusable factory remains `Send + Sync` across restarts. Custom raw
/// actors can acquire fallible or asynchronous resources at the start of
/// [`run`](Self::run), where failure participates in supervision and readiness.
///
/// This trait is deliberately not implemented for plain closures: an actor is
/// a named type that implements `RawActor`, which keeps the message type visible
/// at the definition site and the actor's state explicit.
pub trait RawActor: Send + 'static {
    /// The message type this actor receives.
    type Msg: Send + 'static;

    /// Returns the deadline for reporting readiness explicitly from
    /// [`RawContext::mark_ready`](crate::raw::RawContext::mark_ready).
    ///
    /// Handler-style [`Actor`](crate::Actor) implementations do this
    /// automatically after `on_start`; custom raw actors are ready immediately
    /// unless they return a deadline from this method. Missing that deadline
    /// fails the actor incarnation and is governed by its restart policy. A
    /// shutdown request disarms the readiness deadline so the actor retains
    /// its configured cooperative shutdown grace. If readiness and the
    /// deadline are both observable in the same scheduler turn, readiness
    /// wins.
    fn manual_readiness(&self) -> Option<Duration> {
        None
    }

    /// Runs the actor until it finishes or shutdown is requested.
    ///
    /// The context belongs to the actor incarnation and is borrowed for this
    /// invocation. A `RawActor` decorator may therefore inspect the context
    /// after an inner actor returns or invoke that actor again with the same
    /// context. Repeated invocations share all incarnation-local state:
    /// readiness can only be reported once, a stop request remains set, and
    /// the same mailbox, timers, offloads, watches, and identity carry over.
    /// A custom raw inner actor's return does not by itself close external
    /// mailbox intake. The framework guarantees closure after the outermost
    /// invocation returns. The provided [`Actor`](crate::Actor) loop closes
    /// intake whenever its receive loop decides to stop, including a local
    /// stop, so it can drain a fixed accepted prefix. Once closed, intake
    /// remains closed across later calls.
    fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> impl Future<Output = ExitResult> + Send;
}
