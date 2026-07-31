use std::future::Future;

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
/// A raw actor receives the mailbox-owning [`RawContext`], including
/// `recv`, `try_recv`, and `mark_ready`. Loop-owned timers and continuations
/// depend on the framework-owned handler loop, so a raw actor expresses those
/// branches directly with Tokio futures beside `recv`, while watches,
/// offloads, blocking work, identity, and scope access remain available on
/// `RawContext` itself.
///
/// Implementors can use
/// `async fn run(&mut self, ctx: RawContext<Self::Msg>) -> ExitResult` in
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

    /// Returns whether this actor reports readiness explicitly from
    /// [`RawContext::mark_ready`](crate::raw::RawContext::mark_ready).
    ///
    /// Handler-style [`Actor`](crate::Actor) implementations do this
    /// automatically after `on_start`; custom raw actors are ready immediately
    /// unless they override this method.
    fn readiness_gated(&self) -> bool {
        false
    }

    /// Runs the actor until it finishes or shutdown is requested.
    fn run(&mut self, ctx: RawContext<Self::Msg>) -> impl Future<Output = ExitResult> + Send;
}
