use std::future::Future;

use crate::actor::context::ActorContext;
pub(crate) use kokage_supervisor::BoxError;

/// The result type returned by actor run, startup, and message functions.
///
/// `Ok(())` keeps a handler-style actor running after startup or one handled
/// message. Call [`LiveContext::stop`](crate::LiveContext::stop) before
/// returning successfully to request a clean self-stop. A custom [`RawActor`]
/// owns its receive loop, so returning `Ok(())` simply completes that actor
/// cleanly.
pub type ActorResult = Result<(), BoxError>;

/// Async actor interface with a typed mailbox.
///
/// [`Actor`](crate::Actor) is the recommended starting
/// point for ordinary actors: it provides the receive loop, lifecycle hooks,
/// and shutdown drain policy. Implement `RawActor` directly when an actor needs
/// custom loop control.
///
/// # Capability contract
///
/// A raw actor receives the mailbox-owning [`ActorContext`], including
/// `recv`, `try_recv`, and `mark_ready`. It does not implement
/// [`LiveContext`](crate::LiveContext): loop-owned timers and continuations
/// depend on the framework-owned handler loop. A raw actor expresses those
/// branches directly with Tokio futures beside `recv`, while watches,
/// offloads, blocking work, identity, and restricted scope access remain
/// available on `ActorContext` itself.
///
/// Implementors can use
/// `async fn run(&mut self, ctx: ActorContext<Self::Msg>) -> ActorResult` in
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
    /// [`ActorContext::mark_ready`](crate::ActorContext::mark_ready).
    ///
    /// Handler-style [`Actor`](crate::Actor) implementations do this
    /// automatically after `on_start`; custom raw actors are ready immediately
    /// unless they override this method.
    fn readiness_gated(&self) -> bool {
        false
    }

    /// Runs the actor until it finishes or graph shutdown is requested.
    fn run(&mut self, ctx: ActorContext<Self::Msg>) -> impl Future<Output = ActorResult> + Send;
}
