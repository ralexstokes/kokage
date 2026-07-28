use crate::actor::raw::RawActor;

/// A reusable recipe for constructing one actor incarnation.
///
/// [`build`](Self::build) is invoked exactly once for the initial start and
/// once for every supervised restart. It runs inside the supervised actor
/// future, so construction panics follow the same binding, monitoring, and
/// supervision path as startup and run panics.
///
/// Factory fields and closure captures are durable configuration that survives
/// restarts. The returned actor owns fresh incarnation-local state, which does
/// not need to implement [`Clone`]. This boundary is also the place to make
/// state lifetime explicit: counters, allocators, or handles stored by the
/// factory survive actor failure, while values constructed by `build` reset.
/// Shared durable state can live behind an `Arc`, in a database, or in another
/// actor.
///
/// Fallible or asynchronous resource acquisition belongs in
/// [`Actor::on_start`](crate::Actor::on_start) (the OTP `init` idiom), where
/// failure participates in supervision and readiness.
///
/// # Deriving a named factory
///
/// With the default `derive` feature, `#[derive(ActorFactory)]` generates an
/// `<Actor>Factory` containing each unmarked actor field. Those fields are
/// cloned for every incarnation. `#[factory(default)]` omits local fields from
/// the generated factory and freshly default-constructs them instead:
///
/// ```
/// # #[cfg(feature = "derive")]
/// # fn main() {
/// # use std::sync::{Arc, atomic::AtomicU64};
/// # use tokio_otp::{Actor, MessageContext, ActorResult, GraphBuilder};
/// #[derive(tokio_otp::ActorFactory)]
/// struct Worker {
///     // This allocator lives in WorkerFactory, so its value survives restarts.
///     ids: Arc<AtomicU64>,
///     // This queue belongs to one actor incarnation and resets on restart.
///     #[factory(default)]
///     pending: Vec<String>,
/// }
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         let _ = (&self.ids, &self.pending);
/// #         Ok(())
/// #     }
/// # }
///
/// let ids = Arc::new(AtomicU64::new(0));
/// let mut graph = GraphBuilder::new();
/// let (actor_slot, _) = graph.slot("worker");
/// graph.define(actor_slot, WorkerFactory { ids: ids.clone() });
/// # }
/// # #[cfg(not(feature = "derive"))]
/// # fn main() {}
/// ```
///
/// Derivation is intended for the common clone-configuration/default-local
/// split. Implement this trait directly when fresh fields need custom
/// synchronous construction.
///
/// Any zero-argument constructor path, such as `Worker::new` or
/// `Worker::default`, already implements this trait through the blanket
/// implementation for closures and functions; no `Default`-specific factory
/// machinery is needed. A direct blanket implementation for every
/// `RawActor + Default` actor would overlap this closure blanket and is not
/// permitted by Rust's coherence rules (E0119).
pub trait ActorFactory: Send + Sync + 'static {
    /// The actor constructed for each incarnation.
    type Actor: RawActor;

    /// Constructs fresh incarnation-local actor state.
    fn build(&self) -> Self::Actor;
}

impl<A, F> ActorFactory for F
where
    A: RawActor,
    F: Fn() -> A + Send + Sync + 'static,
{
    type Actor = A;

    fn build(&self) -> Self::Actor {
        self()
    }
}
