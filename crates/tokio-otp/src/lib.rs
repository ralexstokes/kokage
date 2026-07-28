#![warn(missing_docs)]

//! The front door to OTP-style fault tolerance on `tokio`: supervised typed
//! actor graphs with one integrated [`Runtime`].
//!
//! For the common setup — every actor of a graph running as its own
//! supervised child — build a graph, place it in a [`SupervisionTree`], and
//! spawn the resulting runtime:
//!
//! ```no_run
//! use tokio_otp::prelude::*;
//!
//! struct Echo;
//!
//! impl Actor for Echo {
//!     type Msg = String;
//!
//!     async fn handle(&mut self, message: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
//!         println!("{message}");
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut graph = GraphBuilder::new();
//! let (echo_slot, echo) = graph.slot("Echo");
//! graph.define(echo_slot, || Echo);
//!
//! let graph = graph.build()?;
//! let runtime = SupervisionTree::graph(&graph)
//!     .strategy(Strategy::OneForOne)
//!     .build()?;
//! let handle = runtime.spawn();
//!
//! echo.send("hello".to_owned()).await?;
//! handle.shutdown_and_wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! See [`SupervisionTree`] for recursive composition and per-actor policy
//! examples. [`RuntimeBuilder`] remains thin graph-in-one-scope sugar. The
//! [`prelude`] re-exports the day-one composition and actor surface plus the
//! core `tokio-supervisor` policies. Observability, advanced configuration,
//! and raw supervisor construction stay at their crate roots.
//!
//! # Core types
//!
//! | Type | Role |
//! |------|------|
//! | [`SupervisionTree`] / [`ReservedSupervisionTree`] | Primary recursive declaration; reserve the non-cloneable form when a scope handle is needed before build. |
//! | [`Runtime`] / [`RuntimeBuilder`] | Configured executable runtime, plus thin graph-in-one-scope sugar. |
//! | [`RuntimeHandle`] | Control surface for shutdown, completion, and observability; dynamic-scope handles also add actors, task children, and subtrees. |
//! | [`GraphBuilder`] / [`Graph`] | Constructs and validates the actor graph; wiring plus runnable actors. |
//! | [`Actor`] | Handler-style actor definition with a provided receive loop. |
//! | [`RawActor`] | Custom-loop typed actor definition (the escape hatch). |
//! | [`ActorRef`] | Cloneable, restart-stable, typed mailbox sender. |
//! | [`ActorContext`] | The full context a [`RawActor`] run receives: mailbox, watches, lifetime, blocking work, shutdown token. |
//! | [`StartContext`] / [`MessageContext`] / [`StopContext`] | Stage views of that context handed to the [`Actor`] lifecycle hooks. |
//! | [`AmbientContext`] | Identity, shutdown observation, and blocking work shared by every context. |
//! | [`LiveContext`] | Timers, continuations, and other capabilities shared by the running stages. |
//! | [`MailboxMode`] | FIFO or latest-wins storage policy selected per actor. |
//! | [`Reply`] | One-shot response channel carried inside request messages. |
//! | [`RunnableActor`] | One actor plus stable binding — the unit of execution. |
//! | [`timers`] | Cross-actor one-shot and interval delivery tied to an actor lifetime. |
//!
//! # Composition modes
//!
//! - **Ordered actor trees** via [`SupervisionTree::new`] or
//!   [`SupervisionTree::graph`]: per-actor supervision, recursive actor-aware
//!   subtrees, arbitrary task children, and actor-owned scopes.
//! - **Dynamic actor membership** via [`SupervisionTree::dynamic`]: an
//!   initially empty `OneForOne` scope that accepts actors and subtrees at
//!   runtime. [`Runtime::dynamic`] is its pre-reserved convenience builder.
//!
//! Fate-sharing is selected with [`Strategy::OneForAll`]
//! or supervision-tree shape; graphs themselves are not execution units.
//!
//! # Delivery contract: at-most-once
//!
//! Mailboxes are incarnation-owned: each actor run binds a fresh mailbox, and
//! messages accepted by a dead incarnation are lost with it. Delivery is
//! therefore **at-most-once**, with loss windows at restart and shutdown.
//! Stronger guarantees (acknowledgements, redelivery) are user protocol built
//! on [`ActorRef::call`] and [`Reply`], not transport features.
//!
//! [`ActorContext::recv`] is fail-fast during shutdown: it returns `None` as
//! soon as shutdown is requested, even when messages are still queued. That is
//! the primitive, not the default policy: [`Actor`]'s framework-owned loop
//! defaults to [`DrainPolicy::Drain`] and finishes the queued mailbox before
//! stopping. A hand-written [`RawActor`] loop opts back in with
//! [`ActorContext::try_recv`].
//!
//! Restarts also lose queued messages: a restarted actor binds a fresh
//! mailbox, so messages queued behind a poison message are dropped with the
//! old one. This is deliberate — a mailbox that survived restarts would
//! redeliver the poison message that caused the crash, converting one
//! failure into a restart loop. [`ActorRef::send`] rides through restart
//! windows when a rebind is expected. Registration factories are invoked once
//! per incarnation, so a restart receives freshly constructed actor state (see
//! [`Actor`]).
//!
//! Actors can watch a peer with [`ActorContext::watch`]. The watch follows
//! the logical actor across restarts: each [`MonitorEvent`] — `Up` when an
//! incarnation starts, `Down` when it exits, a final `Terminated` when the
//! actor is permanently gone, and `Lagged` if a stalled observer misses
//! transitions under overload — is mapped into the observer's message type and
//! delivered through the ordinary mailbox. Watches survive restarts of both
//! actors, [`CancellationHandle::cancel`] suppresses future delivery, and
//! permanent removal of either actor membership ends the watch.
//!
//! # Static declarations
//!
//! Use `#[derive(ActorFactory)]` on named-field actors to generate reusable
//! factory structs without repeating configuration fields or clone code. For
//! cyclic actor graphs and the supervision scopes that run them, derive
//! [`Supervision`] on a named-field struct whose fields are the actors. The
//! wiring closure receives typed refs for every field before any actor is
//! constructed; see the [`Supervision`] docs for the full contract, and mind
//! the bounded-mailbox cycle hazard documented on [`GraphBuilder`].
//!
//! # Hand-driving actors
//!
//! Supervision through [`Runtime`] is the normal host, but the execution
//! surface is public: each [`RunnableActor`] can be driven directly with
//! [`run_until`](RunnableActor::run_until), which is how tests (and hosts
//! with their own supervision story) run actors.
//!
//! ```
//! use tokio_otp::{
//!     Actor, ActorResult, CancellationToken, DEFAULT_SHUTDOWN_BOUND, GraphBuilder, MessageContext,
//!     Reply, RestartPolicy,
//! };
//!
//! enum CounterMsg {
//!     Add(u64),
//!     Total(Reply<u64>),
//! }
//!
//! struct Counter {
//!     total: u64,
//! }
//!
//! impl Actor for Counter {
//!     type Msg = CounterMsg;
//!
//!     async fn handle(
//!         &mut self,
//!         message: CounterMsg,
//!         _ctx: &mut MessageContext<'_, Self>,
//!     ) -> ActorResult {
//!         match message {
//!             CounterMsg::Add(n) => self.total += n,
//!             CounterMsg::Total(reply) => reply.send(self.total),
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = GraphBuilder::new();
//! builder.name("example");
//! let (counter_slot, counter) = builder.slot("Counter");
//! builder.define(counter_slot, || Counter { total: 0 });
//! let graph = builder.build().expect("valid graph");
//!
//! let actor = graph.actors()[0].clone();
//! let stop = CancellationToken::new();
//! let run = tokio::spawn({
//!     let stop = stop.clone();
//!     async move {
//!         actor
//!             .run_until(stop.cancelled(), RestartPolicy::Never, DEFAULT_SHUTDOWN_BOUND)
//!             .await
//!     }
//! });
//!
//! counter.send(CounterMsg::Add(2)).await.expect("send succeeded");
//! counter.send(CounterMsg::Add(3)).await.expect("send succeeded");
//! assert_eq!(
//!     counter
//!         .call(std::time::Duration::from_secs(1), CounterMsg::Total)
//!         .await?,
//!     5
//! );
//!
//! stop.cancel();
//! run.await??;
//! # Ok(())
//! # }
//! ```
//!
//! # Observability
//!
//! `tracing` spans and structured logs are emitted automatically for
//! supervisor, actor, and mailbox lifecycle. Pull-based message counters and
//! live mailbox usage are available through [`ActorRef::stats`] and
//! [`RuntimeHandle::actor_stats`]; exporting time-series is a user-side
//! sampler task over those surfaces (see `examples/actor_metrics.rs`).
//! Actors registered with [`ActorOptions::message_size`] expose
//! application-defined accepted-byte totals through [`ActorStats`] and emit
//! size metrics when the `metrics` feature is enabled. The same options can
//! select a [`MailboxMode`] and apply to statically or dynamically registered
//! actors.
//! Install subscribers and samplers at the application boundary, not inside
//! the library.
//!
//! # Deliberate dependency coupling
//!
//! Public mailbox errors are crate-owned, so changing the underlying channel
//! implementation does not change the actor API. Cancellation is deliberately
//! different: [`CancellationToken`] is the shared shutdown vocabulary at the
//! Tokio ecosystem boundary. [`ActorContext::shutdown_token`],
//! [`ActorContext::run_blocking`], and the shutdown futures passed to
//! [`RunnableActor::run_until`] compose directly with that exact
//! `tokio_util::sync::CancellationToken` type. Applications can therefore
//! connect actor shutdown to existing cancellation trees without adapters. The
//! token is re-exported at this crate's root so applications do not need an
//! additional dependency path.
//!
//! Snapshot subscriptions deliberately expose Tokio's
//! [`watch::Receiver`](tokio::sync::watch::Receiver). Both
//! [`RuntimeHandle::subscribe_snapshots`] and
//! [`RestrictedScope::subscribe_snapshots`] return the ecosystem type so
//! consumers can use its conflating delivery and `wait_for` API directly.
//!
//! # Examples
//!
//! - `examples/supervised_actors.rs` — per-actor supervision with default
//!   policies.
//! - `examples/supervision.rs` — a cyclic graph wired with
//!   `#[derive(Supervision)]`.
//! - `examples/drain_policy.rs` — draining queued actor messages during
//!   shutdown.
//! - `examples/individual_actor_policies.rs` — per-actor restart/shutdown
//!   overrides.
//! - `examples/dynamic_actors.rs` — adding and removing actors at runtime.
//! - `examples/directory.rs` — a typed, userland name directory actor.
//! - `examples/ref_rebind.rs` — refs riding through supervised restarts.
//! - `examples/graph_failures.rs` — supervisor policy around actor failures.
//! - `examples/mailbox_backpressure.rs`, `examples/send_vs_try_send.rs` —
//!   bounded mailboxes and send flavors.
//! - `examples/builder_validation.rs` — build-time graph validation errors.
//! - `examples/blocking_work.rs`, `examples/blocking_lifecycle.rs` —
//!   cooperative and detached blocking work.
//! - `examples/actor_metrics.rs` — the stats-sampler export pattern.
//! - `examples/actor_tracing.rs`, `examples/supervisor_snapshot_trace.rs` —
//!   tracing and snapshot observability.
//! - `examples/json_edge.rs` — decoding byte-oriented JSON frames into typed
//!   actor messages with `serde_json` at the application boundary.
//!
//! # Cargo features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | yes | Re-exports `#[derive(ActorFactory)]` and `#[derive(Supervision)]`. |
//! | `metrics` | no | Supervisor lifecycle metrics plus opt-in actor message-size metrics. |
//! | `serde` | no | Serialization support for supervision outlines and view types. |

mod actor;
mod builder;
mod runtime;
mod supervision;
mod supervision_derive;
pub mod timers;

/// Common imports for `tokio-otp` consumers.
///
/// This prelude is intentionally limited to the actor traits and contexts,
/// primary [`SupervisionTree`] composition path, core policies, and common
/// send/call errors. Observability and advanced surfaces remain available at
/// the crate root without being injected by a glob import.
pub mod prelude {
    // Resolves the `Supervision` trait, and additionally the derive macro of
    // the same name when the `derive` feature is on.
    pub use crate::{
        Actor, ActorContext, ActorFactory, ActorOptions, ActorRef, ActorResult, ActorSpec,
        AmbientContext, BoxError, CallError, GraphBuilder, LiveContext, MessageContext, RawActor,
        Reply, RestartIntensity, RestartPolicy, Runtime, RuntimeHandle, SendError, ShutdownPolicy,
        StartContext, StopContext, Strategy, Supervision, SupervisionTree,
    };
}

#[cfg(feature = "derive")]
pub use tokio_otp_derive::{ActorFactory, Supervision};

pub use actor::{
    Actor, ActorContext, ActorFactory, ActorOptions, ActorRef, ActorResult, ActorRunError,
    ActorSlot, ActorStats, ActorSupervisorPathSegment, AmbientContext, BlockingCancelled,
    CallError, CancellationHandle, DEFAULT_SHUTDOWN_BOUND, Down, DownReason, DrainPolicy, Graph,
    GraphBuildError, GraphBuilder, Lifetime, LiveContext, MailboxMode, MessageContext, MessageSize,
    MonitorEvent, OffloadDeadline, OffloadHandle, RawActor, Reply, RestrictedScope, RunnableActor,
    SendError, StartContext, StopContext, TimerKey, TryRecvError,
};
pub use builder::{DynamicRuntimeBuilder, RuntimeBuilder};
pub use runtime::{
    AddSubtreeError, DynamicActorOptions, LifecycleWatchGuard, Runtime, RuntimeHandle,
};
pub use supervision::{
    ActorSpec, ChildOutline, ReservedSupervisionTree, SupervisionOutline, SupervisionScope,
    SupervisionTree,
};
#[doc(hidden)]
pub use supervision_derive::qualified_label;
pub use supervision_derive::{
    DynamicScope, Supervision, SupervisionBuildError, SupervisionFactories,
};
#[doc(hidden)]
pub use tokio_supervisor::{AttachedChild, AttachedChildIdentity};
pub use tokio_supervisor::{
    BackoffPolicy, BoxError, ChildMembershipView, ChildSnapshot, ChildSpec, ChildStateView,
    CompletionGuard, CompletionOutcome, ControlError, ControlOperation, ExitStatusView,
    LifecycleEvent, LifecyclePathSegment, LifecycleWatch, RestartIntensity, RestartPolicy,
    ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, SupervisorBuildError, SupervisorError,
    SupervisorSnapshot, SupervisorStateView,
};
pub use tokio_util::sync::CancellationToken;
