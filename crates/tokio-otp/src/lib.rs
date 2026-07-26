#![warn(missing_docs)]

//! The front door to OTP-style fault tolerance on `tokio`: supervised typed
//! actor graphs with one integrated [`Runtime`].
//!
//! For the common setup — every actor of a graph running as its own
//! supervised child — you need one import and one builder:
//!
//! ```no_run
//! use tokio_otp::prelude::*;
//!
//! struct Echo;
//!
//! impl Actor for Echo {
//!     type Msg = String;
//!
//!     async fn handle(&mut self, message: String, _ctx: &mut MessageContext<'_, String>) -> ActorResult {
//!         println!("{message}");
//!         Ok(Continue)
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut graph = GraphBuilder::new();
//! let echo = graph.add(|| Echo);
//!
//! let runtime = Runtime::builder()
//!     .graph(graph.build()?)
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
//! See [`RuntimeBuilder`] for a complete example. The [`prelude`] re-exports
//! this crate's whole surface plus the common types of `tokio-supervisor`,
//! which remains independently usable for supervision without actors.
//!
//! # Core types
//!
//! | Type | Role |
//! |------|------|
//! | [`Runtime`] / [`RuntimeBuilder`] | Owns a supervisor and actor factory — the common composition. |
//! | [`RuntimeHandle`] | Control surface for shutdown and observability; dynamic-scope handles also mutate membership. |
//! | [`GraphBuilder`] / [`Graph`] | Constructs and validates the actor graph; wiring plus runnable actors. |
//! | [`Actor`] | Handler-style actor definition with a provided receive loop. |
//! | [`RawActor`] | Custom-loop typed actor definition (the escape hatch). |
//! | [`ActorRef`] | Cloneable, restart-stable, typed mailbox sender. |
//! | [`ActorContext`] | The full context a [`RawActor`] run receives: mailbox, watches, timers, blocking work, shutdown token. |
//! | [`StartContext`] / [`MessageContext`] / [`StopContext`] | Stage views of that context handed to the [`Actor`] lifecycle hooks. |
//! | [`LiveContext`] | The capabilities shared by the running stages; the type shared helpers take. |
//! | [`MailboxMode`] | FIFO or latest-wins storage policy selected per actor. |
//! | [`Reply`] | One-shot response channel carried inside request messages. |
//! | [`RunnableActor`] | One actor plus stable binding — the unit of execution. |
//!
//! # Composition modes
//!
//! - **Ordered actor trees** via [`Runtime::builder`]: per-actor supervision
//!   with per-actor policy overrides, recursive actor-aware subtrees, and
//!   arbitrary statically declared non-actor children. Add nested scopes with
//!   [`RuntimeBuilder::subtree`].
//! - **Dynamic actor membership** via [`Runtime::dynamic`]: an initially empty
//!   `OneForOne` scope that accepts actors and subtrees at runtime.
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
//! # Static topologies
//!
//! Use `#[derive(ActorFactory)]` on named-field actors to generate reusable
//! factory structs without repeating configuration fields or clone code. For cyclic
//! actor graphs, derive [`Topology`] on a named-field struct whose fields are
//! the actors. The wiring closure receives typed refs for every field before
//! any actor is constructed; see the [`Topology`] docs for the full contract,
//! and mind the bounded-mailbox cycle hazard documented on [`GraphBuilder`].
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
//! use tokio_otp::prelude::Continue;
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
//!         _ctx: &mut MessageContext<'_, CounterMsg>,
//!     ) -> ActorResult {
//!         match message {
//!             CounterMsg::Add(n) => self.total += n,
//!             CounterMsg::Total(reply) => reply.send(self.total),
//!         }
//!         Ok(Continue)
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = GraphBuilder::new();
//! builder.name("example");
//! let counter = builder.add(|| Counter { total: 0 });
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
//! token is re-exported here (and in [`prelude`]) so common local
//! examples and small applications do not need an additional import path.
//!
//! # Examples
//!
//! - `examples/supervised_actors.rs` — per-actor supervision with default
//!   policies.
//! - `examples/topology.rs` — a cyclic graph wired with `#[derive(Topology)]`.
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
//! | `derive` | yes | Re-exports `#[derive(ActorFactory)]` and `#[derive(Topology)]`. |
//! | `metrics` | no | Supervisor lifecycle metrics plus opt-in actor message-size metrics. |

mod actor;
mod builder;
mod runtime;
mod supervision;
mod topology;

/// Common imports for `tokio-otp` consumers.
///
/// This prelude is intentionally limited to the traits, builders, policies,
/// and observation types used by the primary [`Runtime::builder`] composition
/// path. Common send/call errors are included; other error types and advanced
/// composition surfaces remain available at the crate root without being
/// injected by a glob import.
pub mod prelude {
    // Resolves the `Topology` trait, and additionally the derive macro of the
    // same name when the `derive` feature is on.
    pub use crate::{
        Actor, ActorContext, ActorFactory, ActorOptions, ActorRef, ActorResult, AddSubtreeError,
        BlockingCancelled, BoxError, CallError, CancellationHandle, CancellationToken, Down,
        DownReason, DrainPolicy, DynamicRuntimeBuilder, DynamicScope, Flow,
        Flow::{Continue, Stop},
        Graph, GraphBuilder, LifecycleWatchGuard, LiveContext, MailboxMode, MessageContext,
        MessageSize, MonitorEvent, OffloadDeadline, OffloadHandle, RawActor, Reply, Runtime,
        RuntimeBuilder, RuntimeHandle, SendError, StartContext, StartingScope, StateTimeoutSlot,
        StopContext, StoppingScope, SupervisionTree, Topology, TopologyBuildError,
        TopologyFactories,
    };
    pub use tokio_supervisor::{
        AttachedChild, AttachedChildIdentity, BackoffPolicy, ChildMembershipView, ChildSnapshot,
        ChildStateView, CompletionGuard, CompletionOutcome, ControlOperation, ExitStatusView,
        LifecycleEvent, LifecycleEventKind, LifecyclePathSegment, LifecycleWatch,
        RecursiveLifecycleEvent, RecursiveLifecycleEventKind, RecursiveLifecycleWatch,
        RestartIntensity, RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy, Strategy,
        SupervisorSnapshot, SupervisorStateView, prelude::SupervisorSnapshotReceiverExt,
    };
}

#[cfg(feature = "derive")]
pub use tokio_otp_derive::{ActorFactory, Topology};

pub use actor::{
    Actor, ActorContext, ActorFactory, ActorOptions, ActorRef, ActorResult, ActorRunError,
    ActorSlot, ActorStats, BlockingCancelled, BoxError, CallError, CancellationHandle,
    DEFAULT_SHUTDOWN_BOUND, Down, DownReason, DrainPolicy, Flow, Graph, GraphBuildError,
    GraphBuilder, LiveContext, MailboxMode, MessageContext, MessageSize, MonitorEvent,
    OffloadDeadline, OffloadHandle, RawActor, Reply, RunnableActor, RunnableActorBuilder,
    SendError, StartContext, StartingScope, StateTimeoutSlot, StopContext, StoppingScope,
    SupervisorPathSegment, TryRecvError,
};
pub use builder::{DynamicRuntimeBuilder, RuntimeBuilder};
pub use runtime::{
    AddSubtreeError, DynamicActorOptions, LifecycleWatchGuard, Runtime, RuntimeHandle,
};
pub use supervision::{
    ActorSpec, ChildOutline, SupervisionOutline, SupervisionScope, SupervisionTree,
};
pub use tokio_supervisor::{
    AttachedChild, AttachedChildIdentity, BackoffPolicy, ChildContext, ChildMembershipView,
    ChildResult, ChildSnapshot, ChildSpec, ChildStateView, CompletionGuard, CompletionOutcome,
    ControlError, ControlOperation, DynamicSupervisorBuilder, ExitStatusView, LifecycleEvent,
    LifecycleEventKind, LifecyclePathSegment, LifecycleWatch, RecursiveLifecycleEvent,
    RecursiveLifecycleEventKind, RecursiveLifecycleWatch, RestartIntensity, RestartPolicy,
    ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, Supervisor, SupervisorBuildError,
    SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSnapshot, SupervisorSpec,
    SupervisorStateView, SupervisorToken, prelude::SupervisorSnapshotReceiverExt,
};
pub use tokio_util::sync::CancellationToken;
#[doc(hidden)]
pub use topology::qualified_label;
pub use topology::{DynamicScope, Topology, TopologyBuildError, TopologyFactories};
