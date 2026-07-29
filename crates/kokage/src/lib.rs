#![warn(missing_docs)]

//! The front door to OTP-style supervision trees and typed actors over an
//! async scheduler (Tokio today), with an owning [`Runtime`] and integrated
//! non-owning [`RuntimeHandle`] values.
//!
//! For the common setup — every actor of a graph running as its own
//! supervised child — build a graph, move it into an [`OrderedTree`], and
//! spawn the tree:
//!
//! ```no_run
//! use kokage::prelude::*;
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
//! let echo = graph.actor("Echo", || Echo);
//!
//! let graph = graph.build()?;
//! let handle = OrderedTree::graph(graph)
//!     .strategy(Strategy::OneForOne)
//!     .spawn()?;
//!
//! echo.send("hello".to_owned()).await?;
//! handle.shutdown_and_wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! See [`OrderedTree`] for recursive composition and per-actor policy
//! examples. The [`prelude`] re-exports the day-one composition and actor
//! surface plus the core `kokage-supervisor` policies. Host-facing execution
//! types live in [`host`], observation types live in [`observe`], and advanced
//! actor configuration stays at the crate root.
//!
//! These tiers are semantic, not a hard cap on the number of root exports.
//! Types that configure actors and supervision trees, or that name the
//! results and errors of their primary methods, remain at the root even when
//! they are used less often. The `host` and `observe` modules isolate two
//! coherent secondary surfaces instead of moving every non-prelude type.
//!
//! # Core types
//!
//! | Type | Role |
//! |------|------|
//! | [`OrderedTree`] / [`DynamicTree`] | Single-use, identity-owning supervision declarations; their handles are available before spawn. |
//! | [`Runtime`] | Owns a spawned root and requests graceful shutdown when dropped. |
//! | [`RuntimeHandle`] | Non-owning control and observation surface; [`RuntimeHandle::dynamic`] exposes dynamic membership when supported. |
//! | [`GraphBuilder`] / [`Graph`] | Constructs and validates the actor graph; wiring plus runnable actors. |
//! | [`Actor`] | Handler-style actor definition with a provided receive loop. |
//! | [`host::RawActor`] | Custom-loop typed actor definition (the escape hatch). |
//! | [`ActorRef`] | Cloneable, restart-stable, typed mailbox sender. |
//! | [`host::ActorContext`] | The full context a [`host::RawActor`] run receives: mailbox, watches, cross-actor timers, blocking work, shutdown token. |
//! | [`StartContext`] / [`MessageContext`] / [`StopContext`] | Stage views of that context handed to the [`Actor`] lifecycle hooks. |
//! | [`LiveContext`] | Timers, continuations, and other capabilities shared by the running stages. |
//! | [`MailboxMode`] | FIFO or latest-wins storage policy selected per actor. |
//! | [`Reply`] | One-shot response channel carried inside request messages. |
//! | [`host::RunnableActor`] | One actor plus stable binding — the unit of execution. |
//!
//! # Composition modes
//!
//! - **Ordered actor trees** via [`OrderedTree::new`] or
//!   [`OrderedTree::graph`]: per-actor supervision, recursive actor-aware
//!   subtrees, arbitrary task children, and actor-owned scopes.
//! - **Dynamic actor membership** via [`DynamicTree::new`]: an
//!   initially empty `OneForOne` scope that accepts actors and subtrees at
//!   runtime. Its handle can be captured during wiring before the tree spawns.
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
//! [`host::ActorContext::recv`] is fail-fast during shutdown: it returns `None` as
//! soon as shutdown is requested, even when messages are still queued. That is
//! the primitive, not the default policy: [`Actor`]'s framework-owned loop
//! defaults to [`DrainPolicy::Drain`] and finishes the queued mailbox before
//! stopping. A hand-written [`host::RawActor`] loop opts back in with
//! [`host::ActorContext::try_recv`].
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
//! Actors can watch a peer with [`host::ActorContext::watch`]. The watch follows
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
//! constructed; see the [`Supervision`] derive docs for the generated API, and
//! mind the bounded-mailbox cycle hazard documented on [`GraphBuilder`].
//! Derive names are intentionally not part of [`prelude`]; add
//! `use kokage::{ActorFactory, Supervision};` when using their unqualified
//! names alongside `use kokage::prelude::*`, or qualify the derive as
//! `#[derive(kokage::Supervision)]`.
//!
//! # Hand-driving actors
//!
//! Supervision through [`OrderedTree`] or [`DynamicTree`] is the normal host,
//! but the lower-level execution surface is also public: each
//! [`host::RunnableActor`] can be driven directly with
//! [`run_until`](host::RunnableActor::run_until), which is how tests and hosts
//! with their own supervision story run actors.
//!
//! ```
//! use kokage::{
//!     Actor, ActorResult, CancellationToken, GraphBuilder, MessageContext, Reply, RestartPolicy,
//!     host::DEFAULT_SHUTDOWN_BOUND,
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
//! let counter = builder.actor("Counter", || Counter { total: 0 });
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
//! application-defined accepted-byte totals through [`observe::ActorStats`] and emit
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
//! Tokio ecosystem boundary. [`host::ActorContext::shutdown_token`],
//! [`host::ActorContext::run_blocking`], and the shutdown futures passed to
//! [`host::RunnableActor::run_until`] compose directly with that exact
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
mod runtime;
mod supervision;
mod supervision_derive;

/// Raw actor and task-hosting machinery.
///
/// Most applications use [`Actor`], [`GraphBuilder`], and a supervision tree.
/// This module contains the lower-level execution surface for custom receive
/// loops, directly driven runnable actors, and arbitrary supervised tasks.
/// For task children it re-exports every type needed to name a
/// [`kokage_supervisor::ChildSpec::task`] factory as a standalone function.
///
/// `ChildSpec` is exposed here for task children. Its lower-level
/// `ChildSpec::supervisor` constructor still requires `kokage-supervisor`'s
/// `Supervisor`; compose actor-aware nested scopes with
/// [`OrderedTree::subtree`] or [`DynamicRuntime::add_subtree`] instead.
pub mod host {
    pub use crate::actor::{
        ActorContext, ActorRunError, DEFAULT_SHUTDOWN_BOUND, RawActor, RunnableActor,
    };
    pub use kokage_supervisor::{BoxError, ChildContext, ChildResult, ChildSpec};
}

/// Runtime observation, lifecycle, topology, and completion types.
///
/// Control remains on [`RuntimeHandle`]; this module groups the values and
/// streams returned by that handle without injecting them into the crate root.
pub mod observe {
    pub use crate::{
        actor::{ActorStats, SupervisorPathSegment},
        runtime::LifecycleWatchGuard,
        supervision::{ChildOutline, SupervisionOutline},
    };
    pub use kokage_supervisor::{
        ChildExitView, ChildLifecycleEvent, ChildLifecycleEventKind, ChildLifecycleWatch,
        ChildMembershipView, ChildSnapshot, ChildStateView, CompletionGuard, CompletionOutcome,
        ExitStatusView, LifecycleEvent, LifecycleEventKind, LifecyclePathSegment, LifecycleWatch,
        SupervisorLifecycleEvent, SupervisorSnapshot, SupervisorStateView,
    };
}

/// Implementation bridge used by `kokage` derive expansions.
#[doc(hidden)]
pub mod __private {
    pub use crate::supervision_derive::{
        SupervisionFactories, qualified_label, validate_derived_builder,
    };
}

/// Common imports for `kokage` consumers.
///
/// This prelude is intentionally limited to the actor traits and contexts,
/// primary [`OrderedTree`] composition path, core policies, and common
/// send/call errors. Observation and raw-hosting surfaces live in
/// [`observe`] and [`host`] without being injected by a glob import.
///
/// Derive traits and macros are explicit root imports rather than prelude
/// members. Add `use kokage::{ActorFactory, Supervision};` for unqualified
/// `#[derive(ActorFactory)]` and `#[derive(Supervision)]`, or use their fully
/// qualified `kokage::...` names.
pub mod prelude {
    pub use crate::{
        Actor, ActorOptions, ActorRef, ActorResult, ActorSpec, ActorStatus, CallError, DynamicTree,
        GraphBuilder, LiveContext, MessageContext, OrderedTree, Reply, RestartConfig,
        RestartPolicy, Runtime, RuntimeHandle, SendError, ShutdownPolicy, StartContext,
        StopContext, Strategy, TrySendError,
    };
}

#[cfg(feature = "derive")]
pub use kokage_derive::{ActorFactory, Supervision};

pub use actor::{
    Actor, ActorFactory, ActorOptions, ActorRef, ActorResult, ActorSlot, ActorStatus,
    BlockingCancelled, CallError, CancellationHandle, DownReason, DrainPolicy,
    DynamicRestrictedScope, Graph, GraphBuildError, GraphBuilder, GraphLookupError, LiveContext,
    MailboxMode, MessageContext, MonitorEvent, OffloadDeadline, Reply, RestrictedScope, SendError,
    StartContext, StopContext, TaskHandle, TimerKey, TrySendError,
};
pub use kokage_supervisor::{
    BackoffPolicy, ControlError, RestartConfig, RestartPolicy, ScopeKind, ShutdownPolicy, Strategy,
    SupervisorBuildError, SupervisorError,
};
pub use runtime::{DynamicActorOptions, DynamicRuntime, Runtime, RuntimeHandle};
pub use supervision::{ActorSpec, DynamicTree, OrderedTree, TreeNode};
pub use supervision_derive::{DynamicScope, Supervision};
pub use tokio_util::sync::CancellationToken;
