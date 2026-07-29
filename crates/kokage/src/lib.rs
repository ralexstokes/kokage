#![warn(missing_docs)]

//! The front door to OTP-style supervision trees and typed actors over an
//! async scheduler (Tokio today), with an owning [`Runtime`] and integrated
//! non-owning [`RuntimeHandle`] values.
//!
//! Declare each actor with [`ActorSpec`], place the specs directly in an
//! [`OrderedTree`], and spawn the tree:
//!
//! ```no_run
//! use kokage::{ActorSpec, prelude::*};
//!
//! struct Echo;
//!
//! impl Actor for Echo {
//!     type Msg = String;
//!
//!     async fn handle(
//!         &mut self,
//!         message: String,
//!         _ctx: &mut MessageContext<'_, Self>,
//!     ) -> ActorResult {
//!         println!("{message}");
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let echo = ActorSpec::new("echo", || Echo);
//! let echo_ref = echo.actor_ref();
//! let runtime = OrderedTree::new().actor(echo).spawn()?;
//!
//! echo_ref.send("hello".to_owned()).await?;
//! runtime.shutdown_and_wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`ActorSlot`] supports cyclic wiring: create every slot and clone its
//! typed ref first, then consume each slot with [`ActorSlot::define`] and
//! place the resulting specs in the desired scopes. Actor ids are local to
//! their containing scope, so sibling scopes may reuse an id.
//!
//! The [`prelude`] re-exports the day-one composition and actor surface plus
//! snapshot observation. Host-facing execution types live in [`host`],
//! lifecycle-history types live in [`observe`], and advanced actor and
//! supervisor configuration stays at the crate root.
//!
//! # Core types
//!
//! | Type | Role |
//! |------|------|
//! | [`ActorSpec`] / [`ActorSlot`] | Single-actor declarations and typed cyclic wiring. |
//! | [`OrderedTree`] / [`DynamicTree`] | Single-use, identity-owning supervision declarations; their handles are available before spawn. |
//! | [`Runtime`] | Owns a spawned root and requests graceful shutdown when dropped. |
//! | [`RuntimeHandle`] | Non-owning control and observation surface; [`RuntimeHandle::dynamic`] exposes dynamic membership when supported. |
//! | [`Actor`] | Handler-style actor definition with a provided receive loop. |
//! | [`host::RawActor`] | Custom-loop typed actor definition (the escape hatch). |
//! | [`ActorRef`] | Cloneable, restart-stable, typed mailbox sender. |
//! | [`StartContext`] / [`MessageContext`] / [`StopContext`] | Stage-specific actor lifecycle capabilities. |
//! | [`MailboxMode`] | FIFO or latest-wins storage policy selected per actor. |
//! | [`Reply`] | One-shot response channel carried inside request messages. |
//! | [`host::RunnableActor`] | One actor plus stable binding — the unit of direct execution. |
//!
//! # Delivery contract: at-most-once
//!
//! Mailboxes are incarnation-owned: each actor run binds a fresh mailbox, and
//! messages accepted by a dead incarnation are lost with it. Delivery is
//! therefore **at-most-once**, with loss windows at restart and shutdown.
//! Stronger guarantees are user protocols built on [`ActorRef::call`] and
//! [`Reply`]. [`ActorRef::send`] rides through restart windows when a
//! rebind is expected.
//!
//! [`host::ActorContext::recv`] returns `None` as soon as shutdown is
//! requested. [`Actor`]'s framework-owned loop defaults to
//! [`DrainPolicy::Drain`] and finishes queued messages before stopping; a
//! hand-written [`host::RawActor`] loop can inspect remaining work with
//! [`host::ActorContext::try_recv`].
//!
//! Actors can watch a peer with [`host::ActorContext::watch`]. Watches follow
//! logical actor membership across restarts and deliver [`MonitorEvent`]s
//! through the observer's ordinary mailbox.
//!
//! # Static declarations
//!
//! Use `#[derive(ActorFactory)]` on named-field actors to generate reusable
//! factory structs without repeating configuration fields or clone code.
//! Derive macros are intentionally not part of [`prelude`]; import
//! [`ActorFactory`] from the crate root or use
//! `#[derive(kokage::ActorFactory)]`.
//!
//! # Cyclic wiring
//!
//! Slots let factories refer to actors declared later without string lookup:
//!
//! ```
//! use kokage::{ActorSlot, ActorSpec, OrderedTree};
//! # struct Left(kokage::ActorRef<()>);
//! # struct Right(kokage::ActorRef<()>);
//! # impl kokage::Actor for Left { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
//! # impl kokage::Actor for Right { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
//! let left_slot = ActorSlot::<()>::new("left");
//! let left = left_slot.actor_ref();
//! let right_slot = ActorSlot::<()>::new("right");
//! let right = right_slot.actor_ref();
//!
//! let left_actor = left_slot.define({ let right = right.clone(); move || Left(right.clone()) });
//! let right_actor = right_slot.define({ let left = left.clone(); move || Right(left.clone()) });
//! let tree = OrderedTree::new().actor(left_actor).actor(right_actor);
//! # let _ = (left, right, tree);
//! ```
//!
//! # Hand-driving actors
//!
//! Supervision through [`OrderedTree`] or [`DynamicTree`] is the normal
//! host, but [`ActorSpec::into_runnable`] exposes one actor for direct hosts:
//!
//! ```
//! use kokage::{
//!     Actor, ActorResult, ActorSpec, CancellationToken, MessageContext,
//!     RestartPolicy, host::DEFAULT_SHUTDOWN_BOUND,
//! };
//! # struct Worker;
//! # impl Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult { Ok(()) } }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let actor = ActorSpec::new("worker", || Worker).into_runnable();
//! let stop = CancellationToken::new();
//! let run = tokio::spawn({
//!     let stop = stop.clone();
//!     async move {
//!         actor
//!             .run_until(stop.cancelled(), RestartPolicy::Never, DEFAULT_SHUTDOWN_BOUND)
//!             .await
//!     }
//! });
//! stop.cancel();
//! run.await??;
//! # Ok(())
//! # }
//! ```
//!
//! # Observability
//!
//! `tracing` spans and structured logs are emitted automatically for
//! supervisor, actor, and mailbox lifecycle. Message counters and live mailbox
//! usage are available through [`ActorRef::stats`] and
//! [`RuntimeHandle::actor_stats`]. Actors configured with
//! [`ActorSpec::message_size`] also expose accepted-byte totals.
//!
//! # Examples
//!
//! - `examples/supervised_actors.rs` — per-actor supervision.
//! - `examples/supervision.rs` — cyclic typed wiring with actor slots.
//! - `examples/dynamic_actors.rs` — adding and removing actors at runtime.
//! - `examples/ref_rebind.rs` — refs riding through supervised restarts.
//! - `examples/builder_validation.rs` — tree validation at spawn.
//! - `examples/actor_metrics.rs` and `examples/actor_tracing.rs` —
//!   observability patterns.
//!
//! # Cargo features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | yes | Re-exports `#[derive(ActorFactory)]`. |
//! | `metrics` | no | Supervisor lifecycle metrics plus opt-in actor message-size metrics. |
//! | `serde` | no | Serialization support for outlines, actor stats, and view types. |

mod actor;
mod runtime;
mod supervision;

/// Raw actor and task-hosting machinery.
///
/// Most applications use [`ActorSpec`] and a supervision tree.
/// This module contains the lower-level execution surface for custom receive
/// loops, directly driven runnable actors, and arbitrary supervised tasks.
/// For task children it re-exports every type needed to name a
/// [`kokage_supervisor::ChildSpec::task`] factory as a standalone function.
///
/// `ChildSpec` is exposed here for task children. Its lower-level
/// `ChildSpec::supervisor` constructor still requires `kokage-supervisor`'s
/// `Supervisor`; compose actor-aware nested scopes with
/// [`OrderedTree::subtree`] or [`DynamicRuntimeHandle::add_subtree`] instead.
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
        ChildExitView, ChildMembershipView, ChildSnapshot, ChildStateView, CompletionError,
        CompletionGuard, CompletionOutcome, LifecycleEvent, LifecycleEventKind,
        LifecyclePathSegment, LifecycleWatch, SnapshotRecvError, SupervisorSnapshot,
        SupervisorSnapshotReceiver, SupervisorStateView,
    };
}

/// Common imports for `kokage` consumers.
///
/// This prelude is intentionally limited to the actor traits and contexts,
/// the primary [`OrderedTree`] composition path, its [`ActorSpec`] declaration,
/// and the snapshot pair used by
/// application health and readiness code. Advanced configuration, error,
/// dynamic-membership, lifecycle-history, and raw-hosting types remain at the
/// crate root or in [`observe`] and [`host`].
///
/// Derive macros are explicit root imports rather than prelude members. Add
/// `use kokage::ActorFactory;` for an unqualified `#[derive(ActorFactory)]`,
/// or use its fully qualified `kokage::ActorFactory` name.
pub mod prelude {
    pub use crate::{
        Actor, ActorRef, ActorResult, ActorSpec, LiveContext, MessageContext, OrderedTree, Reply,
        StartContext, StopContext,
        observe::{SupervisorSnapshot, SupervisorSnapshotReceiver},
    };
}

#[cfg(feature = "derive")]
pub use kokage_derive::ActorFactory;

pub use actor::{
    Actor, ActorFactory, ActorRef, ActorResult, ActorSlot, ActorSpec, ActorStatus,
    BlockingCancelled, CallError, CancellationHandle, DownReason, DrainPolicy,
    DynamicRestrictedScope, LiveContext, MailboxMode, MessageContext, MonitorEvent,
    OffloadDeadline, Reply, RestrictedScope, SendError, StartContext, StopContext, TaskHandle,
    TimerKey, TrySendError,
};
pub use kokage_supervisor::{
    BackoffPolicy, CancellationToken, ControlError, RestartConfig, RestartPolicy, ScopeKind,
    ShutdownPolicy, Strategy, SupervisorBuildError, SupervisorError, TerminalMembership,
};
pub use runtime::{DynamicRuntime, DynamicRuntimeHandle, Runtime, RuntimeHandle};
pub use supervision::{DynamicTree, OrderedTree, TreeNode};
