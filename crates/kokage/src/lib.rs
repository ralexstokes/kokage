#![warn(missing_docs)]

//! The front door to OTP-style supervision trees and typed actors over an
//! async scheduler (Tokio today), with owning [`RunningTree`] and
//! [`RunningDynamicTree`] values plus non-owning scope references.
//!
//! Add each actor's id and factory directly to a [`Tree`], then spawn it.
//! Use [`ActorSpec`] when a declaration needs explicit policy overrides:
//!
//! ```no_run
//! use kokage::prelude::*;
//!
//! struct Echo;
//!
//! impl Actor for Echo {
//!     type Msg = String;
//!
//!     async fn handle(
//!         &mut self,
//!         message: String,
//!         _ctx: &mut Context<'_, Self>,
//!     ) -> ExitResult {
//!         println!("{message}");
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut tree = Tree::new();
//! let echo_ref = tree.add_actor("echo", || Echo);
//! let runtime = tree.spawn()?;
//!
//! echo_ref.send("hello".to_owned()).await?;
//! runtime.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`ActorSlot`] supports cyclic wiring: create every slot and clone its
//! typed ref first, then consume each slot with [`ActorSlot::define`] and
//! configure and place the resulting specs in the desired scopes. Actor ids
//! are local to their containing scope, so sibling scopes may reuse an id.
//!
//! The [`prelude`] re-exports the common composition, actor, and task surface plus
//! snapshot observation. Raw actor execution types live in [`raw`],
//! lifecycle-history types live in [`observe`], and less common actor and
//! supervisor types stay at the crate root.
//!
//! # Core types
//!
//! | Type | Role |
//! |------|------|
//! | [`ActorSpec`] / [`TaskSpec`] | Single-actor and arbitrary async-task declarations. |
//! | [`ActorSlot`] | Typed cyclic actor wiring. |
//! | [`Tree`] / [`DynamicTree`] | Single-use, identity-owning supervision declarations; their scopes are available before spawn. |
//! | [`RunningTree`] / [`RunningDynamicTree`] | Own a spawned ordered or dynamic supervision tree and request graceful shutdown when dropped. |
//! | [`ScopeRef`] / [`DynamicScopeRef`] | Cheaply cloneable, non-owning scope references; only the dynamic form carries membership mutation. |
//! | [`Actor`] | Handler-style actor definition with a provided receive loop. |
//! | [`raw::RawActor`] | Custom-loop typed actor definition (the escape hatch). |
//! | [`ActorRef`] | Cloneable, restart-stable, typed mailbox sender. |
//! | [`TaskRef`] | Cloneable, restart-stable task completion and readiness handle. |
//! | [`Context`] / [`StopContext`] | Live and shutdown actor lifecycle capabilities. |
//! | [`Mailbox`] | FIFO or latest-wins storage policy selected per actor. |
//! | [`Reply`] | One-shot response carried inside request messages; [`Reply::channel`] is the lower-level split-deadline escape hatch. |
//! | [`Guard`] | Cancel-on-drop ownership for explicitly scoped operations; [`Guard::detach`] opts into fire-and-forget. |
//!
//! # Composition modes
//!
//! - **Ordered actor trees** via [`Tree::new`]: per-actor supervision,
//!   recursive actor-aware subtrees, arbitrary task children, and explicit
//!   leader-owned scopes.
//! - **Dynamic actor membership** via [`DynamicTree::new`]: an initially empty
//!   `OneForOne` scope that accepts actor specs and subtrees at runtime. Its
//!   scope is available before spawn for typed wiring.
//!
//! Fate sharing is selected with [`Strategy::OneForAll`] or tree shape; actor
//! wiring does not choose execution topology.
//!
//! # Delivery contract: at-most-once
//!
//! Mailboxes are incarnation-owned: each actor run binds a fresh mailbox, and
//! messages accepted by a dead incarnation are lost with it. Delivery is
//! therefore **at-most-once**, with loss windows at restart and shutdown.
//! Stronger guarantees are user protocols built on [`ActorRef::call`] and
//! [`Reply`]. [`ActorRef::send`] rides through restart windows when a
//! rebind is expected, and a terminal [`SendError`] returns the unaccepted
//! message. [`ActorRef::try_send`] returns every fail-fast rejection with its
//! message; [`ActorRef::send_timeout`] does the same after a bounded capacity
//! or restart wait. Applying [`tokio::time::timeout`] to `send` is lossy
//! because cancelling that future drops its message.
//!
//! [`raw::RawContext::recv`] returns `None` as soon as shutdown is
//! requested. [`Actor`]'s framework-owned loop defaults to
//! [`Shutdown::graceful_for`] and finishes queued messages before stopping; a
//! hand-written [`raw::RawActor`] loop can inspect remaining work with
//! [`raw::RawContext::try_recv`].
//!
//! Restarts also lose queued messages: the new incarnation binds a fresh
//! mailbox, so messages accepted behind a poison message are dropped with the
//! failed run. Preserving that queue would redeliver the poison message and
//! turn one failure into a restart loop. [`ActorRef::send`] can wait through an
//! unbound restart window, but it cannot recover a message already accepted by
//! the failed incarnation.
//!
//! # Observing lifecycle
//!
//! Peer monitors and supervisor lifecycle streams are two projections of one
//! lifecycle model: a membership is added, its incarnations start and exit,
//! and the membership is eventually removed. [`MonitorEvent`] projects the
//! actor-relative transitions in a [`MonitorEventKind`] delivered through a
//! typed peer's ordinary mailbox. [`observe::LifecycleEventKind`] projects the
//! same transitions through one `Child` envelope and an
//! [`observe::ChildEventKind`] in an operations-oriented recursive tree stream,
//! alongside membership, restart, and supervisor transitions. The mechanisms
//! remain separate so each keeps the identity,
//! ordering, and delivery contract appropriate to its audience.
//!
//! [`raw::RawContext::watch`] follows logical membership across restarts;
//! `Lagged` reports sustained observer overload. Watches survive restarts of
//! both actors, and permanent removal of either membership ends the watch.
//! Ordinary watches are membership-owned; ordinary offloads are owned by the
//! current actor incarnation. Their explicitly scoped variants, mailbox timers,
//! and lifecycle/completion pumps return a [`Guard`]. Dropping it cancels the
//! operation; retain it or call [`Guard::detach`] to keep the scoped work alive.
//!
//! # Static declarations
//!
//! Enable the opt-in `derive` feature and use `#[derive(ActorFactory)]` on
//! named-field actors to generate reusable factory structs without repeating
//! configuration fields or clone code.
//! Derive macros are intentionally not part of [`prelude`]; import
//! [`ActorFactory`] from the crate root or use
//! `#[derive(kokage::ActorFactory)]`.
//!
//! # Cyclic wiring
//!
//! Slots let factories refer to actors declared later without string lookup:
//!
//! ```
//! use kokage::{ActorSlot, prelude::*};
//! # struct Left(kokage::ActorRef<()>);
//! # struct Right(kokage::ActorRef<()>);
//! # impl kokage::Actor for Left { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
//! # impl kokage::Actor for Right { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
//! let left_slot = ActorSlot::<()>::new("left");
//! let left = left_slot.actor_ref();
//! let right_slot = ActorSlot::<()>::new("right");
//! let right = right_slot.actor_ref();
//!
//! let left_actor = left_slot.define({ let right = right.clone(); move || Left(right.clone()) });
//! let right_actor = right_slot.define({ let left = left.clone(); move || Right(left.clone()) });
//! let mut tree = Tree::new();
//! tree.add_actor_spec(left_actor);
//! tree.add_actor_spec(right_actor);
//! # let _ = (left, right, tree);
//! ```
//!
//! # Observability
//!
//! `tracing` spans and structured logs are emitted automatically for
//! supervisor, actor, and mailbox lifecycle. Message counters and live mailbox
//! usage are available through [`ActorRef::stats`] and
//! [`ScopeRef::actor_stats`]. Actors configured with
//! [`ActorSpec::message_size`] also expose accepted-byte totals.
//!
//! # Runtime-independent boundaries
//!
//! Public mailbox errors, cancellation tokens, and snapshot receivers are
//! crate-owned. Applications can build cancellation trees with
//! [`CancellationToken`] and expose
//! [`observe::SupervisorSnapshotReceiver`] without leaking the scheduler's
//! channel or cancellation implementation into their own APIs.
//!
//! # Examples
//!
//! - `examples/supervised_actors.rs` — per-actor supervision.
//! - `examples/supervision.rs` — cyclic typed wiring with actor slots.
//! - `examples/drain_policy.rs` — draining queued messages during shutdown.
//! - `examples/individual_actor_policies.rs` — per-actor policy overrides.
//! - `examples/dynamic_actors.rs` — adding and removing actors at runtime.
//! - `examples/directory.rs` — a typed, userland name directory.
//! - `examples/ref_rebind.rs` — refs riding through supervised restarts.
//! - `examples/graph_failures.rs` — supervisor policy around actor failures.
//! - `examples/mailbox_backpressure.rs` and `examples/send_vs_try_send.rs` —
//!   bounded mailboxes and send flavors.
//! - `examples/builder_validation.rs` — tree validation at spawn.
//! - `examples/blocking_work.rs` and `examples/blocking_lifecycle.rs` —
//!   cooperative and detached blocking work.
//! - `examples/actor_metrics.rs` and `examples/actor_tracing.rs` —
//!   observability patterns.
//! - `examples/json_edge.rs` — decoding byte-oriented JSON into typed messages.
//! - `examples/task_one_for_one_restart.rs` and
//!   `examples/task_one_for_all_pipeline.rs` — task-supervision strategies.
//! - `examples/task_dynamic_children.rs` and
//!   `examples/task_nested_supervisor.rs` — dynamic and nested task scopes.
//! - `examples/task_metrics.rs` and `examples/task_tracing.rs` —
//!   task-supervisor observability.
//!
//! # Cargo features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | no | Re-exports `#[derive(ActorFactory)]`. |
//! | `host` | no | Direct single-actor hosting through `ActorSpec::into_host` and `raw::ActorHost`. |
//! | `metrics` | no | Supervisor lifecycle metrics plus opt-in actor message-size metrics. |
//! | `serde` | no | Serialization support for outlines, actor stats, and view types. |

mod actor;
mod runtime;
mod supervision;
mod supervisor;

/// Raw actor hosting machinery.
///
/// Most applications use [`ActorSpec`] and a supervision tree.
/// This module contains the lower-level execution surface for custom receive
/// loops and, with the `host` feature, directly driven actor hosts. Handler
/// actors, supervised task declarations, and their shared [`ExitResult`] live
/// at the crate root.
pub mod raw {
    #[cfg(feature = "host")]
    pub use crate::actor::{ActorHost, ActorRunError, DEFAULT_SHUTDOWN_BOUND, IncarnationExit};
    pub use crate::actor::{RawActor, RawContext};
}

/// Runtime observation, lifecycle, and topology types.
///
/// Scope handles remain at the crate root; this module groups the observation
/// values and lower-level streams they return.
pub mod observe {
    #[cfg(feature = "serde")]
    pub use crate::supervision::{ChildOutline, SupervisionOutline};
    pub use crate::{
        actor::{ActorStats, ScopedActorStats},
        runtime::{ScopeChange, ScopeChanges},
        supervisor::{
            ChildEvent, ChildEventKind, ChildMembershipView, ChildSnapshot, ChildStateView,
            ExitStatus, LifecycleEvent, LifecycleEventKind, LifecycleObservation, LifecycleWatch,
            ScopeKind, ScopePathSegment, SnapshotRecvError, SupervisorSnapshot,
            SupervisorSnapshotReceiver, SupervisorStateView,
        },
    };
}

/// Common imports for `kokage` consumers.
///
/// This prelude covers the actor traits and contexts, static and dynamic tree
/// composition, child declarations, common supervision policies and errors,
/// actor-owned operations, and the snapshot pair used by application health
/// and readiness code. Cyclic-wiring declarations, specialized errors,
/// lifecycle-history types, and raw actor hosting remain at the crate root or
/// in [`observe`] and [`raw`].
///
/// With the `derive` feature enabled, derive macros are explicit root imports
/// rather than prelude members. Add `use kokage::ActorFactory;` for an
/// unqualified `#[derive(ActorFactory)]`, or use its fully qualified
/// `kokage::ActorFactory` name.
pub mod prelude {
    pub use crate::{
        Actor, ActorRef, ActorSpec, Backoff, CallError, Context, ControlError, DynamicScopeRef,
        DynamicTree, ExitResult, Guard, Mailbox, MailboxShutdown, MonitorEvent, MonitorEventKind,
        OneShotTaskSpec, Reply, RestartPolicy, RestartSettings, RunningDynamicTree, RunningTree,
        ScopeChange, ScopeRef, SendError, SendErrorKind, Shutdown, StopContext, Strategy,
        TaskContext, TaskRef, TaskSpec, TimerKey, Tree,
        observe::{SupervisorSnapshot, SupervisorSnapshotReceiver},
    };
}

#[cfg(feature = "derive")]
pub use kokage_derive::ActorFactory;

pub use actor::{
    Actor, ActorFactory, ActorRef, ActorSlot, ActorSpec, BlockingCancelled, CallError, Context,
    ExitResult, Mailbox, MonitorEvent, MonitorEventKind, OffloadDeadline, Reply, ReplyError,
    ReplyReceiver, SendError, SendErrorKind, StopContext, TimerKey,
};
pub use runtime::{
    DynamicScopeRef, RunningDynamicTree, RunningTree, ScopeChange, ScopeChanges, ScopeRef,
    TaskError, TaskRef,
};
pub use supervision::{DynamicTree, SubtreeSpec, Tree};
pub use supervisor::{
    Backoff, BoxError, BuildError, CancellationToken, ControlError, ExitStatus, Guard,
    MailboxShutdown, OneShotTaskSpec, RestartPolicy, RestartSettings, Shutdown, Strategy,
    SupervisorError, TaskContext, TaskSpec,
};
