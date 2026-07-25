#![warn(missing_docs)]

//! Structured task supervision for Tokio, inspired by Erlang/OTP.
//!
//! `tokio-supervisor` manages the lifecycle of a group of async tasks
//! (*children*), automatically restarting them according to configurable
//! policies when they fail, panic, or are aborted. Supervisors can be nested
//! to form supervision trees with independent restart scopes.
//!
//! # Core concepts
//!
//! | Type | Role |
//! |------|------|
//! | [`SupervisorBuilder`] | Constructs and validates a supervisor. |
//! | [`Supervisor`] | A configured supervisor, ready to [`spawn`](Supervisor::spawn). |
//! | [`SupervisorHandle`] | Control and observe a running supervisor. |
//! | [`ChildSpec`] | Pairs an async factory with restart/shutdown policies. |
//! | [`ChildContext`] | Per-spawn context given to each child (id, generation, cancellation token). |
//!
//! # Strategies
//!
//! [`Strategy`] controls what happens when a child exits unexpectedly:
//!
//! - **[`OneForOne`](Strategy::OneForOne)** — only the failed child is
//!   restarted. Siblings are unaffected. This is the default.
//! - **[`OneForAll`](Strategy::OneForAll)** — all children are stopped and
//!   restarted together. [`Never`](RestartPolicy::Never) children are still
//!   drained with the group but are not respawned. Use this when children have
//!   hard interdependencies.
//! - **[`RestForOne`](Strategy::RestForOne)** — the failed child and children
//!   declared after it are stopped and restarted; earlier children remain
//!   running. Use this for ordered pipelines.
//!
//! # Restart policies
//!
//! Each child has a [`RestartPolicy`]:
//!
//! - **[`Always`](RestartPolicy::Always)** — always restarted, regardless of
//!   exit status.
//! - **[`OnFailure`](RestartPolicy::OnFailure)** (default) — restarted only on
//!   failure (`Err`, panic, or abort). A clean `Ok(())` exit is final.
//! - **[`Never`](RestartPolicy::Never)** — never restarted. Runs at most
//!   once.
//!
//! Restarts are bounded by a [`RestartIntensity`] limit (default: 5 restarts
//! within 30 seconds). When exceeded, the supervisor exits with
//! [`SupervisorError::RestartIntensityExceeded`]. An optional [`BackoffPolicy`]
//! inserts a delay before each restart attempt (fixed, exponential, or
//! jittered exponential). A shutdown request always wins over a pending
//! restart delay, including zero-delay restarts.
//!
//! # Shutdown
//!
//! Each child has a [`ShutdownPolicy`] that controls how it is stopped:
//!
//! - **[`CooperativeStrict`](ShutdownMode::CooperativeStrict)** — cancel the
//!   child's token and wait up to the grace period. If the child does not
//!   exit, a timeout error is reported after aborting the Tokio task.
//! - **[`CooperativeThenAbort`](ShutdownMode::CooperativeThenAbort)** (default,
//!   5 s grace) — cooperative with a fallback Tokio abort.
//! - **[`Abort`](ShutdownMode::Abort)** — abort the Tokio task immediately.
//!
//! When the supervisor is draining multiple cooperative children at once
//! (during shutdown or a [`OneForAll`](Strategy::OneForAll) restart), it uses a
//! shared deadline equal to the maximum grace period among the active
//! cooperative children. Group and full-shutdown drains are atomic critical
//! sections, so control commands wait behind them until the old generation is
//! gone; ordinary removals, restart backoffs, and readiness gates remain
//! responsive.
//!
//! All shutdown modes are cooperative at Tokio poll boundaries. A non-yielding
//! future is never forcibly preempted. If you need hard-stop guarantees for
//! blocking work, isolate it in a dedicated blocking pool or external process
//! and supervise the boundary.
//!
//! # Dynamic children
//!
//! Children can be added and removed at runtime through the
//! [`SupervisorHandle`]:
//!
//! - [`add_child`](SupervisorHandle::add_child) /
//!   [`remove_child`](SupervisorHandle::remove_child) target that handle's
//!   supervisor.
//! - [`add_supervisor`](SupervisorHandle::add_supervisor) adds a first-class
//!   nested supervisor; [`supervisor`](SupervisorHandle::supervisor) returns
//!   its restart-stable handle.
//!
//! Control operations wait when the control channel is full. Successful adds
//! resolve once membership is inserted and startup is scheduled, which can be
//! before the child spawns under sequential startup; use
//! [`SupervisorHandle::wait_started`] for readiness. Active removals resolve
//! only after detachment, without blocking distinct-id control operations.
//!
//! Supervisors may start empty or have their last child removed. They idle at
//! zero children and continue accepting control commands until shutdown.
//!
//! # Nested supervisors
//!
//! A [`Supervisor`] is added as a first-class child with
//! [`SupervisorBuilder::supervisor`] or
//! [`SupervisorHandle::add_supervisor`]. The nested supervisor:
//!
//! - Forwards lifecycle events to the parent as
//!   [`SupervisorEvent::Nested`] wrappers.
//! - Publishes its snapshot into the parent's
//!   [`ChildSnapshot::supervisor`] field.
//! - Has a restart-stable direct handle whose subscriptions and snapshots
//!   survive nested restarts.
//! - Is restarted by the parent according to its [`SupervisorSpec`] policies.
//!
//! # Observability
//!
//! The crate provides two supervisor observation primitives plus diagnostic
//! projections:
//!
//! - **[`SupervisorSnapshot`] state** — current state and cumulative counters,
//!   read directly or through [`SupervisorHandle::subscribe_snapshots`].
//! - **[`LifecycleEvent`] streams** — ordered, reliable direct-child
//!   transitions from [`SupervisorHandle::watch_lifecycle`].
//! - **[`SupervisorEvent`] subscriptions** — a legacy, lossy broadcast for
//!   logging and dashboards via [`SupervisorHandle::subscribe`].
//! - **`tracing` spans and logs** — automatic structured output for every
//!   lifecycle event. The supervisor runs inside an `info_span!("supervisor")`
//!   and each child inside an `info_span!("child")`, both carrying
//!   `supervisor_name`, `supervisor_path`, `child_id`, and `generation` fields.
//! - **`metrics` counters, gauges, and histograms** (requires the **`metrics`**
//!   feature) — lowest-cardinality view, best for dashboards and alerting.
//!   Emits `supervisor.children.running`, `supervisor.children.started`,
//!   `supervisor.children.exited`, `supervisor.restarts`,
//!   `supervisor.restart_intensity_exceeded`, `supervisor.events.dropped`,
//!   `supervisor.shutdown_timeouts`, and `supervisor.child_shutdown.duration`.
//!
//! ## Snapshot/lifecycle alignment
//!
//! Create a lifecycle watch first, then read a snapshot, then discard watched
//! events with `seq <= snapshot.lifecycle_seq`. This yields a gap-free
//! state-plus-stream view without replay. Lifecycle overflow is explicit as
//! [`LifecycleEventKind::Lagged`]; the legacy event broadcast can lose nested
//! forwarded events without an equivalent end-to-end marker.
//!
//! ## Nested event forwarding
//!
//! Forwarding of nested supervisor events to the parent is best-effort. If a
//! nested supervisor's event receiver lags behind, the runtime logs a warning
//! and increments the `supervisor.events.dropped` counter (when the `metrics`
//! feature is enabled).
//!
//! # Deliberate dependency coupling
//!
//! [`ChildContext::shutdown_token`] returns the exact
//! [`tokio_util::sync::CancellationToken`] used internally. This is a
//! deliberate public boundary: child futures can join the supervisor's token
//! into application cancellation trees and pass it to Tokio ecosystem APIs
//! without a crate-specific wrapper or adapter. Other implementation details,
//! including supervisor control channels and their errors, remain crate-owned.
//!
//! # Quick start
//!
//! ```no_run
//! use tokio_supervisor::{ChildSpec, SupervisorBuilder};
//! use tracing_subscriber::FmtSubscriber;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let subscriber = FmtSubscriber::builder().finish();
//! tracing::subscriber::set_global_default(subscriber)?;
//!
//! let supervisor = SupervisorBuilder::new()
//!     .child(ChildSpec::new("worker", |ctx| async move {
//!         ctx.shutdown_token().cancelled().await;
//!         Ok(())
//!     }))
//!     .build()?;
//!
//! let handle = supervisor.spawn();
//! let _lifecycle = handle.watch_lifecycle();
//! let _snapshot = handle.snapshot();
//! # handle.shutdown();
//! # handle.wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Cargo features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `metrics` | no | Enables `metrics` crate integration for counters, gauges, and histograms. |
//!
//! # Examples
//!
//! - `examples/one_for_one_restart.rs` — basic restart behaviour.
//! - `examples/one_for_all_pipeline.rs` — interdependent children with
//!   `OneForAll`.
//! - `examples/nested_supervisor.rs` — supervision trees.
//! - `examples/dynamic_children.rs` — adding and removing children at runtime.
//! - `examples/per_child_restart_intensity.rs` — per-child intensity overrides.
//! - `examples/shutdown_with_cancellation_token.rs` — graceful shutdown driven
//!   by a signal.
//! - `examples/subscribe_to_events.rs` — reacting to lifecycle events.
//! - `examples/subscribe_to_snapshots.rs` — polling supervisor state.
//! - `examples/tracing.rs` — structured logging output.
//! - `examples/metrics.rs` — Prometheus metrics (requires `--features metrics`).

mod attachment;
mod builder;
mod child;
mod context;
mod error;
mod event;
mod handle;
mod lifecycle;
mod monitor;
mod observability;
pub mod prelude;
mod restart;
mod runtime;
mod shutdown;
mod snapshot;
mod strategy;
mod supervisor;

pub use attachment::{AttachedChild, AttachedChildIdentity};
pub use builder::{StartMode, SupervisorBuilder};
pub use child::{BoxError, ChildResult, ChildSpec, SupervisorSpec};
pub use context::{ChildContext, SupervisorToken};
pub use error::{ControlError, SupervisorBuildError, SupervisorError};
pub use event::{EventPathSegment, ExitStatusView, SupervisorEvent};
pub use handle::SupervisorHandle;
pub use lifecycle::{LifecycleEvent, LifecycleEventKind, LifecycleWatch};
#[allow(deprecated)]
pub use monitor::RestartWatch;
pub use restart::{BackoffPolicy, RestartIntensity, RestartPolicy};
pub use shutdown::{AutoShutdown, ShutdownMode, ShutdownPolicy};
pub use snapshot::{
    ChildMembershipView, ChildSnapshot, ChildStateView, SupervisorSnapshot, SupervisorStateView,
};
pub use strategy::Strategy;
pub use supervisor::Supervisor;
