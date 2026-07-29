#![warn(missing_docs)]

//! Structured task supervision over an async scheduler, inspired by
//! Erlang/OTP. Runtime services enter through [`Scheduler`]; the
//! `kokage-tokio` crate provides the standard Tokio binding.
//!
//! `kokage-supervisor` manages the lifecycle of a group of async tasks
//! (*children*), automatically restarting them according to configurable
//! policies when they fail, panic, or are aborted. Supervisors can be nested
//! to form supervision trees with independent restart scopes.
//!
//! # Core concepts
//!
//! | Type | Role |
//! |------|------|
//! | [`Supervisor`] | Constructs ordered or dynamic supervisors and spawns built ones. |
//! | [`SupervisorHandle`] | Control and observe a running supervisor. |
//! | [`ChildSpec`] | Declares a task or nested supervisor with restart/shutdown policies. |
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
//! Restarts are bounded by a [`RestartConfig`] limit (default: 5 restarts
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
//! - **[`Cooperative`](ShutdownPolicy::Cooperative)** (default, 5 s grace) —
//!   cancel the child's token and wait up to its grace period, then abort and
//!   report a timeout for shutdown or removal if it does not exit. During a
//!   group restart, the old generation is escalated to abort and the restart
//!   proceeds once that task exits.
//! - **[`Abort`](ShutdownPolicy::Abort)** — abort the scheduler task immediately.
//!
//! Ordered scopes drain in reverse declaration order, giving each cooperative
//! child its own grace period before moving to the previous child. Once an
//! abort is issued, the cursor advances promptly even if a non-yielding future
//! has not reached a poll boundary. Dynamic scopes drain all active children
//! concurrently under one shared deadline equal to the maximum configured
//! grace. Ordered group and full-shutdown drains are atomic critical sections,
//! so observation never sees a later generation overlap an earlier one.
//!
//! Task aborts take effect at scheduler cancellation boundaries. A non-yielding
//! future is never forcibly preempted. If you need hard-stop guarantees for
//! blocking work, isolate it in a dedicated blocking pool or external process
//! and supervise the boundary.
//!
//! # Scope kinds
//!
//! [`Supervisor::ordered`] creates an ordered builder: a declared sequence with
//! readiness-gated startup, reverse sequential teardown, and immutable runtime
//! membership. [`Supervisor::dynamic`] creates a dynamic builder for an empty scope:
//! membership is written at runtime, startup is immediate, teardown is
//! concurrent, and the strategy is always [`OneForOne`](Strategy::OneForOne).
//!
//! Dynamic membership is controlled through the [`SupervisorHandle`]:
//!
//! - [`add_child`](SupervisorHandle::add_child) /
//!   [`remove_child`](SupervisorHandle::remove_child) target that handle's
//!   supervisor.
//! - [`ChildSpec::supervisor`] declares a first-class nested supervisor through
//!   that same `add_child` method; [`supervisor`](SupervisorHandle::supervisor)
//!   returns its restart-stable handle.
//!
//! Successful dynamic adds resolve once membership is inserted and immediate
//! startup is scheduled; use [`SupervisorHandle::wait_started`] for readiness.
//! Active removals resolve only after detachment, without blocking distinct-id
//! control operations. Ordered handles reject every add/remove operation with
//! [`ControlError::UnsupportedByScopeKind`].
//!
//! Dynamic supervisors may start empty or have their last child removed. They
//! idle at zero children and continue accepting control commands until
//! shutdown. Empty ordered supervisors remain empty.
//!
//! # Nested supervisors
//!
//! A [`Supervisor`] is wrapped with [`ChildSpec::supervisor`] and added through
//! an ordered builder's `child` method or [`SupervisorHandle::add_child`]. The
//! nested supervisor:
//!
//! - Appears in ancestor
//!   [`watch_lifecycle_recursive`](SupervisorHandle::watch_lifecycle_recursive)
//!   streams with a path that identifies each exact supervisor incarnation.
//! - Publishes its snapshot into the parent's
//!   [`ChildSnapshot::supervisor`] field.
//! - Has a restart-stable direct handle whose subscriptions and snapshots
//!   survive nested restarts.
//! - Is restarted by the parent according to its [`ChildSpec`] policies.
//! - Is recursively hard-aborted when its wrapper uses [`ShutdownPolicy::Abort`]
//!   or when a terminal, non-revivable ancestor fails. A parent-restartable
//!   failed incarnation lets nested runtimes finish cooperatively before their
//!   stable identities rebind; a normal cooperative stop likewise applies the
//!   nested scope's own child policies.
//!
//! # Observability
//!
//! The crate provides two supervisor observation primitives plus diagnostic
//! projections:
//!
//! - **[`SupervisorSnapshot`] state** — current state and cumulative counters,
//!   read directly or through [`SupervisorHandle::subscribe_snapshots`].
//! - **Lifecycle streams** — ordered direct-child
//!   [`ChildLifecycleEvent`]s from
//!   [`SupervisorHandle::watch_lifecycle`] (including scheduled restarts), or
//!   the whole tree (adding supervisor transitions and restart-intensity
//!   failures) from [`SupervisorHandle::watch_lifecycle_recursive`]. Both
//!   report overflow explicitly rather than losing events silently.
//! - **`tracing` spans and logs** — automatic structured output for every
//!   lifecycle event. The supervisor runs inside an `info_span!("supervisor")`
//!   and each child inside an `info_span!("child")`, both carrying
//!   `supervisor_name`, `supervisor_path`, `child_id`, and `generation` fields.
//! - **`metrics` counters, gauges, and histograms** (requires the **`metrics`**
//!   feature) — lowest-cardinality view, best for dashboards and alerting.
//!   Emits `supervisor.children.running`, `supervisor.children.started`,
//!   `supervisor.children.exited`, `supervisor.restarts`,
//!   `supervisor.restart_intensity_exceeded`, `supervisor.shutdown_timeouts`,
//!   and `supervisor.child_shutdown.duration`.
//!
//! ## Snapshot/lifecycle alignment
//!
//! Create a lifecycle watch first, then read a snapshot, then discard watched
//! child transition events with `seq <= snapshot.lifecycle_seq`. This yields a gap-free
//! state-plus-stream view without replay. Direct lifecycle overflow is
//! explicit as [`ChildLifecycleEventKind::Lagged`]; recursive stream overflow
//! uses [`LifecycleEventKind::Lagged`] as a tree-wide marker.
//!
//! # Runtime-independent boundaries
//!
//! Child shutdown uses the crate-owned [`CancellationToken`], and snapshot
//! subscriptions return [`SupervisorSnapshotReceiver`]. These façade types
//! preserve cancellation-tree and conflating-watch semantics without exposing
//! the scheduler's channel or cancellation implementation in public APIs.
//!
//! # Quick start
//!
//! ```no_run
//! use kokage_supervisor::{ChildSpec, Supervisor};
//! use kokage_tokio::TokioSupervisorExt as _;
//! use tracing_subscriber::FmtSubscriber;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let subscriber = FmtSubscriber::builder().finish();
//! tracing::subscriber::set_global_default(subscriber)?;
//!
//! let supervisor = Supervisor::ordered()
//!     .child(ChildSpec::task("worker", |ctx| async move {
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
//! | `serde` | no | Implements `Serialize` and `Deserialize` for public policy, event, and snapshot views. |
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
//! - `examples/watch_lifecycle_recursive.rs` — reacting to tree lifecycle
//!   events.
//! - `examples/subscribe_to_snapshots.rs` — polling supervisor state.
//! - `examples/tracing.rs` — structured logging output.
//! - `examples/metrics.rs` — Prometheus metrics (requires `--features metrics`).

mod attachment;
mod builder;
mod cancellation;
mod child;
mod completion;
mod context;
mod error;
mod event;
mod handle;
mod lifecycle;
mod observability;
pub mod prelude;
mod restart;
mod runtime;
mod scheduler;
mod scope;
mod shutdown;
mod snapshot;
mod strategy;
mod supervisor;

#[cfg(test)]
mod test_scheduler {
    use std::{sync::Arc, time::Instant};

    use crate::{BoxFuture, Scheduler, TaskError, TaskHandle};

    #[derive(Clone)]
    pub(crate) struct TokioTestScheduler(Option<tokio::runtime::Handle>);

    impl TokioTestScheduler {
        pub(crate) fn current() -> Self {
            Self(tokio::runtime::Handle::try_current().ok())
        }
    }

    impl Scheduler for TokioTestScheduler {
        fn spawn(&self, future: BoxFuture<()>) -> TaskHandle {
            task_handle(
                self.0
                    .as_ref()
                    .expect("test attempted to spawn outside a Tokio runtime")
                    .spawn(future),
            )
        }

        fn spawn_blocking(&self, function: Box<dyn FnOnce() + Send>) -> TaskHandle {
            task_handle(
                self.0
                    .as_ref()
                    .expect("test attempted to spawn blocking work outside a Tokio runtime")
                    .spawn_blocking(function),
            )
        }

        fn sleep_until(&self, deadline: Instant) -> BoxFuture<()> {
            if self.0.is_some() {
                Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                    deadline,
                )))
            } else {
                Box::pin(std::future::pending())
            }
        }

        fn now(&self) -> Instant {
            if self.0.is_some() {
                tokio::time::Instant::now().into_std()
            } else {
                Instant::now()
            }
        }
    }

    fn task_handle(handle: tokio::task::JoinHandle<()>) -> TaskHandle {
        let abort = handle.abort_handle();
        let finished = Arc::new(abort.clone());
        TaskHandle::new(
            Box::pin(async move {
                handle.await.map_err(|error| {
                    if error.is_panic() {
                        TaskError::panicked(error.into_panic())
                    } else {
                        TaskError::cancelled()
                    }
                })
            }),
            move || abort.abort(),
            move || finished.is_finished(),
        )
    }
}

/// Implementation bridge for crates layered on top of `kokage-supervisor`.
///
/// This module is not a stable public API. It exists so `kokage` can attach
/// process-local actor metadata without exposing attachment machinery as part
/// of the ordinary supervisor surface.
#[doc(hidden)]
pub mod __private {
    use std::any::Any;

    use crate::{ChildSpec, SupervisorHandle};
    pub use crate::{
        attachment::{AttachedChild, AttachedChildIdentity},
        scheduler::{TaskAbortHandle, TaskId, TaskJoin, TaskSet, yield_now},
    };

    /// Adds process-local metadata to a child specification.
    pub fn attach<T>(child: ChildSpec, attachment: T) -> ChildSpec
    where
        T: Any + Send + Sync,
    {
        child.attachment(attachment)
    }

    /// Returns process-local metadata from the current supervision tree.
    pub fn attached_children<T>(handle: &SupervisorHandle) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        handle.attached_children()
    }

    /// Spawns lifecycle-bound background work once the supervisor's scheduler
    /// is installed.
    pub fn spawn_detached(handle: &SupervisorHandle, future: crate::BoxFuture<()>) {
        handle.spawn_detached(future);
    }
}

pub use builder::{DynamicSupervisorBuilder, OrderedSupervisorBuilder};
pub use cancellation::CancellationToken;
pub use child::{BoxError, ChildResult, ChildSpec};
pub use completion::{CompletionGuard, CompletionOutcome};
pub use context::ChildContext;
pub use error::{ControlError, SupervisorBuildError, SupervisorError};
pub use event::ExitStatusView;
pub use handle::SupervisorHandle;
pub use lifecycle::{
    ChildLifecycleEvent, ChildLifecycleEventKind, ChildLifecycleWatch, LifecycleEvent,
    LifecycleEventKind, LifecyclePathSegment, LifecycleWatch, SupervisorLifecycleEvent,
};
pub use restart::{BackoffPolicy, RestartConfig, RestartPolicy};
pub use scheduler::{BoxFuture, Scheduler, TaskError, TaskHandle};
pub use scope::ScopeKind;
pub use shutdown::ShutdownPolicy;
pub use snapshot::{
    ChildExitView, ChildMembershipView, ChildSnapshot, ChildStateView, SnapshotRecvError,
    SupervisorSnapshot, SupervisorSnapshotReceiver, SupervisorStateView,
};
pub use strategy::Strategy;
pub use supervisor::Supervisor;
