//! Common imports for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

// Keep this list mirrored by tokio_otp::prelude; its prelude test guards drift.
pub use crate::{
    BackoffPolicy, BoxError, ChildContext, ChildMembershipView, ChildResult, ChildSnapshot,
    ChildSpec, ChildStateView, CompletionOutcome, ControlError, DynamicSupervisorBuilder,
    ExitStatusView, LifecycleEvent, LifecyclePathSegment, LifecycleWatch, RestartIntensity,
    RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, Supervisor,
    SupervisorBuildError, SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSnapshot,
    SupervisorSpec, SupervisorStateView,
};
