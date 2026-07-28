//! Common imports for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

// Keep this list mirrored by tokio_otp::prelude; its prelude test guards drift.
pub use crate::{
    AttachedChild, AttachedChildIdentity, BackoffPolicy, BoxError, ChildContext,
    ChildMembershipView, ChildResult, ChildSnapshot, ChildSpec, ChildStateView, CompletionGuard,
    CompletionOutcome, ControlError, ControlOperation, DynamicSupervisorBuilder, ExitStatusView,
    LifecycleEvent, LifecycleEventKind, LifecyclePathSegment, LifecycleWatch,
    RecursiveLifecycleEvent, RecursiveLifecycleEventKind, RecursiveLifecycleWatch,
    RestartIntensity, RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, Supervisor,
    SupervisorBuildError, SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSnapshot,
    SupervisorSpec, SupervisorStateView,
};
