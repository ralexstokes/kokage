use thiserror::Error;

/// Errors returned when building a [`Supervisor`](crate::Supervisor) from a
/// [`SupervisorBuilder`](crate::SupervisorBuilder).
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum SupervisorBuildError {
    /// Two or more children share the same id string.
    #[error("duplicate child id: {0}")]
    DuplicateChildId(String),
    /// A configuration value (channel capacity, restart intensity, etc.) is
    /// invalid.
    #[error("invalid supervisor configuration: {0}")]
    InvalidConfig(&'static str),
}

/// Fatal errors that cause a running supervisor to exit.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum SupervisorError {
    /// The supervisor stopped or a child exited before reporting startup
    /// readiness.
    #[error("supervisor startup aborted: {0}")]
    StartupAborted(String),
    /// A child exceeded its [`RestartIntensity`](crate::RestartIntensity)
    /// limit, so the supervisor cannot continue.
    #[error("restart intensity exceeded")]
    RestartIntensityExceeded,
    /// One or more children did not exit within their configured grace period
    /// during shutdown. The contained string lists the timed-out child ids.
    #[error("shutdown timed out: {0}")]
    ShutdownTimedOut(String),
    /// An unexpected internal condition. Indicates a bug in the supervisor
    /// runtime.
    #[error("internal supervisor error: {0}")]
    Internal(String),
}

/// Errors returned by control-plane operations on a
/// [`SupervisorHandle`](crate::SupervisorHandle) (e.g. adding or removing
/// children at runtime).
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlError {
    /// The operation is incompatible with the supervisor's immutable scope
    /// kind.
    #[error("{operation} is unsupported by {kind} scopes")]
    UnsupportedByScopeKind {
        /// The rejected operation.
        operation: crate::ControlOperation,
        /// The immutable kind of the target scope.
        kind: crate::ScopeKind,
    },
    /// A child with this id already exists in the supervisor.
    #[error("duplicate child id: {0}")]
    DuplicateChildId(String),
    /// No child with this id is known to the supervisor.
    #[error("unknown child id: {0}")]
    UnknownChildId(String),
    /// A removal request for this child is already in progress.
    #[error("child removal already in progress: {0}")]
    ChildRemovalInProgress(String),
    /// The child spec contains invalid configuration.
    #[error("invalid child configuration: {0}")]
    InvalidConfig(&'static str),
    /// The supervisor is in the process of shutting down and is no longer
    /// accepting commands.
    #[error("supervisor is stopping")]
    SupervisorStopping,
    /// A child did not exit within its grace period during removal.
    #[error("child removal timed out: {0}")]
    ShutdownTimedOut(String),
    /// The supervisor task has already exited and the control channel is
    /// closed.
    #[error("supervisor control plane is unavailable")]
    Unavailable,
    /// An unexpected internal condition. Indicates a bug in the supervisor
    /// runtime.
    #[error("internal supervisor control error: {0}")]
    Internal(String),
}
