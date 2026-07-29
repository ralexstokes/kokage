use thiserror::Error;

/// Errors returned when validating or spawning an [`OrderedTree`](crate::OrderedTree),
/// a [`DynamicTree`](crate::DynamicTree), or a dynamic subtree insertion.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum BuildError {
    /// Two or more children share the same id string.
    #[error("duplicate child id: {0}")]
    DuplicateChildId(String),
    /// A configuration value (channel capacity, restart intensity, etc.) is
    /// invalid.
    #[error("invalid configuration: {0}")]
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
    /// A child exceeded its [`Restart`](crate::Restart)
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

/// Errors returned by [`DynamicRuntimeHandle`](crate::DynamicRuntimeHandle)
/// control operations such as adding or removing children at runtime.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlError {
    /// No child with this id is known to the supervisor.
    #[error("unknown child id: {0}")]
    UnknownChildId(String),
    /// A removal request for this child is already in progress.
    #[error("child removal already in progress: {0}")]
    ChildRemovalInProgress(String),
    /// A requested control-plane change was rejected during validation.
    ///
    /// Validation may happen while preparing the request or inside the running
    /// supervisor. Higher-level APIs that collapse both phases document when
    /// the nested build error does not uniquely identify a phase.
    #[error("control operation rejected: {0}")]
    Rejected(#[from] BuildError),
    /// The supervisor is in the process of shutting down and is no longer
    /// accepting commands.
    #[error("supervisor is stopping")]
    SupervisorStopping,
    /// The operation failed because the supervisor encountered a fatal error.
    #[error("supervisor operation failed: {0}")]
    Failed(#[from] SupervisorError),
    /// The supervisor task has already exited and the control channel is
    /// closed.
    #[error("supervisor control plane is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::BuildError;

    #[test]
    fn invalid_config_display_is_not_supervisor_specific() {
        assert_eq!(
            BuildError::InvalidConfig("actor mailbox capacity must be non-zero").to_string(),
            "invalid configuration: actor mailbox capacity must be non-zero"
        );
    }
}
