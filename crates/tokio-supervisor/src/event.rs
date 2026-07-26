use std::time::Duration;

/// Snapshot of how a child task exited.
///
/// This is a cloneable, displayable view of the exit status; the original error
/// value (if any) is converted to its `Display` string.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ExitStatusView {
    /// The child returned `Ok(())`.
    Completed,
    /// The child returned an `Err`. The string is the error's `Display` output.
    Failed(String),
    /// The child task panicked.
    Panicked,
    /// The child task was aborted by the supervisor (e.g. after a grace-period
    /// timeout).
    Aborted,
    /// The child's cooperative shutdown grace expired. The supervisor first
    /// offered the child wrapper a tidy-abort accounting beat and then
    /// hard-aborted it if necessary.
    ShutdownTimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeEvent {
    SupervisorStarted,
    SupervisorStopping,
    SupervisorStopped,
    ChildStarted {
        id: String,
        generation: u64,
    },
    ChildRemoved {
        id: String,
    },
    ChildExited {
        id: String,
        generation: u64,
        status: ExitStatusView,
    },
    ChildRestartScheduled {
        id: String,
        generation: u64,
        delay: Duration,
    },
    ChildRestarted {
        id: String,
        old_generation: u64,
        new_generation: u64,
    },
    RestartIntensityExceeded,
}
