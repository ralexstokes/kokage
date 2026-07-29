use std::time::Duration;

/// Internal classification of how a child task exited.
///
/// This is a cloneable, displayable view of the exit status; the original error
/// value (if any) is converted to its `Display` string.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub(crate) enum ExitKind {
    /// The child returned `Ok(())`.
    Completed,
    /// The child returned an `Err`. The string is the error's `Display` output.
    Failed(String),
    /// The child task panicked.
    Panicked,
    /// The child task was aborted by the supervisor.
    Aborted {
        /// Whether cooperative shutdown exhausted its grace period before the
        /// supervisor aborted the task.
        after_grace: bool,
    },
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
        status: ExitKind,
    },
    ChildRestartScheduled {
        id: String,
        lineage: u64,
        generation: u64,
        delay: Duration,
        total_restarts: u64,
        child_restart_count: u64,
    },
    ChildRestarted {
        id: String,
        old_generation: u64,
        new_generation: u64,
    },
    RestartIntensityExceeded,
}
