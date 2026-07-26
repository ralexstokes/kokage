use std::time::Duration;

/// Controls whether clean exits from significant children automatically stop
/// their supervisor.
///
/// A child is marked significant with
/// [`ChildSpec::significant`](crate::ChildSpec::significant) or
/// [`SupervisorSpec::significant`](crate::SupervisorSpec::significant).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum AutoShutdown {
    /// Significant child exits do not stop the supervisor.
    #[default]
    Never,
    /// Stop when any significant child exits cleanly.
    AnySignificant,
    /// Stop after every significant child has exited cleanly.
    ///
    /// A significant child using [`RestartPolicy::Never`](crate::RestartPolicy::Never)
    /// that exits with a failure, or is cancelled by a sibling's group
    /// restart, cannot later satisfy this condition. The supervisor will
    /// continue running until explicitly stopped.
    AllSignificant,
}

/// How the supervisor stops a child task during shutdown or removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ShutdownMode {
    /// Like [`CooperativeThenAbort`](ShutdownMode::CooperativeThenAbort), but
    /// failing to exit within the grace period is reported as a timeout error
    /// instead of being aborted silently.
    ///
    /// Abort remains cooperative at Tokio poll boundaries, so a non-yielding
    /// future can outlive the shutdown call briefly. For hard-stop guarantees,
    /// isolate blocking work outside the supervised Tokio task.
    CooperativeStrict,
    /// Wait for the grace period, then issue a Tokio abort and return promptly.
    ///
    /// Abort remains cooperative at Tokio poll boundaries, so a non-yielding
    /// future can outlive the shutdown call briefly. For hard-stop guarantees,
    /// isolate blocking work outside the supervised Tokio task.
    CooperativeThenAbort,
    /// Issue a Tokio abort and return promptly.
    ///
    /// Abort remains cooperative at Tokio poll boundaries, so this mode does not
    /// forcibly preempt a non-yielding future. For a nested supervisor child,
    /// the abort cascades recursively through the nested subtree instead of
    /// leaving that subtree to drain without a supervisor above it.
    Abort,
}

/// Shutdown behaviour for a single child, combining a [`ShutdownMode`] with a
/// grace period.
///
/// The default is [`CooperativeThenAbort`](ShutdownMode::CooperativeThenAbort)
/// with a 5-second grace period.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ShutdownPolicy {
    /// How long to wait for the child to exit after its cancellation token is
    /// triggered.
    pub grace: Duration,
    /// What to do when the grace period expires (or immediately, for
    /// [`Abort`](ShutdownMode::Abort)).
    pub mode: ShutdownMode,
}

impl ShutdownPolicy {
    /// Creates a policy with an explicit mode and grace period.
    pub fn new(grace: Duration, mode: ShutdownMode) -> Self {
        Self { grace, mode }
    }

    /// Strict cooperative shutdown: cancel the child and wait up to `grace`
    /// for it to exit. If the child does not exit within the grace period, the
    /// task is aborted and a timeout error is reported.
    pub fn cooperative_strict(grace: Duration) -> Self {
        Self::new(grace, ShutdownMode::CooperativeStrict)
    }

    /// Cancel the child and wait up to `grace`; if it has not exited by then,
    /// abort the Tokio task.
    pub fn cooperative_then_abort(grace: Duration) -> Self {
        Self::new(grace, ShutdownMode::CooperativeThenAbort)
    }

    /// Abort the Tokio task immediately with no grace period.
    pub fn abort() -> Self {
        Self::new(Duration::ZERO, ShutdownMode::Abort)
    }
}

impl Default for ShutdownPolicy {
    fn default() -> Self {
        Self::cooperative_then_abort(Duration::from_secs(5))
    }
}
