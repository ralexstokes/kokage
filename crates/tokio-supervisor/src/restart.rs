use std::time::Duration;

use crate::error::SupervisorBuildError;

/// Controls whether a child is restarted after it exits.
///
/// The default is [`OnFailure`](RestartPolicy::OnFailure).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Always restart the child, regardless of exit status. Equivalent to
    /// OTP's `permanent`.
    Always,
    /// Restart only on failure (`Err`, panic, or abort). A clean `Ok(())`
    /// exit is treated as intentional completion and is not restarted.
    /// Equivalent to OTP's `transient`.
    #[default]
    OnFailure,
    /// Never restart. The child runs at most once and is not restarted after
    /// any exit. Equivalent to OTP's `temporary`.
    Never,
}

impl RestartPolicy {
    pub(crate) fn should_restart(self, is_failure: bool) -> bool {
        match self {
            Self::Always => true,
            Self::OnFailure => is_failure,
            Self::Never => false,
        }
    }
}

/// Delay strategy applied between restart attempts.
///
/// The default is [`None`](BackoffPolicy::None) (immediate restart).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum BackoffPolicy {
    /// Restart immediately with no delay.
    #[default]
    None,
    /// Wait a constant duration before every restart attempt.
    Fixed(Duration),
    /// Wait an exponentially increasing duration: `base * factor^attempt`,
    /// clamped to `max`. The attempt count tracks consecutive restarts and
    /// resets after an incarnation runs longer than the restart intensity's
    /// `within` duration. It is independent of the sliding intensity window.
    Exponential {
        /// Initial delay applied on the first restart.
        base: Duration,
        /// Multiplicative factor applied per attempt.
        factor: u32,
        /// Upper bound on the computed delay.
        max: Duration,
    },
    /// Same progression as [`Exponential`](BackoffPolicy::Exponential), but
    /// each delay is uniformly jittered into `[delay/2, delay]` (equal jitter)
    /// to decorrelate concurrent restarts.
    JitteredExponential {
        /// Initial delay applied on the first restart.
        base: Duration,
        /// Multiplicative factor applied per attempt.
        factor: u32,
        /// Upper bound on the computed delay before jitter is applied.
        max: Duration,
    },
}

impl BackoffPolicy {
    fn validate(self) -> Result<(), SupervisorBuildError> {
        match self {
            Self::None => Ok(()),
            Self::Fixed(delay) => {
                require_non_zero_duration(delay, "fixed backoff delay must be non-zero")
            }
            Self::Exponential { base, factor, max }
            | Self::JitteredExponential { base, factor, max } => {
                require_non_zero_duration(base, "exponential backoff base must be non-zero")?;
                if factor == 0 {
                    return Err(SupervisorBuildError::InvalidConfig(
                        "exponential backoff factor must be non-zero",
                    ));
                }
                require_non_zero_duration(max, "exponential backoff max must be non-zero")
            }
        }
    }
}

/// Bounds on how often a child (or child group) may be restarted before the
/// supervisor gives up and exits with [`SupervisorError::RestartIntensityExceeded`].
///
/// Tracks a sliding window of restart timestamps: if more than `max_restarts`
/// occur within `within`, the intensity limit is breached.
///
/// The default is 5 restarts within 30 seconds with no backoff.
///
/// [`SupervisorError::RestartIntensityExceeded`]: crate::SupervisorError::RestartIntensityExceeded
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct RestartIntensity {
    /// Maximum number of restarts allowed inside the sliding window.
    pub max_restarts: usize,
    /// Length of the sliding window. Must be non-zero.
    pub within: Duration,
    /// Delay strategy inserted before each restart attempt.
    pub backoff: BackoffPolicy,
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30))
    }
}

impl RestartIntensity {
    /// Creates a new intensity limit with no backoff.
    pub fn new(max_restarts: usize, within: Duration) -> Self {
        Self {
            max_restarts,
            within,
            backoff: BackoffPolicy::None,
        }
    }

    /// Attaches a [`BackoffPolicy`] to this intensity configuration.
    #[must_use]
    pub fn with_backoff(mut self, backoff: BackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), SupervisorBuildError> {
        require_non_zero_duration(self.within, "restart intensity window must be non-zero")?;
        self.backoff.validate()
    }
}

fn require_non_zero_duration(
    duration: Duration,
    message: &'static str,
) -> Result<(), SupervisorBuildError> {
    if duration.is_zero() {
        return Err(SupervisorBuildError::InvalidConfig(message));
    }

    Ok(())
}
