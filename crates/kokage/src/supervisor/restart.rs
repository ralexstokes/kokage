use std::time::Duration;

use crate::supervisor::error::BuildError;

/// Delay applied between restart attempts.
///
/// Use [`fixed`](Self::fixed) for a constant delay or
/// [`exponential`](Self::exponential) for a delay that grows after consecutive
/// short-lived incarnations. The default restarts immediately. Match the enum
/// directly when reading a declaration; the constructors remain the concise
/// way to build one. Downstream matches need a catch-all arm because the enum
/// is non-exhaustive.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Backoff {
    /// Restarts immediately.
    #[default]
    None,
    /// Wait the same duration before every restart.
    Fixed(Duration),
    /// Grow the delay exponentially, optionally applying equal jitter.
    Exponential {
        /// Initial deterministic delay.
        base: Duration,
        /// Multiplier applied for each consecutive short-lived incarnation.
        factor: u32,
        /// Maximum deterministic delay.
        max: Duration,
        /// Whether equal jitter selects a delay between half and all of the
        /// deterministic value.
        jitter: bool,
    },
}

impl Backoff {
    /// Restarts immediately.
    pub const fn none() -> Self {
        Self::None
    }

    /// Waits the same `delay` before every restart.
    pub const fn fixed(delay: Duration) -> Self {
        Self::Fixed(delay)
    }

    /// Uses `base * factor^attempt`, clamped to `max`.
    pub const fn exponential(base: Duration, factor: u32, max: Duration) -> Self {
        Self::Exponential {
            base,
            factor,
            max,
            jitter: false,
        }
    }

    /// Uses jittered `base * factor^attempt`, clamped to `max`.
    ///
    /// Each delay is selected from the equal-jitter interval
    /// `[deterministic_delay / 2, deterministic_delay]`.
    pub const fn exponential_with_jitter(base: Duration, factor: u32, max: Duration) -> Self {
        Self::Exponential {
            base,
            factor,
            max,
            jitter: true,
        }
    }

    fn validate(self) -> Result<(), BuildError> {
        match self {
            Self::None => Ok(()),
            Self::Fixed(delay) => {
                require_non_zero_duration(delay, "fixed backoff delay must be non-zero")
            }
            Self::Exponential {
                base, factor, max, ..
            } => {
                require_non_zero_duration(base, "exponential backoff base must be non-zero")?;
                if factor == 0 {
                    return Err(BuildError::InvalidConfig(
                        "exponential backoff factor must be non-zero",
                    ));
                }
                require_non_zero_duration(max, "exponential backoff max must be non-zero")
            }
        }
    }
}

/// Complete restart behavior for a supervised child.
///
/// A single value selects which exits restart, the restart budget, and the
/// delay between attempts. The default is [`on_failure`](Self::on_failure),
/// limited to five restarts within thirty seconds, with immediate retries.
///
/// With the `serde` feature, the `mode` field serializes as `Always` (every
/// exit), `OnFailure` (only errors, panics, and aborts), or `Never` (at most
/// one run).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RestartPolicy {
    mode: RestartCondition,
    max_restarts: usize,
    within: Duration,
    backoff: Backoff,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum RestartCondition {
    Always,
    #[default]
    OnFailure,
    Never,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::on_failure()
    }
}

impl RestartPolicy {
    const fn with_mode(mode: RestartCondition) -> Self {
        Self {
            mode,
            max_restarts: 5,
            within: Duration::from_secs(30),
            backoff: Backoff::none(),
        }
    }

    /// Restarts after every exit, including clean completion.
    pub const fn always() -> Self {
        Self::with_mode(RestartCondition::Always)
    }

    /// Restarts after an error, panic, or abort, but not clean completion.
    pub const fn on_failure() -> Self {
        Self::with_mode(RestartCondition::OnFailure)
    }

    /// Never restarts; the child runs at most once.
    pub const fn never() -> Self {
        Self::with_mode(RestartCondition::Never)
    }

    /// Sets the sliding restart budget.
    #[must_use]
    pub const fn limit(mut self, max_restarts: usize, within: Duration) -> Self {
        self.max_restarts = max_restarts;
        self.within = within;
        self
    }

    /// Sets the delay between restart attempts.
    #[must_use]
    pub const fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub(crate) const fn should_restart(self, is_failure: bool) -> bool {
        match self.mode {
            RestartCondition::Always => true,
            RestartCondition::OnFailure => is_failure,
            RestartCondition::Never => false,
        }
    }

    pub(crate) const fn is_always(self) -> bool {
        matches!(self.mode, RestartCondition::Always)
    }

    pub(crate) const fn is_never(self) -> bool {
        matches!(self.mode, RestartCondition::Never)
    }

    pub(crate) const fn max_restarts(self) -> usize {
        self.max_restarts
    }

    pub(crate) const fn within(self) -> Duration {
        self.within
    }

    pub(crate) const fn backoff_policy(self) -> Backoff {
        self.backoff
    }

    pub(crate) fn validate(self) -> Result<(), BuildError> {
        require_non_zero_duration(self.within, "restart intensity window must be non-zero")?;
        self.backoff.validate()
    }
}

fn require_non_zero_duration(duration: Duration, message: &'static str) -> Result<(), BuildError> {
    if duration.is_zero() {
        return Err(BuildError::InvalidConfig(message));
    }
    Ok(())
}
