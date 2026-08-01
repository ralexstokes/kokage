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
/// The enum is intentionally transparent: match it directly when inspecting a
/// declaration or construct a variant directly when that is clearer than the
/// fluent helpers. Downstream matches need a catch-all arm because the enum is
/// non-exhaustive.
///
/// With the `serde` feature, the variant is tagged as `condition`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "condition"))]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Restart after every exit, including clean completion.
    Always {
        /// Maximum eligible exits inside `within` before the scope fails.
        max_restarts: usize,
        /// Sliding window used by the restart budget.
        within: Duration,
        /// Delay applied before each replacement incarnation.
        backoff: Backoff,
    },
    /// Restart errors, panics, and aborts, but not clean completion.
    OnFailure {
        /// Maximum eligible exits inside `within` before the scope fails.
        max_restarts: usize,
        /// Sliding window used by the restart budget.
        within: Duration,
        /// Delay applied before each replacement incarnation.
        backoff: Backoff,
    },
    /// Never restart; the child runs at most once.
    ///
    /// [`limit`](Self::limit) and [`backoff`](Self::backoff) have no effect on
    /// this variant.
    Never,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::on_failure()
    }
}

impl RestartPolicy {
    /// Restarts after every exit, including clean completion.
    pub const fn always() -> Self {
        Self::Always {
            max_restarts: 5,
            within: Duration::from_secs(30),
            backoff: Backoff::none(),
        }
    }

    /// Restarts after an error, panic, or abort, but not clean completion.
    pub const fn on_failure() -> Self {
        Self::OnFailure {
            max_restarts: 5,
            within: Duration::from_secs(30),
            backoff: Backoff::none(),
        }
    }

    /// Never restarts; the child runs at most once.
    ///
    /// Calling [`limit`](Self::limit) or [`backoff`](Self::backoff) on the
    /// returned policy has no effect.
    pub const fn never() -> Self {
        Self::Never
    }

    /// Sets the sliding restart budget.
    ///
    /// This modifier has no effect on [`Never`](Self::Never), which has no
    /// restart attempts to limit.
    #[must_use]
    pub const fn limit(mut self, new_max_restarts: usize, new_within: Duration) -> Self {
        match self {
            Self::Always {
                ref mut max_restarts,
                ref mut within,
                ..
            }
            | Self::OnFailure {
                ref mut max_restarts,
                ref mut within,
                ..
            } => {
                *max_restarts = new_max_restarts;
                *within = new_within;
            }
            Self::Never => {}
        }
        self
    }

    /// Sets the delay between restart attempts.
    ///
    /// This modifier has no effect on [`Never`](Self::Never), which has no
    /// restart attempts to delay.
    #[must_use]
    pub const fn backoff(mut self, new_backoff: Backoff) -> Self {
        match self {
            Self::Always {
                ref mut backoff, ..
            }
            | Self::OnFailure {
                ref mut backoff, ..
            } => *backoff = new_backoff,
            Self::Never => {}
        }
        self
    }

    pub(crate) const fn should_restart(self, is_failure: bool) -> bool {
        match self {
            Self::Always { .. } => true,
            Self::OnFailure { .. } => is_failure,
            Self::Never => false,
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_always(self) -> bool {
        matches!(self, Self::Always { .. })
    }

    pub(crate) const fn is_never(self) -> bool {
        matches!(self, Self::Never)
    }

    pub(crate) fn validate(self) -> Result<(), BuildError> {
        match self {
            Self::Always {
                within, backoff, ..
            }
            | Self::OnFailure {
                within, backoff, ..
            } => {
                require_non_zero_duration(within, "restart intensity window must be non-zero")?;
                backoff.validate()
            }
            Self::Never => Ok(()),
        }
    }

    /// Returns the maximum eligible exits in the restart window.
    pub const fn max_restarts(self) -> Option<usize> {
        match self {
            Self::Always { max_restarts, .. } | Self::OnFailure { max_restarts, .. } => {
                Some(max_restarts)
            }
            Self::Never => None,
        }
    }

    /// Returns the sliding restart-budget window.
    pub const fn within(self) -> Option<Duration> {
        match self {
            Self::Always { within, .. } | Self::OnFailure { within, .. } => Some(within),
            Self::Never => None,
        }
    }

    /// Returns the delay applied before each replacement incarnation.
    pub const fn backoff_policy(self) -> Option<Backoff> {
        match self {
            Self::Always { backoff, .. } | Self::OnFailure { backoff, .. } => Some(backoff),
            Self::Never => None,
        }
    }
}

fn require_non_zero_duration(duration: Duration, message: &'static str) -> Result<(), BuildError> {
    if duration.is_zero() {
        return Err(BuildError::InvalidConfig(message));
    }
    Ok(())
}
