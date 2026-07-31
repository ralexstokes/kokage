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
        /// Maximum number of eligible exits inside `within`.
        max_restarts: usize,
        /// Sliding window used by `max_restarts`.
        within: Duration,
        /// Delay applied before each replacement incarnation starts.
        backoff: Backoff,
    },
    /// Restart errors, panics, and aborts, but not clean completion.
    OnFailure {
        /// Maximum number of eligible exits inside `within`.
        max_restarts: usize,
        /// Sliding window used by `max_restarts`.
        within: Duration,
        /// Delay applied before each replacement incarnation starts.
        backoff: Backoff,
    },
    /// Never restart; the child runs at most once.
    Never,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::on_failure()
    }
}

impl RestartPolicy {
    const fn defaults(always: bool) -> Self {
        if always {
            Self::Always {
                max_restarts: 5,
                within: Duration::from_secs(30),
                backoff: Backoff::none(),
            }
        } else {
            Self::OnFailure {
                max_restarts: 5,
                within: Duration::from_secs(30),
                backoff: Backoff::none(),
            }
        }
    }

    /// Restarts after every exit, including clean completion.
    pub const fn always() -> Self {
        Self::defaults(true)
    }

    /// Restarts after an error, panic, or abort, but not clean completion.
    pub const fn on_failure() -> Self {
        Self::defaults(false)
    }

    /// Never restarts; the child runs at most once.
    pub const fn never() -> Self {
        Self::Never
    }

    /// Sets the sliding restart budget.
    #[must_use]
    pub const fn limit(self, max_restarts: usize, within: Duration) -> Self {
        match self {
            Self::Always { backoff, .. } => Self::Always {
                max_restarts,
                within,
                backoff,
            },
            Self::OnFailure { backoff, .. } => Self::OnFailure {
                max_restarts,
                within,
                backoff,
            },
            Self::Never => Self::Never,
        }
    }

    /// Sets the delay between restart attempts.
    #[must_use]
    pub const fn backoff(self, backoff: Backoff) -> Self {
        match self {
            Self::Always {
                max_restarts,
                within,
                ..
            } => Self::Always {
                max_restarts,
                within,
                backoff,
            },
            Self::OnFailure {
                max_restarts,
                within,
                ..
            } => Self::OnFailure {
                max_restarts,
                within,
                backoff,
            },
            Self::Never => Self::Never,
        }
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
        let Some((_, within, backoff)) = self.settings() else {
            return Ok(());
        };
        require_non_zero_duration(within, "restart intensity window must be non-zero")?;
        backoff.validate()
    }

    pub(crate) const fn settings(self) -> Option<(usize, Duration, Backoff)> {
        match self {
            Self::Always {
                max_restarts,
                within,
                backoff,
            }
            | Self::OnFailure {
                max_restarts,
                within,
                backoff,
            } => Some((max_restarts, within, backoff)),
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
