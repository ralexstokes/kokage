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

/// Restart budget and delay shared by restartable [`RestartPolicy`] variants.
///
/// The settings are one stable payload so adding another restart tuning knob
/// does not change every policy variant. Use [`new`](Self::new) for direct
/// construction or the fluent methods on [`RestartPolicy`] for the common
/// case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct RestartSettings {
    max_restarts: usize,
    within: Duration,
    backoff: Backoff,
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(30))
    }
}

impl RestartSettings {
    /// Creates an immediate-retry budget of `max_restarts` inside `within`.
    pub const fn new(max_restarts: usize, within: Duration) -> Self {
        Self {
            max_restarts,
            within,
            backoff: Backoff::none(),
        }
    }

    /// Replaces the sliding restart budget.
    #[must_use]
    pub const fn limit(mut self, max_restarts: usize, within: Duration) -> Self {
        self.max_restarts = max_restarts;
        self.within = within;
        self
    }

    /// Replaces the delay between restart attempts.
    #[must_use]
    pub const fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Returns the maximum number of eligible exits inside [`within`](Self::within).
    pub const fn max_restarts(self) -> usize {
        self.max_restarts
    }

    /// Returns the sliding window used by [`max_restarts`](Self::max_restarts).
    pub const fn within(self) -> Duration {
        self.within
    }

    /// Returns the delay applied before each replacement incarnation starts.
    pub const fn backoff_policy(self) -> Backoff {
        self.backoff
    }

    fn validate(self) -> Result<(), BuildError> {
        require_non_zero_duration(self.within, "restart intensity window must be non-zero")?;
        self.backoff.validate()
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
#[cfg_attr(feature = "serde", serde(tag = "condition", content = "settings"))]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Restart after every exit, including clean completion.
    Always(RestartSettings),
    /// Restart errors, panics, and aborts, but not clean completion.
    OnFailure(RestartSettings),
    /// Never restart; the child runs at most once.
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
        Self::Always(RestartSettings::new(5, Duration::from_secs(30)))
    }

    /// Restarts after an error, panic, or abort, but not clean completion.
    pub const fn on_failure() -> Self {
        Self::OnFailure(RestartSettings::new(5, Duration::from_secs(30)))
    }

    /// Never restarts; the child runs at most once.
    pub const fn never() -> Self {
        Self::Never
    }

    /// Sets the sliding restart budget.
    #[must_use]
    pub const fn limit(self, max_restarts: usize, within: Duration) -> Self {
        match self {
            Self::Always(settings) => Self::Always(settings.limit(max_restarts, within)),
            Self::OnFailure(settings) => Self::OnFailure(settings.limit(max_restarts, within)),
            Self::Never => Self::Never,
        }
    }

    /// Sets the delay between restart attempts.
    #[must_use]
    pub const fn backoff(self, backoff: Backoff) -> Self {
        match self {
            Self::Always(settings) => Self::Always(settings.backoff(backoff)),
            Self::OnFailure(settings) => Self::OnFailure(settings.backoff(backoff)),
            Self::Never => Self::Never,
        }
    }

    pub(crate) const fn should_restart(self, is_failure: bool) -> bool {
        match self {
            Self::Always(_) => true,
            Self::OnFailure(_) => is_failure,
            Self::Never => false,
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_always(self) -> bool {
        matches!(self, Self::Always(_))
    }

    pub(crate) const fn is_never(self) -> bool {
        matches!(self, Self::Never)
    }

    pub(crate) fn validate(self) -> Result<(), BuildError> {
        let Some(settings) = self.settings() else {
            return Ok(());
        };
        settings.validate()
    }

    /// Returns the tuning payload for restartable conditions.
    pub const fn settings(self) -> Option<RestartSettings> {
        match self {
            Self::Always(settings) | Self::OnFailure(settings) => Some(settings),
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
