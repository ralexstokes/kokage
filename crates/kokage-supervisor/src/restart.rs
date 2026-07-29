use std::time::Duration;

use crate::error::SupervisorBuildError;

/// Delay applied between restart attempts.
///
/// Use [`fixed`](Self::fixed) for a constant delay or
/// [`exponential`](Self::exponential) for a delay that grows after consecutive
/// short-lived incarnations. The default restarts immediately.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Backoff {
    kind: BackoffKind,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum BackoffKind {
    #[default]
    None,
    Fixed(Duration),
    Exponential {
        base: Duration,
        factor: u32,
        max: Duration,
        jitter: bool,
    },
}

impl Backoff {
    /// Restarts immediately.
    pub const fn none() -> Self {
        Self {
            kind: BackoffKind::None,
        }
    }

    /// Waits the same `delay` before every restart.
    pub const fn fixed(delay: Duration) -> Self {
        Self {
            kind: BackoffKind::Fixed(delay),
        }
    }

    /// Uses `base * factor^attempt`, clamped to `max`.
    pub const fn exponential(base: Duration, factor: u32, max: Duration) -> Self {
        Self {
            kind: BackoffKind::Exponential {
                base,
                factor,
                max,
                jitter: false,
            },
        }
    }

    /// Applies equal jitter in `[delay / 2, delay]` to exponential delays.
    #[must_use]
    pub const fn jitter(mut self) -> Self {
        if let BackoffKind::Exponential { jitter, .. } = &mut self.kind {
            *jitter = true;
        }
        self
    }

    fn validate(self) -> Result<(), SupervisorBuildError> {
        match self.kind {
            BackoffKind::None => Ok(()),
            BackoffKind::Fixed(delay) => {
                require_non_zero_duration(delay, "fixed backoff delay must be non-zero")
            }
            BackoffKind::Exponential {
                base, factor, max, ..
            } => {
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

/// Complete restart behavior for a supervised child.
///
/// A single value selects which exits restart, the restart budget and delay,
/// and whether a terminal dynamic membership is removed. The default is
/// [`on_failure`](Self::on_failure), limited to five restarts within thirty
/// seconds, with immediate retries and retained terminal membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Restart {
    mode: RestartMode,
    max_restarts: usize,
    within: Duration,
    backoff: Backoff,
    remove_when_done: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum RestartMode {
    Always,
    #[default]
    OnFailure,
    Never,
}

impl Default for Restart {
    fn default() -> Self {
        Self::on_failure()
    }
}

impl Restart {
    const fn with_mode(mode: RestartMode) -> Self {
        Self {
            mode,
            max_restarts: 5,
            within: Duration::from_secs(30),
            backoff: Backoff::none(),
            remove_when_done: false,
        }
    }

    /// Restarts after every exit, including clean completion.
    pub const fn always() -> Self {
        Self::with_mode(RestartMode::Always)
    }

    /// Restarts after an error, panic, or abort, but not clean completion.
    pub const fn on_failure() -> Self {
        Self::with_mode(RestartMode::OnFailure)
    }

    /// Never restarts; the child runs at most once.
    pub const fn never() -> Self {
        Self::with_mode(RestartMode::Never)
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

    /// Removes the membership after an exit this policy does not restart.
    #[must_use]
    pub const fn remove_when_done(mut self) -> Self {
        self.remove_when_done = true;
        self
    }

    /// Returns the maximum restarts allowed inside the sliding window.
    pub const fn max_restarts(self) -> usize {
        self.max_restarts
    }

    /// Returns the sliding restart-budget window.
    pub const fn within(self) -> Duration {
        self.within
    }

    /// Returns the configured retry delay.
    pub const fn backoff_value(self) -> Backoff {
        self.backoff
    }

    #[doc(hidden)]
    pub const fn should_restart(self, is_failure: bool) -> bool {
        match self.mode {
            RestartMode::Always => true,
            RestartMode::OnFailure => is_failure,
            RestartMode::Never => false,
        }
    }

    pub(crate) const fn is_always(self) -> bool {
        matches!(self.mode, RestartMode::Always)
    }

    #[doc(hidden)]
    pub const fn is_never(self) -> bool {
        matches!(self.mode, RestartMode::Never)
    }

    pub(crate) const fn remove_on_exit(self) -> bool {
        self.remove_when_done
    }

    pub(crate) fn validate(self) -> Result<(), SupervisorBuildError> {
        require_non_zero_duration(self.within, "restart intensity window must be non-zero")?;
        self.backoff.validate()
    }
}

pub(crate) enum BackoffParts {
    None,
    Fixed(Duration),
    Exponential {
        base: Duration,
        factor: u32,
        max: Duration,
        jitter: bool,
    },
}

impl Backoff {
    pub(crate) const fn parts(self) -> BackoffParts {
        match self.kind {
            BackoffKind::None => BackoffParts::None,
            BackoffKind::Fixed(delay) => BackoffParts::Fixed(delay),
            BackoffKind::Exponential {
                base,
                factor,
                max,
                jitter,
            } => BackoffParts::Exponential {
                base,
                factor,
                max,
                jitter,
            },
        }
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
