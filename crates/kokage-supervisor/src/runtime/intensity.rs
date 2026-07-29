use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time::Instant;

use crate::restart::{BackoffParts, Restart};

/// Sliding-window restart rate limiter.
///
/// Maintains a deque of restart timestamps. On each `record` call, timestamps
/// older than `intensity.within` are evicted. If the remaining count exceeds
/// `intensity.max_restarts`, [`exceeded`](Self::exceeded) returns `true`.
///
/// Also computes the backoff delay for the next restart attempt based on the
/// configured [`Backoff`]. Backoff uses a consecutive-restart counter
/// that resets after an incarnation runs longer than `intensity.within`; it is
/// independent of timestamp eviction from the intensity window.
pub(crate) struct RestartTracker {
    intensity: Restart,
    times: VecDeque<Instant>,
    rng: JitterRng,
    total_restarts: u64,
    consecutive_restarts: usize,
    run_started_at: Option<Instant>,
}

impl RestartTracker {
    pub(crate) fn new(intensity: Restart) -> Self {
        Self {
            intensity,
            times: VecDeque::new(),
            rng: JitterRng::new(),
            total_restarts: 0,
            consecutive_restarts: 0,
            run_started_at: None,
        }
    }

    pub(crate) fn record_spawn(&mut self, now: Instant) {
        self.run_started_at = Some(now);
    }

    pub(crate) fn record_exit(&mut self, now: Instant) {
        if self
            .run_started_at
            .take()
            .is_some_and(|started_at| now.duration_since(started_at) > self.intensity.within())
        {
            self.consecutive_restarts = 0;
        }
    }

    pub(crate) fn record_restart(&mut self, now: Instant) {
        while let Some(front) = self.times.front() {
            if now.duration_since(*front) > self.intensity.within() {
                self.times.pop_front();
            } else {
                break;
            }
        }
        self.times.push_back(now);
        self.total_restarts = self.total_restarts.saturating_add(1);
        self.consecutive_restarts = self.consecutive_restarts.saturating_add(1);
    }

    pub(crate) fn exceeded(&self) -> bool {
        self.times.len() > self.intensity.max_restarts()
    }

    pub(crate) fn backoff(&mut self) -> Duration {
        let (deterministic, jitter) = match self.intensity.backoff_value().parts() {
            BackoffParts::None => return Duration::ZERO,
            BackoffParts::Fixed(delay) => return delay,
            BackoffParts::Exponential {
                base,
                factor,
                max,
                jitter,
            } => (
                exponential_backoff(
                    base,
                    factor,
                    max,
                    self.consecutive_restarts.saturating_sub(1),
                ),
                jitter,
            ),
        };

        if jitter {
            self.rng.jitter_between(deterministic / 2, deterministic)
        } else {
            deterministic
        }
    }

    pub(crate) fn total_restarts(&self) -> u64 {
        self.total_restarts
    }
}

fn exponential_backoff(base: Duration, factor: u32, max: Duration, steps: usize) -> Duration {
    let mut delay = base;
    for _ in 0..steps {
        delay = delay.saturating_mul(factor);
        if delay >= max {
            return max;
        }
    }
    delay.min(max)
}

struct JitterRng {
    state: u64,
}

impl JitterRng {
    fn new() -> Self {
        // This is only for decorrelating restart delays, not for secrets or
        // adversarial randomness.
        Self {
            state: seed_jitter_rng(),
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = xorshift64(self.state);
        self.state
    }

    fn jitter_between(&mut self, min: Duration, max: Duration) -> Duration {
        if min >= max {
            return max;
        }

        let min_nanos = min.as_nanos();
        let span = max.as_nanos() - min_nanos;
        let offset = u128::from(self.next_u64()) % (span + 1);
        duration_from_nanos(min_nanos + offset)
    }
}

fn seed_jitter_rng() -> u64 {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(err) => err.duration().as_nanos(),
    };
    let seed = (nanos as u64) ^ ((nanos >> 64) as u64) ^ 0x9e37_79b9_7f4a_7c15;
    if seed == 0 { 1 } else { seed }
}

fn xorshift64(mut state: u64) -> u64 {
    if state == 0 {
        state = 1;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    Duration::new(secs as u64, subsec_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Backoff;

    fn tracker(policy: Backoff) -> RestartTracker {
        let intensity = Restart::on_failure()
            .limit(10, Duration::from_secs(10))
            .backoff(policy);
        RestartTracker {
            intensity,
            times: VecDeque::new(),
            rng: JitterRng {
                state: 0x1234_5678_9abc_def0,
            },
            total_restarts: 0,
            consecutive_restarts: 0,
            run_started_at: None,
        }
    }

    fn record_short_restart(tracker: &mut RestartTracker, started_at: Instant) {
        let exited_at = started_at + Duration::from_millis(1);
        tracker.record_spawn(started_at);
        tracker.record_exit(exited_at);
        tracker.record_restart(exited_at);
    }

    #[test]
    fn exponential_backoff_caps_at_maximum() {
        let delay = exponential_backoff(Duration::from_millis(10), 3, Duration::from_millis(50), 4);

        assert_eq!(delay, Duration::from_millis(50));
    }

    #[test]
    fn exponential_backoff_progresses_by_factor() {
        let mut tracker = tracker(Backoff::exponential(
            Duration::from_millis(10),
            2,
            Duration::from_millis(500),
        ));

        let started_at = Instant::now();
        record_short_restart(&mut tracker, started_at);
        assert_eq!(tracker.backoff(), Duration::from_millis(10));

        record_short_restart(&mut tracker, started_at + Duration::from_millis(2));
        assert_eq!(tracker.backoff(), Duration::from_millis(20));

        record_short_restart(&mut tracker, started_at + Duration::from_millis(4));
        assert_eq!(tracker.backoff(), Duration::from_millis(40));

        record_short_restart(&mut tracker, started_at + Duration::from_millis(6));
        assert_eq!(tracker.backoff(), Duration::from_millis(80));
    }

    #[test]
    fn exponential_backoff_with_factor_one_stays_constant() {
        let mut tracker = tracker(Backoff::exponential(
            Duration::from_millis(25),
            1,
            Duration::from_millis(500),
        ));

        let started_at = Instant::now();
        for offset in 0..4 {
            record_short_restart(&mut tracker, started_at + Duration::from_millis(offset * 2));
            assert_eq!(tracker.backoff(), Duration::from_millis(25));
        }
    }

    #[test]
    fn exponential_backoff_overflow_clamps_to_maximum() {
        let delay = exponential_backoff(
            Duration::from_secs(u64::MAX / 2 + 1),
            3,
            Duration::from_secs(90),
            1,
        );

        assert_eq!(delay, Duration::from_secs(90));
    }

    #[test]
    fn jittered_backoff_stays_within_equal_jitter_bounds() {
        let mut tracker = tracker(
            Backoff::exponential(Duration::from_millis(80), 2, Duration::from_millis(500)).jitter(),
        );
        let started_at = Instant::now();
        record_short_restart(&mut tracker, started_at);
        record_short_restart(&mut tracker, started_at + Duration::from_millis(2));

        let delay = tracker.backoff();

        assert!(delay >= Duration::from_millis(80));
        assert!(delay <= Duration::from_millis(160));
    }

    #[test]
    fn exponential_backoff_does_not_shrink_when_intensity_timestamps_age_out() {
        let mut tracker = tracker(Backoff::exponential(
            Duration::from_millis(10),
            2,
            Duration::from_millis(500),
        ));
        let started_at = Instant::now();

        record_short_restart(&mut tracker, started_at);
        record_short_restart(&mut tracker, started_at + Duration::from_secs(1));
        record_short_restart(&mut tracker, started_at + Duration::from_secs(12));

        assert_eq!(tracker.times.len(), 1);
        assert_eq!(tracker.backoff(), Duration::from_millis(40));
    }

    #[test]
    fn exponential_backoff_resets_after_a_run_outlives_intensity_window() {
        let mut tracker = tracker(Backoff::exponential(
            Duration::from_millis(10),
            2,
            Duration::from_millis(500),
        ));
        let started_at = Instant::now();

        record_short_restart(&mut tracker, started_at);
        record_short_restart(&mut tracker, started_at + Duration::from_secs(1));
        tracker.record_spawn(started_at + Duration::from_secs(2));
        let exited_at = started_at + Duration::from_secs(13);
        tracker.record_exit(exited_at);
        tracker.record_restart(exited_at);

        assert_eq!(tracker.backoff(), Duration::from_millis(10));
    }
}
