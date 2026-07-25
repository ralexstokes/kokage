use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;
use tokio_otp::prelude::*;

use crate::messages::{ControlMsg, HealthMsg};

const BREAKER_WINDOW: Duration = Duration::from_secs(10);
const BREAKER_THRESHOLD: usize = 4;

#[derive(Clone)]
pub struct Health {
    restarts: VecDeque<Instant>,
    observed_restart_total: u64,
    tripped: bool,
    control: ActorRef<ControlMsg>,
}

impl Health {
    pub fn new(control: ActorRef<ControlMsg>) -> Self {
        Self {
            restarts: VecDeque::new(),
            observed_restart_total: 0,
            tripped: false,
            control,
        }
    }
}

impl Actor for Health {
    type Msg = HealthMsg;

    async fn handle(
        &mut self,
        message: HealthMsg,
        _ctx: &mut ActorContext<HealthMsg>,
    ) -> ActorResult {
        match message {
            HealthMsg::RestartsObserved { total } => {
                let count = total.saturating_sub(self.observed_restart_total);
                self.observed_restart_total = self.observed_restart_total.max(total);
                tracing::warn!(total, count, "venue subtree restarts observed");
                let now = Instant::now();
                // The bridge sends an idempotent cumulative total, including
                // after a target restart. Count only the newly represented
                // restarts. They all carry this observation's timestamp
                // rather than their own arrival times, which can only widen
                // the window's view (trip earlier) — the fail-closed direction
                // for a breaker.
                for _ in 0..count {
                    self.restarts.push_back(now);
                }
                while self
                    .restarts
                    .front()
                    .is_some_and(|at| now.duration_since(*at) > BREAKER_WINDOW)
                {
                    self.restarts.pop_front();
                }
                if !self.tripped && self.restarts.len() >= BREAKER_THRESHOLD {
                    self.tripped = true;
                    // Control closes the shared intake gate before awaiting
                    // cancellations. Even if this bounded wait expires, the
                    // already-accepted kill switch still takes effect.
                    let _ = self
                        .control
                        .call(Duration::from_millis(500), |reply| ControlMsg::KillSwitch {
                            reply,
                        })
                        .await;
                }
            }
            HealthMsg::ResetBreaker => {
                self.restarts.clear();
                self.tripped = false;
            }
            HealthMsg::Tripped { reply } => reply.send(self.tripped),
        }
        Ok(Continue)
    }
}
