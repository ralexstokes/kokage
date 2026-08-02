use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::future::join_all;
use kokage::prelude::*;
use tokio::time::Instant;

use crate::protocol::{ControlMsg, GATEWAY_DEADLINE, GatewayMsg, HealthMsg, HealthReport};

pub const BREAKER_WINDOW: Duration = Duration::from_secs(30);
pub const BREAKER_THRESHOLD: usize = 4;

#[derive(kokage::ActorFactory)]
pub struct Control {
    pub gateways: Vec<ActorRef<GatewayMsg>>,
    pub intake_gate: Arc<AtomicBool>,
}

impl Control {
    async fn cancel_all(&self) -> usize {
        join_all(self.gateways.iter().cloned().map(|gateway| async move {
            gateway
                .call(|reply| GatewayMsg::CancelAll { reply }, GATEWAY_DEADLINE)
                .await
                .unwrap_or_default()
        }))
        .await
        .into_iter()
        .sum()
    }
}

impl Actor for Control {
    type Msg = ControlMsg;

    async fn handle(&mut self, message: ControlMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ControlMsg::TripBreaker => {
                // Fail closed before doing any awaited cleanup work.
                self.intake_gate.store(false, Ordering::Release);
                let cancelled = self.cancel_all().await;
                tracing::warn!(cancelled, "restart breaker closed order intake");
            }
            ControlMsg::EmergencyCancelAll { reply } => reply.send(self.cancel_all().await),
        }
        Ok(())
    }
}

#[derive(kokage::ActorFactory)]
pub struct HealthBreaker {
    pub control: ActorRef<ControlMsg>,
    #[factory(default)]
    observed_total: u64,
    #[factory(default)]
    restarts: VecDeque<Instant>,
    #[factory(default)]
    tripped: bool,
}

impl HealthBreaker {
    fn prune(&mut self, now: Instant) {
        while self
            .restarts
            .front()
            .is_some_and(|restart| now.duration_since(*restart) > BREAKER_WINDOW)
        {
            self.restarts.pop_front();
        }
    }

    fn report(&mut self) -> HealthReport {
        self.prune(Instant::now());
        HealthReport {
            observed_total: self.observed_total,
            restarts_in_window: self.restarts.len(),
            tripped: self.tripped,
        }
    }
}

impl Actor for HealthBreaker {
    type Msg = HealthMsg;

    async fn handle(&mut self, message: HealthMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            HealthMsg::RestartsObserved { total } => {
                let newly_observed = total.saturating_sub(self.observed_total);
                self.observed_total = self.observed_total.max(total);
                let now = Instant::now();
                self.prune(now);
                self.restarts
                    .extend(std::iter::repeat_n(now, newly_observed as usize));
                if newly_observed > 0 {
                    tracing::warn!(
                        total,
                        newly_observed,
                        in_window = self.restarts.len(),
                        "venue restart observed"
                    );
                }
                if !self.tripped && self.restarts.len() >= BREAKER_THRESHOLD {
                    self.tripped = true;
                    self.control.send(ControlMsg::TripBreaker).await?;
                }
            }
            HealthMsg::Report { reply } => reply.send(self.report()),
        }
        Ok(())
    }
}
