//! Recoverable intake gate driven by run failures, budget, and restart totals.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use kokage::{Actor, ActorRef, Context, ExitResult, TimerKey};
use tokio::time::Instant;

use crate::{
    messages::{
        BudgetMsg, GUARD_THRESHOLD, GUARD_WINDOW, GuardMsg, GuardReport, PHASE_TIMEOUT,
        PROBE_BACKOFF_BASE, RouterMsg,
    },
    model::ScriptedModel,
};

pub struct Guard {
    budget: ActorRef<BudgetMsg>,
    router: ActorRef<RouterMsg>,
    model: ScriptedModel,
    gate: Arc<AtomicBool>,
    failures: VecDeque<Instant>,
    report: GuardReport,
    backoff_multiplier: u32,
}

const PROBE_TIMER: TimerKey = TimerKey::new("guard-probe");

impl Guard {
    pub fn new(
        budget: ActorRef<BudgetMsg>,
        router: ActorRef<RouterMsg>,
        model: ScriptedModel,
        gate: Arc<AtomicBool>,
    ) -> Self {
        Self {
            budget,
            router,
            model,
            gate,
            failures: VecDeque::new(),
            report: GuardReport::default(),
            backoff_multiplier: 1,
        }
    }

    async fn set_paused(&mut self, paused: bool, ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.report.paused == paused {
            return Ok(());
        }
        self.report.paused = paused;
        self.gate.store(!paused, Ordering::Release);
        self.router.send(RouterMsg::PauseChanged { paused }).await?;
        if paused {
            self.backoff_multiplier = 1;
            ctx.set_timeout(PROBE_TIMER, GuardMsg::Probe, PROBE_BACKOFF_BASE);
        }
        Ok(())
    }
}

impl Actor for Guard {
    type Msg = GuardMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            GuardMsg::RunFailureObserved { chat, task } => {
                tracing::debug!(chat, task, "guard observed run failure");
                self.report.run_failures += 1;
                *self.report.run_failures_by_chat.entry(chat).or_default() += 1;
                let now = Instant::now();
                self.failures.push_back(now);
                while self
                    .failures
                    .front()
                    .is_some_and(|first| now.duration_since(*first) > GUARD_WINDOW)
                {
                    self.failures.pop_front();
                }
                if self.failures.len() >= GUARD_THRESHOLD {
                    self.set_paused(true, ctx).await?;
                }
            }
            GuardMsg::BudgetExceeded => {
                self.set_paused(true, ctx).await?;
            }
            GuardMsg::BridgeRestarts { total } => {
                self.report.bridge_restarts = total;
            }
            GuardMsg::Probe => {
                self.report.probes += 1;
                let under_cap = self
                    .budget
                    .call(|reply| BudgetMsg::UnderCap { reply }, PHASE_TIMEOUT)
                    .await
                    .unwrap_or(false);
                if self.model.probe() && under_cap {
                    self.failures.clear();
                    self.set_paused(false, ctx).await?;
                } else {
                    self.report.failed_probes += 1;
                    self.backoff_multiplier = self.backoff_multiplier.saturating_mul(2).min(8);
                    ctx.set_timeout(
                        PROBE_TIMER,
                        GuardMsg::Probe,
                        PROBE_BACKOFF_BASE * self.backoff_multiplier,
                    );
                }
            }
            GuardMsg::Paused { reply } => reply.send(self.report.paused),
            GuardMsg::Report { reply } => reply.send(self.report.clone()),
        }
        Ok(())
    }
}
