use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use kokage::{Actor, ActorRef, Context, ExitResult, Reply, StopContext, TimerKey};
use tokio::sync::broadcast;

use crate::{
    common::{CALL_BOUND, Evidence, EvidenceTx},
    router::RouterMsg,
};

const PROBE: TimerKey = TimerKey::new("guard-probe");
const FAILURE_WINDOW: Duration = Duration::from_secs(1);
const FAILURE_LIMIT: usize = 2;

#[derive(Clone, Debug)]
pub struct GateNotice {
    pub open: bool,
    pub reason: String,
}

#[derive(Clone, Default)]
pub struct SafetyGate(Arc<AtomicBool>);

impl SafetyGate {
    pub fn new_open() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, open: bool) {
        self.0.store(open, Ordering::Release);
    }
}

#[derive(Clone, Default)]
pub struct ModelControl {
    rate_limited: Arc<AtomicBool>,
}

impl ModelControl {
    pub fn set_rate_limited(&self, limited: bool) {
        self.rate_limited.store(limited, Ordering::Release);
    }

    pub fn is_rate_limited(&self) -> bool {
        self.rate_limited.load(Ordering::Acquire)
    }
}

pub enum BudgetMsg {
    Charge { tokens: u64, reply: Reply<bool> },
    Status(Reply<BudgetStatus>),
    SetCap { cap: u64, reply: Reply<()> },
    Reset { cap: u64, reply: Reply<()> },
    Crash,
}

#[derive(Clone, Copy, Debug)]
pub struct BudgetStatus {
    pub spent: u64,
    pub cap: u64,
    pub exceeded: bool,
}

pub struct Budget {
    spent: Arc<AtomicU64>,
    cap: Arc<AtomicU64>,
    exceeded: Arc<AtomicBool>,
    guard: ActorRef<GuardMsg>,
    evidence: EvidenceTx,
}

impl Budget {
    pub fn new(
        spent: Arc<AtomicU64>,
        cap: Arc<AtomicU64>,
        exceeded: Arc<AtomicBool>,
        guard: ActorRef<GuardMsg>,
        evidence: EvidenceTx,
    ) -> Self {
        Self {
            spent,
            cap,
            exceeded,
            guard,
            evidence,
        }
    }
}

impl Actor for Budget {
    type Msg = BudgetMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            BudgetMsg::Charge { tokens, reply } => {
                let spent = self.spent.load(Ordering::Acquire);
                let allowed = spent.saturating_add(tokens) <= self.cap.load(Ordering::Acquire);
                if allowed {
                    self.spent.fetch_add(tokens, Ordering::AcqRel);
                } else {
                    self.exceeded.store(true, Ordering::Release);
                    let _ = self.guard.try_send(GuardMsg::BudgetExceeded);
                }
                reply.send(allowed);
            }
            BudgetMsg::Status(reply) => reply.send(BudgetStatus {
                spent: self.spent.load(Ordering::Acquire),
                cap: self.cap.load(Ordering::Acquire),
                exceeded: self.exceeded.load(Ordering::Acquire),
            }),
            BudgetMsg::SetCap { cap, reply } => {
                self.cap.store(cap, Ordering::Release);
                reply.send(());
            }
            BudgetMsg::Reset { cap, reply } => {
                self.spent.store(0, Ordering::Release);
                self.cap.store(cap, Ordering::Release);
                self.exceeded.store(false, Ordering::Release);
                let _ = self.guard.try_send(GuardMsg::BudgetRestored);
                reply.send(());
            }
            BudgetMsg::Crash => panic!("scripted budget crash"),
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("budget"));
        Ok(())
    }
}

pub enum GuardMsg {
    Failure(String),
    BudgetExceeded,
    BudgetRestored,
    Probe,
    ProbeResult {
        healthy: bool,
        budget_exceeded: bool,
    },
}

pub struct GuardActor {
    budget: ActorRef<BudgetMsg>,
    router: ActorRef<RouterMsg>,
    gate: SafetyGate,
    notices: broadcast::Sender<GateNotice>,
    model: ModelControl,
    failures: VecDeque<Instant>,
    probe_backoff: Duration,
    evidence: EvidenceTx,
}

impl GuardActor {
    pub fn new(
        budget: ActorRef<BudgetMsg>,
        router: ActorRef<RouterMsg>,
        gate: SafetyGate,
        notices: broadcast::Sender<GateNotice>,
        model: ModelControl,
        evidence: EvidenceTx,
    ) -> Self {
        Self {
            budget,
            router,
            gate,
            notices,
            model,
            failures: VecDeque::new(),
            probe_backoff: Duration::from_millis(20),
            evidence,
        }
    }

    fn announce(&self, open: bool, reason: impl Into<String>) {
        let reason = reason.into();
        self.gate.set(open);
        let notice = GateNotice {
            open,
            reason: reason.clone(),
        };
        let _ = self.notices.send(notice.clone());
        let _ = self.router.try_send(RouterMsg::GateChanged(notice));
        self.evidence.emit(Evidence::GateChanged { open, reason });
    }

    fn trip(&mut self, reason: impl Into<String>, ctx: &mut Context<'_, Self>) {
        if self.gate.is_open() {
            self.announce(false, reason);
        }
        ctx.set_timeout(PROBE, GuardMsg::Probe, self.probe_backoff);
    }
}

impl Actor for GuardActor {
    type Msg = GuardMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.gate.set(true);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            GuardMsg::Failure(reason) => {
                let now = Instant::now();
                self.failures.push_back(now);
                while self
                    .failures
                    .front()
                    .is_some_and(|at| now.duration_since(*at) > FAILURE_WINDOW)
                {
                    self.failures.pop_front();
                }
                if self.failures.len() >= FAILURE_LIMIT {
                    self.trip(format!("failure window: {reason}"), ctx);
                }
            }
            GuardMsg::BudgetExceeded => self.trip("budget cap exceeded", ctx),
            GuardMsg::BudgetRestored => {
                if !self.gate.is_open() {
                    ctx.set_timeout(PROBE, GuardMsg::Probe, Duration::from_millis(1));
                }
            }
            GuardMsg::Probe => {
                let budget = self.budget.clone();
                let model_available = !self.model.is_rate_limited();
                ctx.offload(
                    CALL_BOUND,
                    async move {
                        budget
                            .call(BudgetMsg::Status, CALL_BOUND)
                            .await
                            .map(|status| {
                                (
                                    status.spent <= status.cap
                                        && !status.exceeded
                                        && model_available,
                                    status.exceeded,
                                )
                            })
                            .unwrap_or((false, false))
                    },
                    |result| {
                        let (healthy, budget_exceeded) = result.unwrap_or((false, false));
                        GuardMsg::ProbeResult {
                            healthy,
                            budget_exceeded,
                        }
                    },
                );
            }
            GuardMsg::ProbeResult {
                healthy,
                budget_exceeded,
            } => {
                self.evidence.emit(Evidence::SafetyProbe {
                    healthy,
                    budget_exceeded,
                });
                if healthy {
                    self.failures.clear();
                    self.probe_backoff = Duration::from_millis(20);
                    if !self.gate.is_open() {
                        self.announce(true, "probe succeeded");
                    }
                } else {
                    self.probe_backoff = (self.probe_backoff * 2).min(Duration::from_millis(160));
                    ctx.set_timeout(PROBE, GuardMsg::Probe, self.probe_backoff);
                }
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("guard"));
        Ok(())
    }
}
