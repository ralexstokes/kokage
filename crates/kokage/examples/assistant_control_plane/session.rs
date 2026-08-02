use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorSpec, CallError, Context, DynamicScopeRef, ExitResult, MonitorEvent,
    MonitorEventKind, RestartPolicy, StopContext, TimerKey,
};

use crate::{
    common::{
        CALL_BOUND, Envelope, Evidence, EvidenceTx, JournalEntry, MODEL_BOUND, Stage, TOOL_BOUND,
    },
    gateway::{OutboundMsg, ProgressMsg},
    journal::JournalMsg,
    router::RouterMsg,
    safety::{BudgetMsg, GateNotice, GuardMsg, ModelControl, SafetyGate},
    tool::ToolMsg,
};

const IDLE: TimerKey = TimerKey::new("session-idle");
const RETRY: TimerKey = TimerKey::new("session-retry");
const IDLE_AFTER: Duration = Duration::from_millis(35);
const RETRY_BASE: Duration = Duration::from_millis(5);
const MAX_RUN_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct ScriptedModel {
    control: ModelControl,
    once: Arc<Mutex<MagicTriggers>>,
}

type MagicTriggers = HashSet<(u64, Stage, &'static str)>;

#[derive(Clone, Debug)]
pub(crate) struct ModelTurn {
    tokens: u64,
    panic_actor: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum ModelFailure {
    RateLimited,
}

impl ScriptedModel {
    pub fn new(control: ModelControl) -> Self {
        Self {
            control,
            once: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    async fn turn(
        &self,
        envelope: &Envelope,
        attempt: u32,
        stage: Stage,
    ) -> Result<ModelTurn, ModelFailure> {
        tokio::task::yield_now().await;
        if self.control.is_rate_limited() {
            return Err(ModelFailure::RateLimited);
        }

        if envelope.text == "slow turn"
            && stage == Stage::Planner
            && self.take_once(envelope.id, stage, "slow")
        {
            tokio::time::sleep(Duration::from_millis(90)).await;
        }

        Ok(ModelTurn {
            tokens: stage.tokens(),
            panic_actor: envelope.text == "run panic" && stage == Stage::Engineer && attempt == 1,
        })
    }

    fn take_once(&self, envelope_id: u64, stage: Stage, kind: &'static str) -> bool {
        self.once
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((envelope_id, stage, kind))
    }
}

#[derive(Clone, Default)]
pub struct SessionSettings {
    idle_eviction: Arc<AtomicBool>,
    session_panics: Arc<Mutex<HashSet<u64>>>,
}

impl SessionSettings {
    pub fn enable_idle_eviction(&self, enabled: bool) {
        self.idle_eviction.store(enabled, Ordering::Release);
    }

    fn idle_eviction(&self) -> bool {
        self.idle_eviction.load(Ordering::Acquire)
    }

    fn first_session_panic(&self, envelope_id: u64) -> bool {
        self.session_panics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(envelope_id)
    }
}

#[derive(Clone)]
pub struct SessionDeps {
    pub journal: ActorRef<JournalMsg>,
    pub budget: ActorRef<BudgetMsg>,
    pub guard: ActorRef<GuardMsg>,
    pub tool: ActorRef<ToolMsg>,
    pub outbound: ActorRef<OutboundMsg>,
    pub progress: ActorRef<ProgressMsg>,
    pub router: ActorRef<RouterMsg>,
    pub gate: SafetyGate,
    pub model: ScriptedModel,
    pub settings: SessionSettings,
    pub evidence: EvidenceTx,
}

#[derive(Clone)]
struct RunDeps {
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    guard: ActorRef<GuardMsg>,
    tool: ActorRef<ToolMsg>,
    outbound: ActorRef<OutboundMsg>,
    progress: ActorRef<ProgressMsg>,
    session: ActorRef<SessionMsg>,
    model: ScriptedModel,
    evidence: EvidenceTx,
}

pub enum RunMsg {
    Begin,
    ModelDone {
        stage: Stage,
        result: Result<Result<ModelTurn, ModelFailure>, kokage::OffloadDeadline>,
    },
    ToolDone(Result<Result<String, CallError>, kokage::OffloadDeadline>),
    Reconciled(Result<Result<Option<String>, CallError>, kokage::OffloadDeadline>),
    StreamEnded,
}

struct RunActor {
    deps: RunDeps,
    envelope: Envelope,
    attempt: u32,
    run_id: String,
    tool_key: Option<String>,
}

impl RunActor {
    fn start_model(&self, stage: Stage, ctx: &mut Context<'_, Self>) {
        let model = self.deps.model.clone();
        let envelope = self.envelope.clone();
        let attempt = self.attempt;
        ctx.offload(
            MODEL_BOUND,
            async move { model.turn(&envelope, attempt, stage).await },
            move |result| RunMsg::ModelDone { stage, result },
        );
    }

    async fn append(&self, entry: JournalEntry) -> Result<(), kokage::BoxError> {
        self.deps
            .journal
            .call(|reply| JournalMsg::Append(entry, reply), CALL_BOUND)
            .await?;
        Ok(())
    }

    fn fail(&self, reason: impl Into<String>) -> kokage::BoxError {
        let reason = reason.into();
        let _ = self.deps.guard.try_send(GuardMsg::Failure(reason.clone()));
        self.deps.evidence.emit(Evidence::RunFailed {
            chat: self.envelope.chat.clone(),
            envelope_id: self.envelope.id,
            attempt: self.attempt,
            reason: reason.clone(),
        });
        let _ = self.deps.session.try_send(SessionMsg::RunFailed {
            run_id: self.run_id.clone(),
            reason: reason.clone(),
        });
        io::Error::other(reason).into()
    }

    async fn record_tool_result(
        &self,
        key: String,
        reconciled: bool,
    ) -> Result<(), kokage::BoxError> {
        self.append(JournalEntry::ToolResult {
            chat: self.envelope.chat.clone(),
            envelope_id: self.envelope.id,
            attempt: self.attempt,
            key,
            reconciled,
        })
        .await
    }
}

impl Actor for RunActor {
    type Msg = RunMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.deps.evidence.emit(Evidence::RunStarted {
            chat: self.envelope.chat.clone(),
            envelope_id: self.envelope.id,
            attempt: self.attempt,
            run_id: self.run_id.clone(),
        });
        ctx.continue_with(RunMsg::Begin);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RunMsg::Begin if self.envelope.text == "flood" => {
                let progress = self.deps.progress.clone();
                let envelope_id = self.envelope.id;
                let cancelled = ctx.shutdown_token().clone();
                ctx.offload(
                    Duration::from_secs(30),
                    async move {
                        let mut sequence = 0;
                        loop {
                            sequence += 1;
                            tokio::select! {
                                biased;
                                () = cancelled.cancelled() => break,
                                sent = progress.send(ProgressMsg { envelope_id, sequence }) => {
                                    if sent.is_err() {
                                        break;
                                    }
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    },
                    |_| RunMsg::StreamEnded,
                );
            }
            RunMsg::Begin => self.start_model(Stage::Planner, ctx),
            RunMsg::ModelDone { stage, result } => {
                let turn = match result {
                    Err(_) => return Err(self.fail(format!("{stage} model deadline"))),
                    Ok(Err(ModelFailure::RateLimited)) => {
                        return Err(self.fail("model rate limited"));
                    }
                    Ok(Ok(turn)) => turn,
                };

                if turn.panic_actor {
                    panic!("scripted mid-run panic");
                }

                let allowed = self
                    .deps
                    .budget
                    .call(
                        |reply| BudgetMsg::Charge {
                            tokens: turn.tokens,
                            reply,
                        },
                        CALL_BOUND,
                    )
                    .await?;
                if !allowed {
                    return Err(self.fail("token budget denied"));
                }
                self.append(JournalEntry::ModelTurn {
                    chat: self.envelope.chat.clone(),
                    envelope_id: self.envelope.id,
                    attempt: self.attempt,
                    stage,
                    tokens: turn.tokens,
                })
                .await?;

                match stage {
                    Stage::Planner => self.start_model(Stage::Engineer, ctx),
                    Stage::Engineer => {
                        let key = format!(
                            "{}:{}:{}",
                            self.envelope.chat, self.envelope.id, self.attempt
                        );
                        self.append(JournalEntry::ToolIntent {
                            chat: self.envelope.chat.clone(),
                            envelope_id: self.envelope.id,
                            attempt: self.attempt,
                            key: key.clone(),
                        })
                        .await?;
                        self.tool_key = Some(key.clone());
                        let tool = self.deps.tool.clone();
                        let stall = self.envelope.text == "stall tool";
                        ctx.offload(
                            TOOL_BOUND,
                            async move {
                                tool.call(
                                    |reply| ToolMsg::Execute { key, stall, reply },
                                    CALL_BOUND,
                                )
                                .await
                            },
                            RunMsg::ToolDone,
                        );
                    }
                    Stage::Reviewer => {
                        let text = format!("completed {}", self.envelope.text);
                        self.append(JournalEntry::Assistant {
                            chat: self.envelope.chat.clone(),
                            envelope_id: self.envelope.id,
                            attempt: self.attempt,
                            text: text.clone(),
                        })
                        .await?;
                        self.deps
                            .outbound
                            .send(OutboundMsg::Assistant {
                                envelope_id: self.envelope.id,
                                text,
                            })
                            .await?;
                        self.deps.evidence.emit(Evidence::RunCompleted {
                            chat: self.envelope.chat.clone(),
                            envelope_id: self.envelope.id,
                            attempt: self.attempt,
                        });
                        // Completion is advisory: during a OneForAll drain the
                        // orchestrator is intentionally unbound, so awaiting
                        // it here would hold the sibling run scope open.
                        let _ = self.deps.session.try_send(SessionMsg::RunSucceeded {
                            run_id: self.run_id.clone(),
                        });
                        ctx.stop();
                    }
                }
            }
            RunMsg::ToolDone(Ok(Ok(_result))) => {
                let key = self.tool_key.clone().expect("tool intent precedes result");
                self.record_tool_result(key, false).await?;
                self.start_model(Stage::Reviewer, ctx);
            }
            RunMsg::ToolDone(Err(_)) | RunMsg::ToolDone(Ok(Err(_))) => {
                let key = self.tool_key.clone().expect("tool intent precedes timeout");
                let tool = self.deps.tool.clone();
                ctx.offload(
                    CALL_BOUND,
                    async move {
                        tool.call(|reply| ToolMsg::Query { key, reply }, CALL_BOUND)
                            .await
                    },
                    RunMsg::Reconciled,
                );
            }
            RunMsg::Reconciled(Ok(Ok(Some(_result)))) => {
                let key = self.tool_key.clone().expect("tool intent precedes query");
                self.record_tool_result(key.clone(), true).await?;
                self.deps.evidence.emit(Evidence::ToolReconciled { key });
                self.start_model(Stage::Reviewer, ctx);
            }
            RunMsg::Reconciled(Ok(Ok(None)))
            | RunMsg::Reconciled(Ok(Err(_)))
            | RunMsg::Reconciled(Err(_)) => {
                return Err(self.fail("tool outcome could not be reconciled"));
            }
            RunMsg::StreamEnded => ctx.stop(),
        }
        Ok(())
    }
}

enum SessionPhase {
    Rehydrating,
    Ready,
    Evicting,
}

struct ActiveRun {
    envelope: Envelope,
    run_id: String,
    actor: Option<ActorRef<RunMsg>>,
    cancel: Option<Envelope>,
    cancelling: bool,
}

pub enum SessionMsg {
    Incoming(Envelope),
    GateChanged(GateNotice),
    RunsReady(Result<(), String>),
    Rehydrate,
    ReplayOne,
    RunMounted {
        envelope: Envelope,
        attempt: u32,
        run_id: String,
        result: Result<ActorRef<RunMsg>, String>,
    },
    RunLifecycle {
        run_id: String,
        event: MonitorEvent,
    },
    RunFailed {
        run_id: String,
        reason: String,
    },
    RunSucceeded {
        run_id: String,
    },
    Retry,
    Cancelled {
        run_id: String,
        command: Envelope,
        cancelled: Envelope,
        result: Result<(), String>,
    },
    Idle,
}

pub struct Session {
    chat: String,
    epoch: u64,
    runs: DynamicScopeRef,
    deps: SessionDeps,
    phase: SessionPhase,
    replay: VecDeque<JournalEntry>,
    replayed_incoming: Vec<Envelope>,
    completed: HashSet<u64>,
    seen: HashSet<u64>,
    pending: VecDeque<Envelope>,
    attempts: HashMap<u64, u32>,
    current: Option<ActiveRun>,
    retry_scheduled: bool,
    messages: usize,
}

impl Session {
    pub fn new(chat: String, epoch: u64, runs: DynamicScopeRef, deps: SessionDeps) -> Self {
        Self {
            chat,
            epoch,
            runs,
            deps,
            phase: SessionPhase::Rehydrating,
            replay: VecDeque::new(),
            replayed_incoming: Vec::new(),
            completed: HashSet::new(),
            seen: HashSet::new(),
            pending: VecDeque::new(),
            attempts: HashMap::new(),
            current: None,
            retry_scheduled: false,
            messages: 0,
        }
    }

    fn maybe_start(&mut self, ctx: &mut Context<'_, Self>) {
        if !matches!(self.phase, SessionPhase::Ready)
            || self.current.is_some()
            || self.retry_scheduled
        {
            return;
        }
        let Some(envelope) = self.pending.pop_front() else {
            return;
        };
        if !self.deps.gate.is_open() {
            self.deps.evidence.emit(Evidence::HeldWhilePaused {
                chat: self.chat.clone(),
                envelope_id: envelope.id,
            });
            self.pending.push_front(envelope);
            return;
        }

        let attempt = self.attempts.entry(envelope.id).or_default();
        *attempt += 1;
        let attempt = *attempt;
        let run_id = format!("run-{}-a{attempt}", envelope.id);
        let session = ctx.myself();
        let deps = RunDeps {
            journal: self.deps.journal.clone(),
            budget: self.deps.budget.clone(),
            guard: self.deps.guard.clone(),
            tool: self.deps.tool.clone(),
            outbound: self.deps.outbound.clone(),
            progress: self.deps.progress.clone(),
            session,
            model: self.deps.model.clone(),
            evidence: self.deps.evidence.clone(),
        };
        let actor_envelope = envelope.clone();
        let actor_run_id = run_id.clone();
        let spec = ActorSpec::new(run_id.clone(), move || RunActor {
            deps: deps.clone(),
            envelope: actor_envelope.clone(),
            attempt,
            run_id: actor_run_id.clone(),
            tool_key: None,
        })
        .restart(RestartPolicy::never())
        .remove_on_terminal_exit();
        let runs = self.runs.clone();
        let completion_envelope = envelope.clone();
        let completion_run_id = run_id.clone();
        ctx.offload(
            CALL_BOUND,
            async move {
                runs.add_actor_spec(spec)
                    .await
                    .map_err(|error| error.to_string())
            },
            move |result| SessionMsg::RunMounted {
                envelope: completion_envelope,
                attempt,
                run_id: completion_run_id,
                result: result
                    .map_err(|_| "run mount deadline".to_owned())
                    .and_then(|result| result),
            },
        );
        self.current = Some(ActiveRun {
            envelope,
            run_id,
            actor: None,
            cancel: None,
            cancelling: false,
        });
    }

    async fn retry_current(
        &mut self,
        run_id: &str,
        reason: &str,
        ctx: &mut Context<'_, Self>,
    ) -> Result<(), kokage::BoxError> {
        let Some(active) = self.current.take_if(|active| active.run_id == run_id) else {
            return Ok(());
        };
        let envelope_id = active.envelope.id;
        let attempts = self.attempts.get(&envelope_id).copied().unwrap_or_default();
        if attempts >= MAX_RUN_ATTEMPTS {
            self.append(JournalEntry::Assistant {
                chat: self.chat.clone(),
                envelope_id,
                attempt: attempts,
                text: format!("failed after {attempts} attempts: {reason}"),
            })
            .await?;
            self.completed.insert(envelope_id);
            self.attempts.remove(&envelope_id);
            if self.deps.settings.idle_eviction() && self.pending.is_empty() {
                ctx.set_timeout(IDLE, SessionMsg::Idle, IDLE_AFTER);
            }
            self.maybe_start(ctx);
            return Ok(());
        }
        self.pending.push_front(active.envelope);
        self.retry_scheduled = true;
        let multiplier = 1_u32 << attempts.saturating_sub(1);
        ctx.set_timeout(RETRY, SessionMsg::Retry, RETRY_BASE * multiplier);
        Ok(())
    }

    fn start_cancel(&mut self, ctx: &mut Context<'_, Self>) {
        let Some(active) = &mut self.current else {
            return;
        };
        if active.cancelling {
            return;
        }
        let (Some(actor), Some(command)) = (active.actor.clone(), active.cancel.clone()) else {
            return;
        };
        active.cancelling = true;
        let run_id = active.run_id.clone();
        let cancelled = active.envelope.clone();
        let runs = self.runs.clone();
        ctx.offload(
            CALL_BOUND,
            async move { runs.remove(&actor).await.map_err(|error| error.to_string()) },
            move |result| SessionMsg::Cancelled {
                run_id,
                command,
                cancelled,
                result: result
                    .map_err(|_| "run cancellation deadline".to_owned())
                    .and_then(|result| result),
            },
        );
    }

    async fn append(&self, entry: JournalEntry) -> Result<(), kokage::BoxError> {
        self.deps
            .journal
            .call(|reply| JournalMsg::Append(entry, reply), CALL_BOUND)
            .await?;
        Ok(())
    }
}

impl Actor for Session {
    type Msg = SessionMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let runs = self.runs.clone();
        ctx.offload(
            CALL_BOUND,
            async move { runs.wait_started().await.map_err(|error| error.to_string()) },
            |result| {
                SessionMsg::RunsReady(
                    result
                        .map_err(|_| "run scope readiness deadline".to_owned())
                        .and_then(|result| result),
                )
            },
        );
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SessionMsg::Incoming(envelope) => {
                ctx.clear_timer(IDLE);
                if !self.seen.insert(envelope.id) {
                    return Ok(());
                }
                self.messages += 1;
                if envelope.text == "cancel flood" {
                    if let Some(active) = &mut self.current
                        && active.cancel.is_none()
                    {
                        active.cancel = Some(envelope);
                        self.start_cancel(ctx);
                    }
                    return Ok(());
                }
                if !self.deps.gate.is_open() {
                    self.deps.evidence.emit(Evidence::HeldWhilePaused {
                        chat: self.chat.clone(),
                        envelope_id: envelope.id,
                    });
                    self.pending.push_back(envelope);
                    return Ok(());
                }
                self.pending.push_back(envelope);
                self.maybe_start(ctx);
            }
            SessionMsg::GateChanged(notice) => {
                debug_assert!(!notice.reason.is_empty());
                if notice.open {
                    self.maybe_start(ctx);
                }
            }
            SessionMsg::RunsReady(result) => {
                result.map_err(io::Error::other)?;
                ctx.continue_with(SessionMsg::Rehydrate);
            }
            SessionMsg::Rehydrate => {
                self.replay = self
                    .deps
                    .journal
                    .call(
                        |reply| JournalMsg::Replay {
                            chat: self.chat.clone(),
                            reply,
                        },
                        CALL_BOUND,
                    )
                    .await?
                    .into();
                ctx.continue_with(SessionMsg::ReplayOne);
            }
            SessionMsg::ReplayOne => {
                if let Some(entry) = self.replay.pop_front() {
                    match entry {
                        JournalEntry::Incoming {
                            envelope_id,
                            chat,
                            text,
                        } => {
                            if self.seen.insert(envelope_id) {
                                self.messages += 1;
                                self.replayed_incoming
                                    .push(Envelope::new(envelope_id, chat, text));
                            }
                        }
                        JournalEntry::Assistant { envelope_id, .. } => {
                            self.completed.insert(envelope_id);
                        }
                        _ => {}
                    }
                    ctx.continue_with(SessionMsg::ReplayOne);
                } else {
                    for envelope in self.replayed_incoming.drain(..) {
                        if !self.completed.contains(&envelope.id) && envelope.text != "cancel flood"
                        {
                            self.pending.push_back(envelope);
                        }
                    }
                    self.phase = SessionPhase::Ready;
                    self.deps.evidence.emit(Evidence::Rehydrated {
                        chat: self.chat.clone(),
                        epoch: self.epoch,
                        messages: self.messages,
                    });
                    self.maybe_start(ctx);
                }
            }
            SessionMsg::RunMounted {
                envelope,
                attempt,
                run_id,
                result,
            } => match result {
                Ok(actor) => {
                    if let Some(active) = &mut self.current
                        && active.run_id == run_id
                    {
                        active.actor = Some(actor.clone());
                    }
                    let watched_run = run_id.clone();
                    ctx.watch(&actor, move |event| SessionMsg::RunLifecycle {
                        run_id: watched_run.clone(),
                        event,
                    });
                    self.start_cancel(ctx);
                    let _ = (envelope, attempt);
                }
                Err(reason) => {
                    let cancelled_before_mount = self
                        .current
                        .as_ref()
                        .is_some_and(|active| active.run_id == run_id && active.cancel.is_some());
                    if cancelled_before_mount {
                        self.current = None;
                        self.attempts.remove(&envelope.id);
                        self.maybe_start(ctx);
                    } else {
                        self.deps.evidence.emit(Evidence::RunFailed {
                            chat: self.chat.clone(),
                            envelope_id: envelope.id,
                            attempt,
                            reason: reason.clone(),
                        });
                        self.retry_current(&run_id, &reason, ctx).await?;
                    }
                }
            },
            SessionMsg::RunLifecycle { run_id, event } => match event.kind {
                MonitorEventKind::Started { .. } => {
                    if self.current.as_ref().is_some_and(|active| {
                        active.run_id == run_id
                            && active.envelope.text == "session panic"
                            && self.deps.settings.first_session_panic(active.envelope.id)
                    }) {
                        panic!("scripted session panic with an active run");
                    }
                }
                MonitorEventKind::Exited { status, .. } if status.is_failure() => {
                    self.retry_current(&run_id, "run actor exited", ctx).await?;
                }
                _ => {}
            },
            SessionMsg::RunFailed { run_id, reason } => {
                self.retry_current(&run_id, &reason, ctx).await?;
            }
            SessionMsg::RunSucceeded { run_id } => {
                if let Some(active) = self.current.take_if(|active| active.run_id == run_id) {
                    self.attempts.remove(&active.envelope.id);
                }
                if self.deps.settings.idle_eviction() && self.pending.is_empty() {
                    ctx.set_timeout(IDLE, SessionMsg::Idle, IDLE_AFTER);
                }
                self.maybe_start(ctx);
            }
            SessionMsg::Retry => {
                self.retry_scheduled = false;
                self.maybe_start(ctx);
            }
            SessionMsg::Cancelled {
                run_id,
                command,
                cancelled,
                result,
            } => {
                result.map_err(io::Error::other)?;
                let Some(_active) = self.current.take_if(|active| active.run_id == run_id) else {
                    return Ok(());
                };
                self.attempts.remove(&cancelled.id);
                self.append(JournalEntry::Assistant {
                    chat: self.chat.clone(),
                    envelope_id: cancelled.id,
                    attempt: 0,
                    text: "cancelled active stream".to_owned(),
                })
                .await?;
                self.append(JournalEntry::Assistant {
                    chat: self.chat.clone(),
                    envelope_id: command.id,
                    attempt: 0,
                    text: "cancel command applied".to_owned(),
                })
                .await?;
                self.maybe_start(ctx);
            }
            SessionMsg::Idle => {
                if self.current.is_none()
                    && self.pending.is_empty()
                    && matches!(self.phase, SessionPhase::Ready)
                {
                    self.append(JournalEntry::Checkpoint {
                        chat: self.chat.clone(),
                        messages: self.messages,
                    })
                    .await?;
                    self.append(JournalEntry::Evicted {
                        chat: self.chat.clone(),
                        epoch: self.epoch,
                    })
                    .await?;
                    self.phase = SessionPhase::Evicting;
                    self.deps.evidence.emit(Evidence::EvictionRequested {
                        chat: self.chat.clone(),
                        epoch: self.epoch,
                    });
                    let _ = self.deps.router.try_send(RouterMsg::Evict {
                        chat: self.chat.clone(),
                        epoch: self.epoch,
                    });
                }
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.deps.evidence.emit(Evidence::ActorStopped("session"));
        Ok(())
    }
}
