//! Dynamic conversation orchestrator and owner of transient role-run children.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use kokage::{
    Actor, ActorRef, ActorSpec, CancellationToken, Context, ExitResult, Guard, RestartMode,
    TimerKey,
};
use tokio::time::Instant;

use crate::{
    messages::{
        BudgetMsg, ChatId, GuardMsg, IDLE_TIMEOUT, JournalEntry, JournalMsg, OutboundMsg,
        PHASE_TIMEOUT, PendingInput, ProgressMsg, Proof, Role, RouterMsg, RunOutput, SessionMsg,
        TYPING_PERIOD, TaskId, ToolHostMsg,
    },
    model::ModelClient,
    run::AgentRunFactory,
};

const IDLE_SWEEP_TIMER: TimerKey = TimerKey::new("idle-sweep");

struct ActiveRun {
    task: TaskId,
    role: Role,
    attempt: u64,
    input: PendingInput,
    cancel: CancellationToken,
    failure_reported: bool,
    retry_after_removal: bool,
}

#[derive(kokage::ActorFactory)]
pub struct Session {
    chat: ChatId,
    generation: u64,
    subtree_id: String,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    guard: ActorRef<GuardMsg>,
    outbound: ActorRef<OutboundMsg>,
    progress: ActorRef<ProgressMsg>,
    router: ActorRef<RouterMsg>,
    gate: Arc<AtomicBool>,
    model: Arc<dyn ModelClient>,
    task_sequence: Arc<AtomicU64>,
    proof: Proof,
    #[factory(default)]
    transcript_len: usize,
    #[factory(default)]
    pending: VecDeque<PendingInput>,
    #[factory(default)]
    active: Option<ActiveRun>,
    #[factory(default)]
    heartbeat: Option<Guard>,
    #[factory(default)]
    evict_requested: bool,
}

impl Session {
    async fn append(&self, entry: JournalEntry) -> ExitResult {
        self.journal
            .call(PHASE_TIMEOUT, |reply| JournalMsg::Append {
                chat: self.chat,
                entry,
                reply,
            })
            .await?;
        Ok(())
    }

    fn arm_idle(&mut self, ctx: &mut Context<'_, Self>) {
        ctx.set_timeout(IDLE_SWEEP_TIMER, SessionMsg::IdleSweep, IDLE_TIMEOUT);
    }

    async fn start_run(
        &mut self,
        task: TaskId,
        role: Role,
        attempt: u64,
        input: PendingInput,
        ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        ctx.clear_timeout(IDLE_SWEEP_TIMER);
        if self.heartbeat.is_none() {
            self.heartbeat = Some(ctx.interval_to(
                &self.progress,
                ProgressMsg::Typing { chat: self.chat },
                TYPING_PERIOD,
            ));
        }
        let cancel = CancellationToken::new();
        let role_name = match role {
            Role::Planner => "planner",
            Role::Engineer => "engineer",
            Role::Reviewer => "reviewer",
        };
        let id = format!("run:{task}:{role_name}:{attempt}");
        let children = ctx
            .scope()
            .subtree("children")
            .ok_or("session leader is missing its declared child scope")?;
        let run_ref = children
            .add_actor(
                ActorSpec::new(
                    id.clone(),
                    AgentRunFactory {
                        chat: self.chat,
                        task,
                        role,
                        attempt,
                        user_text: input.text.clone(),
                        model: self.model.clone(),
                        journal: self.journal.clone(),
                        budget: self.budget.clone(),
                        tool_host: self.tool_host.clone(),
                        progress: self.progress.clone(),
                        session: ctx.myself(),
                        cancel: cancel.clone(),
                    },
                )
                .restart(RestartMode::Never),
            )
            .await?;
        ctx.watch(&run_ref, move |event| SessionMsg::RunEvent {
            task,
            role,
            event,
        })
        .detach();
        self.active = Some(ActiveRun {
            task,
            role,
            attempt,
            input,
            cancel,
            failure_reported: false,
            retry_after_removal: false,
        });
        *self
            .proof
            .lock()
            .expect("proof lock poisoned")
            .run_started
            .entry(self.chat)
            .or_default() += 1;
        Ok(())
    }

    async fn start_input(
        &mut self,
        input: PendingInput,
        ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        let task = self.task_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.start_run(task, Role::Planner, 0, input, ctx).await
    }

    async fn complete_task(
        &mut self,
        task: TaskId,
        approved: bool,
        ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        let text = format!(
            "task {task} complete (approved={approved}, prior-context={})",
            self.transcript_len.saturating_sub(1)
        );
        self.append(JournalEntry::Reply {
            task,
            text: text.clone(),
        })
        .await?;
        self.outbound
            .send(OutboundMsg::Reply {
                chat: self.chat,
                text,
            })
            .await?;
        self.heartbeat = None;
        self.proof
            .lock()
            .expect("proof lock poisoned")
            .run_terminal_at
            .insert(self.chat, Instant::now());
        self.active = None;
        self.arm_idle(ctx);
        Ok(())
    }
}

impl Actor for Session {
    type Msg = SessionMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        let mut proof = self.proof.lock().expect("proof lock poisoned");
        proof.session_ready_at.insert(self.chat, Instant::now());
        proof.session_generations.insert(self.chat, self.generation);
        drop(proof);
        ctx.continue_with(SessionMsg::Rehydrate);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SessionMsg::Rehydrate => {
                let replay = self
                    .journal
                    .call(PHASE_TIMEOUT, |reply| JournalMsg::Replay {
                        chat: self.chat,
                        reply,
                    })
                    .await?;
                self.transcript_len = replay
                    .iter()
                    .filter(|entry| matches!(entry.entry, JournalEntry::UserMessage { .. }))
                    .count();
                self.proof
                    .lock()
                    .expect("proof lock poisoned")
                    .session_rehydrated_at
                    .insert(self.chat, Instant::now());
                self.arm_idle(ctx);
            }
            SessionMsg::UserMessage { envelope, text } => {
                if self.evict_requested {
                    // This incarnation has already asked the router to drop
                    // its subtree; accepting new work here would lose it when
                    // removal lands. Bounce the message back to the router:
                    // our Evict was enqueued there before this bounce can be,
                    // so the router routes it to a fresh replacement subtree
                    // rather than forwarding it back to us.
                    self.router
                        .send(RouterMsg::UserMessage {
                            envelope,
                            chat: self.chat,
                            text,
                        })
                        .await?;
                    return Ok(());
                }
                self.transcript_len += 1;
                ctx.clear_timeout(IDLE_SWEEP_TIMER);
                let input = PendingInput { envelope, text };
                if self.active.is_some() || !self.gate.load(Ordering::Acquire) {
                    self.pending.push_back(input);
                    if !self.gate.load(Ordering::Acquire) {
                        self.outbound
                            .send(OutboundMsg::Notice {
                                chat: self.chat,
                                text: "agent control is paused; task journaled".into(),
                            })
                            .await?;
                    }
                } else {
                    self.start_input(input, ctx).await?;
                }
            }
            SessionMsg::RunFinished { task, role, output } => {
                let Some(active) = self.active.as_mut() else {
                    return Ok(());
                };
                if active.task != task || active.role != role {
                    return Ok(());
                }
                match output {
                    RunOutput::Planned(plan) => {
                        tracing::debug!(chat = self.chat, task, %plan, "planner completed");
                        let input = active.input.clone();
                        self.active = None;
                        self.start_run(task, Role::Engineer, 0, input, ctx).await?;
                    }
                    RunOutput::Engineered(output) => {
                        tracing::debug!(chat = self.chat, task, %output, "engineer completed");
                        let input = active.input.clone();
                        self.active = None;
                        self.start_run(task, Role::Reviewer, 0, input, ctx).await?;
                    }
                    RunOutput::Reviewed(approved) => {
                        self.complete_task(task, approved, ctx).await?;
                        if self.gate.load(Ordering::Acquire)
                            && let Some(input) = self.pending.pop_front()
                        {
                            self.start_input(input, ctx).await?;
                        }
                    }
                    RunOutput::RetryableFailure => {
                        if !active.failure_reported {
                            active.failure_reported = true;
                            self.guard
                                .send(GuardMsg::RunFailureObserved {
                                    chat: self.chat,
                                    task,
                                })
                                .await?;
                        }
                        active.retry_after_removal = true;
                    }
                    RunOutput::Cancelled => {
                        self.active = None;
                        self.heartbeat = None;
                        self.proof
                            .lock()
                            .expect("proof lock poisoned")
                            .run_terminal_at
                            .insert(self.chat, Instant::now());
                        self.arm_idle(ctx);
                    }
                }
            }
            SessionMsg::RunEvent { task, role, event } => {
                self.proof
                    .lock()
                    .expect("proof lock poisoned")
                    .monitor_events
                    .entry(task)
                    .or_default()
                    .push(event.clone());
                if let kokage::MonitorEvent::Exited { status, .. } = &event
                    && status.is_failure()
                {
                    if let Some(active) = self.active.as_mut()
                        && active.task == task
                        && active.role == role
                    {
                        if !active.failure_reported {
                            active.failure_reported = true;
                            self.guard
                                .send(GuardMsg::RunFailureObserved {
                                    chat: self.chat,
                                    task,
                                })
                                .await?;
                        }
                        active.retry_after_removal = true;
                    }
                } else if matches!(event, kokage::MonitorEvent::Removed { .. })
                    && let Some(active) = self.active.take()
                {
                    if active.task != task || active.role != role {
                        self.active = Some(active);
                    } else if active.retry_after_removal && self.gate.load(Ordering::Acquire) {
                        self.start_run(
                            active.task,
                            active.role,
                            active.attempt + 1,
                            active.input,
                            ctx,
                        )
                        .await?;
                    } else if active.retry_after_removal {
                        self.pending.push_front(active.input);
                        self.heartbeat = None;
                    }
                }
            }
            SessionMsg::PauseChanged { paused } => {
                if paused {
                    self.outbound
                        .send(OutboundMsg::Notice {
                            chat: self.chat,
                            text: "agent control paused".into(),
                        })
                        .await?;
                    if let Some(active) = &self.active {
                        self.pending.push_front(active.input.clone());
                        active.cancel.cancel();
                    }
                } else if self.active.is_none()
                    && let Some(input) = self.pending.pop_front()
                {
                    self.start_input(input, ctx).await?;
                }
            }
            SessionMsg::Stop => {
                if let Some(active) = &self.active {
                    active.cancel.cancel();
                    self.append(JournalEntry::TaskCancelled { task: active.task })
                        .await?;
                }
            }
            SessionMsg::IdleSweep => {
                if self.evict_requested {
                    // Retirement is a request, not a handshake. If the
                    // membership writer restarted and lost it, this
                    // incarnation would otherwise idle unowned forever, so
                    // re-send until teardown lands — idempotent, because the
                    // router retires by subtree id.
                    self.router
                        .send(RouterMsg::Evict {
                            chat: self.chat,
                            subtree_id: self.subtree_id.clone(),
                        })
                        .await?;
                    self.arm_idle(ctx);
                } else if self.active.is_none() {
                    let task = self.task_sequence.load(Ordering::Relaxed);
                    self.append(JournalEntry::Checkpoint {
                        task,
                        state: format!("{} transcript item(s)", self.transcript_len),
                    })
                    .await?;
                    self.append(JournalEntry::Evicted).await?;
                    self.router
                        .send(RouterMsg::Evict {
                            chat: self.chat,
                            subtree_id: self.subtree_id.clone(),
                        })
                        .await?;
                    self.evict_requested = true;
                    self.arm_idle(ctx);
                }
            }
        }
        Ok(())
    }
}
