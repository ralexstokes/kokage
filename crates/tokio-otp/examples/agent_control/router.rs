//! Session router: the single writer for dynamic session membership.

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio_otp::{
    Actor, ActorContext, ActorRef, ActorResult, ControlError, GraphBuilder, Runtime, RuntimeHandle,
    StartMode, Strategy, prelude::Continue,
};

use crate::{
    messages::{
        BudgetMsg, ChatId, GuardMsg, JournalMsg, OutboundMsg, PHASE_TIMEOUT, PendingInput,
        ProgressMsg, Proof, RouterMsg, SessionMsg, ToolHostMsg,
    },
    model::ModelClient,
    session::SessionFactory,
};

// The supervisor processes control commands serially and a cooperative
// removal drains the departing subtree before the command completes, so an
// awaited add_subtree — even for a distinct id — can queue behind a drain
// whose progress needs this router to keep consuming bounced messages. The
// router therefore never awaits the control plane: both transitions run as
// pipelined steps, and the slot buffers traffic until the step's completion
// message arrives.
enum SessionSlot {
    /// add_subtree is in flight; `evict` records a retirement request that
    /// arrived before the mount completed.
    Mounting {
        actor: ActorRef<SessionMsg>,
        subtree_id: String,
        buffered: Vec<PendingInput>,
        evict: bool,
    },
    Active {
        actor: ActorRef<SessionMsg>,
        subtree_id: String,
    },
    /// remove_child is in flight; raced messages wait here until `Reaped`
    /// confirms the predecessor is gone, then ride into the replacement.
    Removing { buffered: Vec<PendingInput> },
}

pub struct Router {
    /// Filled by `main` after the runtime spawns; unlike router state, the
    /// cell is owned by the factory closure and survives router restarts.
    mount: Arc<OnceLock<RuntimeHandle>>,
    sessions: HashMap<ChatId, SessionSlot>,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    guard: ActorRef<GuardMsg>,
    outbound: ActorRef<OutboundMsg>,
    progress: ActorRef<ProgressMsg>,
    gate: Arc<AtomicBool>,
    model: Arc<dyn ModelClient>,
    task_sequence: Arc<AtomicU64>,
    /// Also factory-owned: the id allocator must survive router incarnations,
    /// or a reborn router would re-mint `session:<chat>#1` while its
    /// predecessor's subtree still exists.
    session_epoch: Arc<AtomicU64>,
    proof: Proof,
}

impl Router {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mount: Arc<OnceLock<RuntimeHandle>>,
        session_epoch: Arc<AtomicU64>,
        journal: ActorRef<JournalMsg>,
        budget: ActorRef<BudgetMsg>,
        tool_host: ActorRef<ToolHostMsg>,
        guard: ActorRef<GuardMsg>,
        outbound: ActorRef<OutboundMsg>,
        progress: ActorRef<ProgressMsg>,
        gate: Arc<AtomicBool>,
        model: Arc<dyn ModelClient>,
        proof: Proof,
    ) -> Self {
        Self {
            mount,
            sessions: HashMap::new(),
            journal,
            budget,
            tool_host,
            guard,
            outbound,
            progress,
            gate,
            model,
            task_sequence: Arc::new(AtomicU64::new(0)),
            session_epoch,
            proof,
        }
    }

    fn mount(&self) -> RuntimeHandle {
        self.mount
            .get()
            .expect("sessions mount bound before chat traffic")
            .clone()
    }

    /// Mints a fresh incarnation for `chat`: a `Mounting` slot routing into
    /// the pre-built stable mailbox, and a pipelined `add_subtree`.
    ///
    /// Each incarnation gets a subtree id no other incarnation ever uses (the
    /// allocator outlives this router), so a replacement never contends with
    /// a predecessor whose removal is still draining, and an `Evict` naming
    /// an id can never be misread as targeting a successor.
    fn mint(&mut self, chat: ChatId, buffered: Vec<PendingInput>, ctx: &ActorContext<RouterMsg>) {
        let generation = self.session_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let subtree_id = format!("session:{chat}#{generation}");
        // The session spawns its run children into its own subtree, but that
        // handle only exists once the mount completes; the cell is filled by
        // the step below, before any traffic is forwarded.
        let subtree_cell = Arc::new(OnceLock::new());
        let mut graph = GraphBuilder::new();
        let actor = graph.actor(
            "session",
            SessionFactory {
                chat,
                generation,
                subtree_id: subtree_id.clone(),
                journal: self.journal.clone(),
                budget: self.budget.clone(),
                tool_host: self.tool_host.clone(),
                guard: self.guard.clone(),
                outbound: self.outbound.clone(),
                progress: self.progress.clone(),
                router: ctx.myself(),
                subtree: subtree_cell.clone(),
                gate: self.gate.clone(),
                model: self.model.clone(),
                task_sequence: self.task_sequence.clone(),
                proof: self.proof.clone(),
            },
        );
        let graph = graph.build().expect("session graph builds");
        let mount = self.mount();
        let step_id = subtree_id.clone();
        ctx.step_or(
            PHASE_TIMEOUT,
            async move {
                // OneForAll: a session panic tears its transient runs down
                // with it; the session is reborn from this builder and
                // rehydrates from the journal, while `Never` run children are
                // skipped by the group respawn and cannot themselves recycle
                // the session.
                let subtree = mount
                    .add_subtree(
                        step_id,
                        Runtime::builder()
                            .graph(graph)
                            .strategy(Strategy::OneForAll)
                            .start_mode(StartMode::Sequential),
                    )
                    .await;
                match subtree {
                    Ok(subtree) => subtree_cell.set(subtree).is_ok(),
                    Err(_) => false,
                }
            },
            false,
            move |ok| RouterMsg::Mounted { chat, ok },
        );
        self.sessions.insert(
            chat,
            SessionSlot::Mounting {
                actor,
                subtree_id,
                buffered,
                evict: false,
            },
        );
    }

    fn pipeline_remove(&self, chat: ChatId, subtree_id: String, ctx: &ActorContext<RouterMsg>) {
        let mount = self.mount();
        let remove_id = subtree_id.clone();
        ctx.step_or(
            PHASE_TIMEOUT,
            async move {
                matches!(
                    mount.remove_child(remove_id).await,
                    Ok(())
                        | Err(ControlError::UnknownChildId(_))
                        | Err(ControlError::ShutdownTimedOut(_))
                )
            },
            false,
            move |done| RouterMsg::Reaped {
                chat,
                subtree_id,
                done,
            },
        );
    }

    /// Removes a subtree that no live slot routes to: an orphan minted by a
    /// previous router incarnation, or a stale duplicate retirement request.
    fn pipeline_sweep(&self, subtree_id: String, ctx: &ActorContext<RouterMsg>) {
        let mount = self.mount();
        let remove_id = subtree_id.clone();
        ctx.step_or(
            PHASE_TIMEOUT,
            async move {
                matches!(
                    mount.remove_child(remove_id).await,
                    Ok(())
                        | Err(ControlError::UnknownChildId(_))
                        | Err(ControlError::ShutdownTimedOut(_))
                        | Err(ControlError::ChildRemovalInProgress(_))
                )
            },
            false,
            move |done| RouterMsg::Swept { subtree_id, done },
        );
    }

    async fn forward(
        actor: &ActorRef<SessionMsg>,
        input: PendingInput,
    ) -> Result<(), tokio_otp::SendError> {
        actor
            .send(SessionMsg::UserMessage {
                envelope: input.envelope,
                text: input.text,
            })
            .await
    }
}

impl Actor for Router {
    type Msg = RouterMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            RouterMsg::UserMessage {
                envelope,
                chat,
                text,
            } => {
                let input = PendingInput { envelope, text };
                match self.sessions.get_mut(chat) {
                    Some(
                        SessionSlot::Mounting { buffered, .. } | SessionSlot::Removing { buffered },
                    ) => buffered.push(input),
                    Some(SessionSlot::Active { actor, .. }) => {
                        let actor = actor.clone();
                        Self::forward(&actor, input).await?;
                    }
                    None => self.mint(chat, vec![input], ctx),
                }
            }
            RouterMsg::Evict { chat, subtree_id } => {
                // Retirement names the subtree it retires, so it can only be
                // honored against the incarnation that sent it. Anything else
                // — an orphan of a previous router incarnation re-requesting,
                // or a duplicate from a reborn session — is retired by name
                // without touching live routing state.
                match self.sessions.get_mut(chat) {
                    Some(SessionSlot::Active {
                        subtree_id: live, ..
                    }) if *live == subtree_id => {
                        self.sessions.insert(
                            chat,
                            SessionSlot::Removing {
                                buffered: Vec::new(),
                            },
                        );
                        self.pipeline_remove(chat, subtree_id, ctx);
                    }
                    Some(SessionSlot::Mounting {
                        subtree_id: live,
                        evict,
                        ..
                    }) if *live == subtree_id => *evict = true,
                    Some(SessionSlot::Removing { .. }) => {}
                    _ => self.pipeline_sweep(subtree_id, ctx),
                }
            }
            RouterMsg::Mounted { chat, ok } => {
                if !ok {
                    // The mount step failed or deadlined; restarting the
                    // router is the example's recovery: buffered traffic is
                    // lost with this incarnation, and any half-mounted
                    // subtree retires itself through the Evict-by-id path.
                    return Err("session subtree mount failed".into());
                }
                let Some(SessionSlot::Mounting {
                    actor,
                    subtree_id,
                    buffered,
                    evict,
                }) = self.sessions.remove(chat)
                else {
                    // Single-writer discipline makes this unreachable; stay
                    // inert rather than guess at membership.
                    return Ok(Continue);
                };
                if evict {
                    // Retirement was requested while the mount was in
                    // flight; honor it now and let the buffer ride into the
                    // next incarnation.
                    self.sessions
                        .insert(chat, SessionSlot::Removing { buffered });
                    self.pipeline_remove(chat, subtree_id, ctx);
                } else {
                    self.sessions.insert(
                        chat,
                        SessionSlot::Active {
                            actor: actor.clone(),
                            subtree_id,
                        },
                    );
                    for input in buffered {
                        Self::forward(&actor, input).await?;
                    }
                }
            }
            RouterMsg::Reaped {
                chat,
                subtree_id,
                done,
            } => {
                if !done {
                    self.pipeline_remove(chat, subtree_id, ctx);
                } else if let Some(SessionSlot::Removing { buffered }) = self.sessions.remove(chat)
                    && !buffered.is_empty()
                {
                    self.mint(chat, buffered, ctx);
                }
            }
            RouterMsg::Swept { subtree_id, done } => {
                if !done {
                    self.pipeline_sweep(subtree_id, ctx);
                }
            }
            RouterMsg::PauseChanged { paused } => {
                for slot in self.sessions.values() {
                    if let SessionSlot::Mounting { actor, .. } | SessionSlot::Active { actor, .. } =
                        slot
                    {
                        let _ = actor.send(SessionMsg::PauseChanged { paused }).await;
                    }
                }
            }
            RouterMsg::Stop { chat } => {
                // Only a running incarnation has work to cancel; sending into
                // a Mounting slot's mailbox would also overtake its buffered
                // replay.
                if let Some(SessionSlot::Active { actor, .. }) = self.sessions.get(chat) {
                    actor.send(SessionMsg::Stop).await?;
                }
            }
        }
        Ok(Continue)
    }
}
