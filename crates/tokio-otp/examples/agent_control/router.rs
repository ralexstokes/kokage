//! Session router: the single writer for dynamic session membership.

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64},
    },
};

use tokio_otp::{
    Actor, ActorContext, ActorRef, ActorResult, BoxError, ControlError, GraphBuilder, Runtime,
    RuntimeHandle, StartMode, Strategy, prelude::Continue,
};

use crate::{
    messages::{
        BudgetMsg, ChatId, GuardMsg, JournalMsg, OutboundMsg, PHASE_TIMEOUT, ProgressMsg, Proof,
        RouterMsg, SessionMsg, ToolHostMsg,
    },
    model::ModelClient,
    session::SessionFactory,
};

struct SessionEntry {
    actor: ActorRef<SessionMsg>,
    subtree_id: String,
}

pub struct Router {
    sessions_handle: Option<RuntimeHandle>,
    sessions: HashMap<ChatId, SessionEntry>,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    guard: ActorRef<GuardMsg>,
    outbound: ActorRef<OutboundMsg>,
    progress: ActorRef<ProgressMsg>,
    gate: Arc<AtomicBool>,
    model: Arc<dyn ModelClient>,
    task_sequence: Arc<AtomicU64>,
    session_epoch: u64,
    proof: Proof,
}

impl Router {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
            sessions_handle: None,
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
            session_epoch: 0,
            proof,
        }
    }

    async fn add_session(
        &mut self,
        chat: ChatId,
        ctx: &ActorContext<RouterMsg>,
    ) -> Result<ActorRef<SessionMsg>, BoxError> {
        let mount = self
            .sessions_handle
            .as_ref()
            .expect("router must be bound before chat traffic")
            .clone();
        // Each conversation incarnation is a distinct subtree with an id no
        // other incarnation ever uses. A respawn therefore never contends
        // with a predecessor's still-draining removal, which is what let the
        // generation-stamped eviction handshake be deleted.
        self.session_epoch += 1;
        let generation = self.session_epoch;
        let subtree_id = format!("session:{chat}#{generation}");
        // The session spawns its run children into its own subtree, but that
        // handle only exists once add_subtree returns; the cell is filled
        // before any traffic is forwarded.
        let subtree_cell = Arc::new(OnceLock::new());
        let mut graph = GraphBuilder::new();
        let actor = graph.actor(
            "session",
            SessionFactory {
                chat,
                generation,
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
        // OneForAll: a session panic tears down its transient runs with it;
        // the session is reborn from this builder and rehydrates from the
        // journal, while `Never` run children are skipped by the group
        // respawn and cannot themselves recycle the session.
        let subtree = mount
            .add_subtree(
                subtree_id.clone(),
                Runtime::builder()
                    .graph(graph.build()?)
                    .strategy(Strategy::OneForAll)
                    .start_mode(StartMode::Sequential),
            )
            .await?;
        assert!(subtree_cell.set(subtree).is_ok());
        self.sessions.insert(
            chat,
            SessionEntry {
                actor: actor.clone(),
                subtree_id,
            },
        );
        Ok(actor)
    }

    fn pipeline_remove(&self, subtree_id: String, ctx: &ActorContext<RouterMsg>) {
        let mount = self.sessions_handle.as_ref().expect("router bound").clone();
        let remove_id = subtree_id.clone();
        ctx.step(
            PHASE_TIMEOUT,
            // Removal is pipelined so an idle eviction never head-of-line
            // blocks unrelated routing work (the same hazard documented for
            // the trading example's order router). Nothing routes on its
            // completion: the chat's map entry is already gone and the next
            // message mints a fresh subtree id, so the retry below only
            // guards against leaking a stuck subtree.
            async move { mount.remove_child(remove_id).await },
            move |outcome| {
                let done = matches!(
                    outcome,
                    Ok(Ok(()))
                        | Ok(Err(ControlError::UnknownChildId(_)))
                        | Ok(Err(ControlError::ShutdownTimedOut(_)))
                );
                RouterMsg::Reaped { subtree_id, done }
            },
        );
    }
}

impl Actor for Router {
    type Msg = RouterMsg;

    async fn handle(&mut self, message: Self::Msg, ctx: &ActorContext<Self::Msg>) -> ActorResult {
        match message {
            RouterMsg::Bind { sessions } => self.sessions_handle = Some(sessions),
            RouterMsg::UserMessage {
                envelope,
                chat,
                text,
            } => {
                let actor = match self.sessions.get(chat) {
                    Some(entry) => entry.actor.clone(),
                    None => self.add_session(chat, ctx).await?,
                };
                actor
                    .send(SessionMsg::UserMessage { envelope, text })
                    .await?;
            }
            RouterMsg::Evict { chat } => {
                // No generation matching: an incarnation sends at most one
                // Evict, and a successor entry for the chat can only be
                // created after that Evict was consumed here, so this always
                // targets the incarnation that sent it. A message the router
                // forwarded before processing this arrives at the retiring
                // session, which bounces it back; per-sender FIFO puts the
                // bounce behind this Evict, so it lands in the None arm above
                // and mints the replacement subtree.
                if let Some(entry) = self.sessions.remove(chat) {
                    self.pipeline_remove(entry.subtree_id, ctx);
                }
            }
            RouterMsg::Reaped { subtree_id, done } => {
                if !done {
                    self.pipeline_remove(subtree_id, ctx);
                }
            }
            RouterMsg::PauseChanged { paused } => {
                for entry in self.sessions.values() {
                    let _ = entry.actor.send(SessionMsg::PauseChanged { paused }).await;
                }
            }
            RouterMsg::Stop { chat } => {
                if let Some(entry) = self.sessions.get(chat) {
                    entry.actor.send(SessionMsg::Stop).await?;
                }
            }
        }
        Ok(Continue)
    }
}
