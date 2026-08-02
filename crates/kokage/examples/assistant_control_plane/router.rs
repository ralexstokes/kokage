use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorRef, ActorSpec, Context, DynamicScopeRef, DynamicTree, ExitResult, Guard, ScopeRef,
    StopContext, Strategy, Tree,
    observe::{ChildEventKind, LifecycleEvent, LifecycleEventKind},
};
use tokio::sync::Notify;

use crate::{
    common::{Envelope, Evidence, EvidenceTx},
    safety::GateNotice,
    session::{Session, SessionDeps, SessionMsg, session_generation},
};

const MEMBERSHIP_BOUND: Duration = Duration::from_secs(1);

#[derive(Clone, Default)]
pub struct RemovalGate {
    hold: Arc<AtomicBool>,
    waiting: Arc<AtomicBool>,
    pub entered: Arc<Notify>,
    pub release: Arc<Notify>,
}

impl RemovalGate {
    pub fn hold(&self, hold: bool) {
        self.hold.store(hold, Ordering::Release);
        if !hold {
            self.release.notify_waiters();
        }
    }

    pub fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Acquire)
    }

    async fn wait_if_held(&self) {
        if self.hold.load(Ordering::Acquire) {
            self.waiting.store(true, Ordering::Release);
            self.entered.notify_waiters();
            while self.hold.load(Ordering::Acquire) {
                self.release.notified().await;
            }
            self.waiting.store(false, Ordering::Release);
        }
    }
}

enum Slot {
    Mounting {
        epoch: u64,
        subtree_id: String,
        buffered: Vec<Envelope>,
    },
    Active {
        epoch: u64,
        subtree_id: String,
        subtree: ScopeRef,
        session: ActorRef<SessionMsg>,
    },
    Removing {
        epoch: u64,
        subtree_id: String,
        buffered: Vec<Envelope>,
    },
}

impl Slot {
    fn identity(&self) -> (u64, &str) {
        match self {
            Self::Mounting {
                epoch, subtree_id, ..
            }
            | Self::Active {
                epoch, subtree_id, ..
            }
            | Self::Removing {
                epoch, subtree_id, ..
            } => (*epoch, subtree_id),
        }
    }
}

pub enum RouterMsg {
    Incoming(Envelope),
    Mounted {
        chat: String,
        epoch: u64,
        subtree_id: String,
        session: ActorRef<SessionMsg>,
        result: Result<ScopeRef, String>,
    },
    Evict {
        chat: String,
        epoch: u64,
    },
    Removed {
        chat: String,
        epoch: u64,
        subtree_id: String,
        result: Result<(), String>,
    },
    MountLifecycle(LifecycleEvent),
    OrphanRemoved {
        subtree_id: String,
        result: Result<(), String>,
    },
    GateChanged(GateNotice),
    Crash,
}

pub struct SessionRouter {
    sessions: DynamicScopeRef,
    deps: SessionDeps,
    epochs: Arc<AtomicU64>,
    removal_gate: RemovalGate,
    slots: HashMap<String, Slot>,
    lifecycle: Option<Guard>,
    evidence: EvidenceTx,
    generation: u64,
}

impl SessionRouter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sessions: DynamicScopeRef,
        deps: SessionDeps,
        epochs: Arc<AtomicU64>,
        removal_gate: RemovalGate,
        evidence: EvidenceTx,
        generation: u64,
    ) -> Self {
        Self {
            sessions,
            deps,
            epochs,
            removal_gate,
            slots: HashMap::new(),
            lifecycle: None,
            evidence,
            generation,
        }
    }

    fn start_mount(&mut self, chat: String, buffered: Vec<Envelope>, ctx: &mut Context<'_, Self>) {
        let epoch = self.epochs.fetch_add(1, Ordering::SeqCst) + 1;
        let safe_chat: String = chat
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character
                } else {
                    '-'
                }
            })
            .collect();
        let subtree_id = format!("session-{safe_chat}-e{epoch}");
        let (tree, session) = build_session_tree(&chat, epoch, self.deps.clone());
        let mount = self.sessions.clone();
        let completion_chat = chat.clone();
        let completion_id = subtree_id.clone();
        let completion_session = session.clone();

        self.slots.insert(
            chat,
            Slot::Mounting {
                epoch,
                subtree_id: subtree_id.clone(),
                buffered,
            },
        );
        ctx.offload(
            MEMBERSHIP_BOUND,
            async move {
                let subtree = mount
                    .add_subtree(subtree_id, tree)
                    .await
                    .map_err(|error| error.to_string())?;
                subtree
                    .wait_started()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(subtree)
            },
            move |result| RouterMsg::Mounted {
                chat: completion_chat,
                epoch,
                subtree_id: completion_id,
                session: completion_session,
                result: result
                    .map_err(|_| "session mount deadline".to_owned())
                    .and_then(|result| result),
            },
        );
    }

    fn sweep_orphan(&self, subtree_id: String, ctx: &mut Context<'_, Self>) {
        let Some(subtree) = self.sessions.subtree(&subtree_id) else {
            return;
        };
        let sessions = self.sessions.clone();
        let completion_id = subtree_id.clone();
        ctx.offload(
            MEMBERSHIP_BOUND,
            async move {
                sessions
                    .remove(&subtree)
                    .await
                    .map_err(|error| error.to_string())
            },
            move |result| RouterMsg::OrphanRemoved {
                subtree_id: completion_id,
                result: result
                    .map_err(|_| "orphan removal deadline".to_owned())
                    .and_then(|result| result),
            },
        );
    }

    fn slot_owns_id(&self, subtree_id: &str) -> bool {
        self.slots
            .values()
            .any(|slot| slot.identity().1 == subtree_id)
    }
}

impl Actor for SessionRouter {
    type Msg = RouterMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.evidence.emit(Evidence::ActorStarted {
            actor: "router",
            generation: self.generation,
        });
        self.lifecycle = Some(
            self.sessions
                .subscribe_lifecycle()
                .direct_children()
                .forward_to(&ctx.myself(), RouterMsg::MountLifecycle),
        );

        for child in self.sessions.snapshot().children {
            self.sweep_orphan(child.id, ctx);
        }
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RouterMsg::Incoming(envelope) => match self.slots.get_mut(&envelope.chat) {
                Some(Slot::Mounting { buffered, .. }) | Some(Slot::Removing { buffered, .. }) => {
                    buffered.push(envelope)
                }
                Some(Slot::Active { session, .. }) => {
                    session.send(SessionMsg::Incoming(envelope)).await?;
                }
                None => self.start_mount(envelope.chat.clone(), vec![envelope], ctx),
            },
            RouterMsg::Mounted {
                chat,
                epoch,
                subtree_id,
                session,
                result,
            } => {
                let current = self.slots.remove(&chat);
                match (current, result) {
                    (
                        Some(Slot::Mounting {
                            epoch: current_epoch,
                            subtree_id: current_id,
                            buffered,
                        }),
                        Ok(subtree),
                    ) if current_epoch == epoch && current_id == subtree_id => {
                        self.evidence.emit(Evidence::Mounted {
                            chat: chat.clone(),
                            epoch,
                            subtree_id: subtree_id.clone(),
                        });
                        for envelope in buffered {
                            session.send(SessionMsg::Incoming(envelope)).await?;
                        }
                        self.slots.insert(
                            chat,
                            Slot::Active {
                                epoch,
                                subtree_id,
                                subtree,
                                session,
                            },
                        );
                    }
                    (Some(slot), Ok(subtree)) => {
                        self.slots.insert(chat, slot);
                        let sessions = self.sessions.clone();
                        let stale_id = subtree_id.clone();
                        ctx.offload(
                            MEMBERSHIP_BOUND,
                            async move {
                                sessions
                                    .remove(&subtree)
                                    .await
                                    .map_err(|error| error.to_string())
                            },
                            move |result| RouterMsg::OrphanRemoved {
                                subtree_id: stale_id,
                                result: result
                                    .map_err(|_| "stale mount removal deadline".to_owned())
                                    .and_then(|result| result),
                            },
                        );
                    }
                    (Some(Slot::Mounting { buffered, .. }), Err(_)) => {
                        self.start_mount(chat, buffered, ctx);
                    }
                    (Some(slot), Err(_)) => {
                        self.slots.insert(chat, slot);
                    }
                    (None, Ok(subtree)) => {
                        let sessions = self.sessions.clone();
                        let stale_id = subtree_id.clone();
                        ctx.offload(
                            MEMBERSHIP_BOUND,
                            async move {
                                sessions
                                    .remove(&subtree)
                                    .await
                                    .map_err(|error| error.to_string())
                            },
                            move |result| RouterMsg::OrphanRemoved {
                                subtree_id: stale_id,
                                result: result
                                    .map_err(|_| "unrouted mount removal deadline".to_owned())
                                    .and_then(|result| result),
                            },
                        );
                    }
                    (None, Err(_)) => {}
                }
            }
            RouterMsg::Evict { chat, epoch } => {
                let Some(slot) = self.slots.remove(&chat) else {
                    return Ok(());
                };
                match slot {
                    Slot::Active {
                        epoch: current_epoch,
                        subtree_id,
                        subtree,
                        ..
                    } if current_epoch == epoch => {
                        self.evidence.emit(Evidence::Removing {
                            chat: chat.clone(),
                            epoch,
                            subtree_id: subtree_id.clone(),
                        });
                        let sessions = self.sessions.clone();
                        let removed_subtree = subtree.clone();
                        let gate = self.removal_gate.clone();
                        let completion_chat = chat.clone();
                        let completion_id = subtree_id.clone();
                        ctx.offload(
                            MEMBERSHIP_BOUND,
                            async move {
                                gate.wait_if_held().await;
                                sessions
                                    .remove(&removed_subtree)
                                    .await
                                    .map_err(|error| error.to_string())
                            },
                            move |result| RouterMsg::Removed {
                                chat: completion_chat,
                                epoch,
                                subtree_id: completion_id,
                                result: result
                                    .map_err(|_| "session removal deadline".to_owned())
                                    .and_then(|result| result),
                            },
                        );
                        self.slots.insert(
                            chat,
                            Slot::Removing {
                                epoch,
                                subtree_id,
                                buffered: Vec::new(),
                            },
                        );
                    }
                    other => {
                        self.slots.insert(chat, other);
                    }
                }
            }
            RouterMsg::Removed {
                chat,
                epoch,
                subtree_id,
                result,
            } => {
                result.map_err(io::Error::other)?;
                let Some(slot) = self.slots.remove(&chat) else {
                    return Ok(());
                };
                match slot {
                    Slot::Removing {
                        epoch: current_epoch,
                        subtree_id: current_id,
                        buffered,
                        ..
                    } if current_epoch == epoch && current_id == subtree_id => {
                        self.evidence.emit(Evidence::Removed {
                            chat: chat.clone(),
                            epoch,
                            subtree_id,
                        });
                        if !buffered.is_empty() {
                            self.start_mount(chat, buffered, ctx);
                        }
                    }
                    other => {
                        self.slots.insert(chat, other);
                    }
                }
            }
            RouterMsg::MountLifecycle(event) => {
                if let LifecycleEventKind::Child(child) = event.kind
                    && matches!(child.kind, ChildEventKind::Added)
                    && !self.slot_owns_id(&child.child_id)
                {
                    self.sweep_orphan(child.child_id, ctx);
                }
            }
            RouterMsg::OrphanRemoved { subtree_id, result } => {
                if result.is_ok() {
                    self.evidence.emit(Evidence::OrphanSwept(subtree_id));
                }
            }
            RouterMsg::GateChanged(notice) => {
                for slot in self.slots.values() {
                    if let Slot::Active { session, .. } = slot {
                        let _ = session.try_send(SessionMsg::GateChanged(notice.clone()));
                    }
                }
            }
            RouterMsg::Crash => panic!("scripted router crash"),
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("router"));
        Ok(())
    }
}

fn build_session_tree(chat: &str, epoch: u64, deps: SessionDeps) -> (Tree, ActorRef<SessionMsg>) {
    let runs_tree = DynamicTree::new();
    let runs = runs_tree.scope();
    let generations = Arc::new(AtomicU64::new(0));
    let session_chat = chat.to_owned();
    let spec = ActorSpec::new("orchestrator", move || {
        Session::new(
            session_chat.clone(),
            epoch,
            runs.clone(),
            deps.clone(),
            session_generation(&generations),
        )
    });
    let session = spec.actor_ref();
    let mut tree = Tree::new().strategy(Strategy::OneForAll);
    tree.add_actor_spec(spec);
    tree.add_subtree("runs", runs_tree);
    (tree, session)
}
