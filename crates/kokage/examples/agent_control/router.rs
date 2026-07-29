//! Session router: the single writer for dynamic session membership.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use kokage::{
    Actor, ActorRef, ActorResult, ActorSlot, Context, ControlError, DynamicTree, Guard,
    OrderedTree, RuntimeHandle, Shutdown, Strategy, SupervisorError,
    observe::{ChildMembershipView, LifecycleEvent, LifecycleEventKind, SupervisorSnapshot},
};

use crate::{
    messages::{
        BudgetMsg, ChatId, GuardMsg, JournalMsg, OutboundMsg, PHASE_TIMEOUT, PendingInput,
        ProgressMsg, Proof, RouterMsg, SessionMsg, ToolHostMsg,
    },
    model::ModelClient,
    session::SessionFactory,
};

// A cooperative removal resolves only after the departing subtree drains. If
// that drain bounces messages through this router, awaiting removal here would
// stop the router from making the drain progress. Removal therefore remains a
// pipelined offload. Distinct-id additions are now dispatched during that drain
// and resolve on insertion; this example retains a symmetric Mounting step so
// every membership transition uses the same explicit completion-message shape.
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

fn mount_reconciliation(
    snapshot: SupervisorSnapshot,
    routes_subtree: impl FnMut(&str) -> bool,
) -> (u64, Vec<String>) {
    let alignment_seq = snapshot.lifecycle_seq;
    mount_reconciliation_parts(
        alignment_seq,
        snapshot
            .children
            .into_iter()
            .map(|child| (child.id, child.membership)),
        routes_subtree,
    )
}

fn mount_reconciliation_parts(
    alignment_seq: u64,
    children: impl IntoIterator<Item = (String, ChildMembershipView)>,
    mut routes_subtree: impl FnMut(&str) -> bool,
) -> (u64, Vec<String>) {
    let orphaned = children
        .into_iter()
        .filter(|(_, membership)| *membership == ChildMembershipView::Active)
        .filter(|(id, _)| !routes_subtree(id))
        .map(|(id, _)| id)
        .collect();
    (alignment_seq, orphaned)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MountEventDisposition {
    ReconcileSnapshot,
    Apply,
    Ignore,
}

fn mount_event_disposition(alignment_seq: u64, event: &LifecycleEvent) -> MountEventDisposition {
    event_disposition(
        alignment_seq,
        event.seq().unwrap_or(0),
        matches!(&event.kind, LifecycleEventKind::Lagged { .. }),
    )
}

fn event_disposition(alignment_seq: u64, event_seq: u64, lagged: bool) -> MountEventDisposition {
    if lagged {
        MountEventDisposition::ReconcileSnapshot
    } else if event_seq > alignment_seq {
        MountEventDisposition::Apply
    } else {
        MountEventDisposition::Ignore
    }
}

#[derive(kokage::ActorFactory)]
pub struct Router {
    /// Reserved before the root is built and retained by `RouterFactory`, so
    /// it survives router restarts without late binding.
    mount: RuntimeHandle,
    #[factory(default)]
    sessions: HashMap<ChatId, SessionSlot>,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    guard: ActorRef<GuardMsg>,
    outbound: ActorRef<OutboundMsg>,
    progress: ActorRef<ProgressMsg>,
    gate: Arc<AtomicBool>,
    model: Arc<dyn ModelClient>,
    #[factory(default)]
    task_sequence: Arc<AtomicU64>,
    /// Also factory-owned: the id allocator must survive router incarnations,
    /// or a reborn router would re-mint `session:<chat>#1` while its
    /// predecessor's subtree still exists.
    session_epoch: Arc<AtomicU64>,
    proof: Proof,
    #[factory(default)]
    mount_watch: Option<Guard>,
    #[factory(default)]
    alignment_seq: u64,
}

impl Router {
    fn mount(&self) -> RuntimeHandle {
        self.mount.clone()
    }

    /// Mints a fresh incarnation for `chat`: a `Mounting` slot routing into
    /// the pre-built stable mailbox, and a pipelined `add_subtree`.
    ///
    /// Each incarnation gets a subtree id no other incarnation ever uses (the
    /// allocator outlives this router), so a replacement never contends with
    /// a predecessor whose removal is still draining, and an `Evict` naming
    /// an id can never be misread as targeting a successor.
    fn mint(&mut self, chat: ChatId, buffered: Vec<PendingInput>, ctx: &mut Context<'_, Self>) {
        let generation = self.session_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let subtree_id = format!("session:{chat}#{generation}");
        let actor_slot = ActorSlot::new("session");
        let (actor_slot, actor) = actor_slot.actor_ref();
        let session_actor = actor_slot
            .define(SessionFactory {
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
                gate: self.gate.clone(),
                model: self.model.clone(),
                task_sequence: self.task_sequence.clone(),
                proof: self.proof.clone(),
            })
            // Draining is load-bearing for eviction: a message forwarded before
            // `Evict` must be bounced to the router for the replacement session.
            .shutdown(Shutdown::drain_for(PHASE_TIMEOUT));
        let mount = self.mount();
        let offload_id = subtree_id.clone();
        ctx.offload(
            PHASE_TIMEOUT,
            async move {
                // OneForAll: a session panic tears its transient runs down
                // with it; the session is reborn from this builder and
                // rehydrates from the journal, while `Never` run children are
                // skipped by the group respawn and cannot themselves recycle
                // the session.
                let subtree = mount
                    .dynamic()
                    .expect("the session mount is declared dynamic")
                    .add_subtree(
                        offload_id,
                        OrderedTree::new().subtree(
                            "session-runtime",
                            OrderedTree::new()
                                .strategy(Strategy::OneForAll)
                                .actor(session_actor)
                                .subtree("children", DynamicTree::new()),
                        ),
                    )
                    .await;
                subtree.is_ok()
            },
            move |result| RouterMsg::Mounted {
                chat,
                ok: result.unwrap_or(false),
            },
        )
        .detach();
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

    fn pipeline_remove(&self, chat: ChatId, subtree_id: String, ctx: &mut Context<'_, Self>) {
        let mount = self.mount();
        let remove_id = subtree_id.clone();
        ctx.offload(
            PHASE_TIMEOUT,
            async move {
                matches!(
                    mount
                        .dynamic()
                        .expect("the session mount is declared dynamic")
                        .remove_child(remove_id)
                        .await,
                    Ok(())
                        | Err(ControlError::UnknownChildId(_))
                        | Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(_)))
                )
            },
            move |result| RouterMsg::Reaped {
                chat,
                subtree_id,
                done: result.unwrap_or(false),
            },
        )
        .detach();
    }

    /// Removes a subtree that no live slot routes to: an orphan minted by a
    /// previous router incarnation, or a stale duplicate retirement request.
    fn pipeline_sweep(&self, subtree_id: String, ctx: &mut Context<'_, Self>) {
        let mount = self.mount();
        let remove_id = subtree_id.clone();
        ctx.offload(
            PHASE_TIMEOUT,
            async move {
                matches!(
                    mount
                        .dynamic()
                        .expect("the session mount is declared dynamic")
                        .remove_child(remove_id)
                        .await,
                    Ok(())
                        | Err(ControlError::UnknownChildId(_))
                        | Err(ControlError::Failed(SupervisorError::ShutdownTimedOut(_)))
                        | Err(ControlError::ChildRemovalInProgress(_))
                )
            },
            move |result| RouterMsg::Swept {
                subtree_id,
                done: result.unwrap_or(false),
            },
        )
        .detach();
    }

    async fn forward(
        actor: &ActorRef<SessionMsg>,
        input: PendingInput,
    ) -> Result<(), kokage::SendError> {
        actor
            .send(SessionMsg::UserMessage {
                envelope: input.envelope,
                text: input.text,
            })
            .await
    }

    fn routes_subtree(&self, subtree_id: &str) -> bool {
        self.sessions.values().any(|slot| match slot {
            SessionSlot::Mounting {
                subtree_id: live, ..
            }
            | SessionSlot::Active {
                subtree_id: live, ..
            } => live == subtree_id,
            SessionSlot::Removing { .. } => false,
        })
    }

    /// Realigns edge-derived routing state with the mount's current truth.
    ///
    /// This is shared by initial watch alignment and lifecycle overflow. A
    /// removal already in progress needs no second request; every active
    /// membership not owned by this router incarnation is swept.
    fn reconcile_mount_snapshot(&mut self, ctx: &mut Context<'_, Self>) {
        let (alignment_seq, orphaned) =
            mount_reconciliation(self.mount.snapshot(), |id| self.routes_subtree(id));
        self.alignment_seq = alignment_seq;
        for subtree_id in orphaned {
            self.pipeline_sweep(subtree_id, ctx);
        }
    }
}

impl Actor for Router {
    type Msg = RouterMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ActorResult {
        // Alignment is watch first, snapshot second, then seq filtering in the
        // handler. A restarted router owns no prior slots, so every membership
        // in the snapshot is an orphan to sweep. A concurrently completed old
        // add appears after the baseline as Added and is swept there instead.
        self.mount_watch = Some(
            self.mount
                .watch_lifecycle_to(&ctx.myself(), RouterMsg::MountLifecycle),
        );
        self.reconcile_mount_snapshot(ctx);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ActorResult {
        match message {
            RouterMsg::MountLifecycle(event) => {
                match mount_event_disposition(self.alignment_seq, &event) {
                    MountEventDisposition::ReconcileSnapshot => {
                        // A marker covers a dropped prefix without one usable
                        // sequence. Always resnapshot instead of applying
                        // ordinary sequence filtering.
                        self.reconcile_mount_snapshot(ctx);
                    }
                    MountEventDisposition::Apply => {
                        self.alignment_seq = event
                            .seq()
                            .expect("only child transitions have an alignment sequence");
                        if let LifecycleEventKind::ChildAdded { child_id, .. } = event.kind
                            && !self.routes_subtree(&child_id)
                        {
                            self.pipeline_sweep(child_id, ctx);
                        }
                    }
                    MountEventDisposition::Ignore => {}
                }
            }
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
                    // The mount offload failed or deadlined; restarting the
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
                    return Ok(());
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lagged_event_bypasses_seq_filter_and_reconciles_active_orphans() {
        assert_eq!(
            event_disposition(72, 72, true),
            MountEventDisposition::ReconcileSnapshot
        );
        assert_eq!(
            event_disposition(72, 71, false),
            MountEventDisposition::Ignore
        );
        assert_eq!(
            event_disposition(72, 73, false),
            MountEventDisposition::Apply
        );

        let (alignment_seq, orphaned) = mount_reconciliation_parts(
            73,
            vec![
                ("routed".to_owned(), ChildMembershipView::Active),
                ("orphan".to_owned(), ChildMembershipView::Active),
                ("already-removing".to_owned(), ChildMembershipView::Removing),
            ],
            |id| id == "routed",
        );

        assert_eq!(alignment_seq, 73);
        assert_eq!(orphaned, ["orphan"]);
    }
}
