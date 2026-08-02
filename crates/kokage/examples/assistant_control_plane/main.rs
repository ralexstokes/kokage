//! An assertion-driven, in-process control plane for a personal multi-agent assistant.
//!
//! The example is deliberately an executable acceptance script rather than a
//! server. The static root is ordered as `core -> sessions -> gateway`, so
//! reverse-order shutdown closes intake first and leaves the journal alive
//! last. Its application topology is:
//!
//! ```text
//! root
//! ├── core: journal -> budget <-> guard -> tool host -> session router
//! ├── sessions: dynamic session subtrees
//! │   └── session-<chat>-e<epoch> (OneForAll)
//! │       ├── orchestrator
//! │       └── runs: dynamic temporary run actors
//! └── gateway (RestForOne)
//!     └── outbound -> conflated progress -> readiness-gated inbound bridge
//! ```
//!
//! The phases prove startup gating; an exact happy-path transcript; bounded
//! slow-model retry and both levels of panic isolation; transport redelivery
//! at the journal/ack boundary; tool reconciliation; cancellable streaming
//! with progress conflation; failure-window and budget pauses; router rebirth,
//! idle eviction, and racing remount; then staged shutdown with telemetry.
//! Every correctness wait is a bounded event, lifecycle, snapshot, or state
//! poll. The only sleeps are inside the two deliberately slow dependencies.

mod common;
mod gateway;
mod journal;
mod router;
mod safety;
mod session;
mod tool;

use std::{
    collections::VecDeque,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common::{CALL_BOUND, Envelope, Evidence, JournalEntry, Stage, WAIT_BOUND};
use gateway::{
    BridgeMsg, ChatTransport, ConnectGate, InboundBridge, OutboundSender, ProgressGate,
    ProgressMsg, ProgressSender, next_generation,
};
use journal::{Journal, JournalStore, SharedJournal};
use kokage::{
    ActorRef, ActorSlot, ActorSpec, DynamicScopeRef, DynamicTree, Mailbox, RestartPolicy,
    RunningTree, ScopeRef, Strategy, Tree,
    observe::{ChildEventKind, ExitStatus, LifecycleEvent, LifecycleEventKind},
};
use router::{RemovalGate, RouterMsg, SessionRouter};
use safety::{Budget, BudgetMsg, GateNotice, GuardActor, ModelControl, SafetyGate};
use session::{ScriptedModel, SessionDeps, SessionSettings};
use tokio::sync::{Mutex, broadcast, mpsc};
use tool::{ToolHost, ToolState};
use tracing::warn;
use tracing_subscriber::prelude::*;

struct Acceptance {
    running: Option<RunningTree>,
    root: ScopeRef,
    core: ScopeRef,
    sessions: DynamicScopeRef,
    gateway: ScopeRef,
    router: ActorRef<RouterMsg>,
    budget: ActorRef<BudgetMsg>,
    progress: ActorRef<ProgressMsg>,
    transport: ChatTransport,
    connect_gate: ConnectGate,
    journal: SharedJournal,
    spent: Arc<AtomicU64>,
    tools: Arc<ToolState>,
    model_control: ModelControl,
    safety_gate: SafetyGate,
    session_settings: SessionSettings,
    progress_gate: ProgressGate,
    removal_gate: RemovalGate,
    evidence: mpsc::UnboundedReceiver<Evidence>,
    evidence_backlog: VecDeque<Evidence>,
    lifecycle: kokage::observe::LifecycleWatch,
    notice_a: broadcast::Receiver<GateNotice>,
    notice_b: broadcast::Receiver<GateNotice>,
    active_session: Option<(u64, String)>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let deliberate = info
            .payload()
            .downcast_ref::<&str>()
            .is_some_and(|message| {
                message.starts_with("scripted ") || message.starts_with("transport disconnected")
            });
        if !deliberate {
            default_panic_hook(info);
        }
    }));
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .without_time()
                .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
        )
        .try_init();

    tokio::time::timeout(Duration::from_secs(20), async {
        let mut acceptance = build()?;
        acceptance.run().await
    })
    .await
    .expect("the complete acceptance script must remain bounded")?;
    Ok(())
}

fn build() -> Result<Acceptance, kokage::BuildError> {
    let (evidence_tx, evidence) = mpsc::unbounded_channel();
    let evidence_tx = common::EvidenceTx::new(evidence_tx);
    let transport = ChatTransport::default();
    let connect_gate = ConnectGate::default();
    let progress_gate = ProgressGate::default();
    let removal_gate = RemovalGate::default();
    let journal: SharedJournal = Arc::new(Mutex::new(JournalStore::default()));
    let tools = Arc::new(ToolState::default());
    let spent = Arc::new(AtomicU64::new(0));
    let model_control = ModelControl::default();
    let scripted_model = ScriptedModel::new(model_control.clone());
    let safety_gate = SafetyGate::new_open();
    let session_settings = SessionSettings::default();
    let epochs = Arc::new(AtomicU64::new(0));
    let (notices, _) = broadcast::channel(32);
    let notice_a = notices.subscribe();
    let notice_b = notices.subscribe();

    // Open every cyclic declaration before defining any factory. Budget and
    // guard close a genuine behavioral cycle; guard also reports gate changes
    // through the router once that later slot is defined.
    let budget_slot = ActorSlot::<BudgetMsg>::new("budget");
    let budget_ref = budget_slot.actor_ref();
    let guard_slot = ActorSlot::new("guard");
    let guard_ref = guard_slot.actor_ref();
    let router_slot = ActorSlot::new("session-router");
    let router_ref = router_slot.actor_ref();

    let journal_generations = Arc::new(AtomicU64::new(0));
    let journal_spec = ActorSpec::new("journal", {
        let journal = Arc::clone(&journal);
        let evidence = evidence_tx.clone();
        move || {
            Journal::new(
                Arc::clone(&journal),
                evidence.clone(),
                next_generation(&journal_generations),
            )
        }
    });
    let journal_ref = journal_spec.actor_ref();

    let outbound_generations = Arc::new(AtomicU64::new(0));
    let outbound_spec = ActorSpec::new("outbound-sender", {
        let evidence = evidence_tx.clone();
        move || OutboundSender::new(evidence.clone(), next_generation(&outbound_generations))
    });
    let outbound_ref = outbound_spec.actor_ref();

    let progress_generations = Arc::new(AtomicU64::new(0));
    let progress_spec = ActorSpec::new("progress-sender", {
        let outbound = outbound_ref.clone();
        let gate = progress_gate.clone();
        let evidence = evidence_tx.clone();
        move || {
            ProgressSender::new(
                outbound.clone(),
                gate.clone(),
                evidence.clone(),
                next_generation(&progress_generations),
            )
        }
    })
    .mailbox(Mailbox::latest());
    let progress_ref = progress_spec.actor_ref();

    let tool_generations = Arc::new(AtomicU64::new(0));
    let tool_spec = ActorSpec::new("tool-host", {
        let tools = Arc::clone(&tools);
        let evidence = evidence_tx.clone();
        move || {
            ToolHost::new(
                Arc::clone(&tools),
                evidence.clone(),
                next_generation(&tool_generations),
            )
        }
    });
    let tool_ref = tool_spec.actor_ref();

    let sessions_tree = DynamicTree::new();
    let sessions = sessions_tree.scope();

    let budget_generations = Arc::new(AtomicU64::new(0));
    let budget_spec = budget_slot.define({
        let spent = Arc::clone(&spent);
        let guard = guard_ref.clone();
        let evidence = evidence_tx.clone();
        move || {
            Budget::new(
                Arc::clone(&spent),
                10_000,
                guard.clone(),
                evidence.clone(),
                next_generation(&budget_generations),
            )
        }
    });

    let guard_generations = Arc::new(AtomicU64::new(0));
    let guard_spec = guard_slot.define({
        let budget = budget_ref.clone();
        let router = router_ref.clone();
        let gate = safety_gate.clone();
        let notices = notices.clone();
        let model = model_control.clone();
        let evidence = evidence_tx.clone();
        move || {
            GuardActor::new(
                budget.clone(),
                router.clone(),
                gate.clone(),
                notices.clone(),
                model.clone(),
                evidence.clone(),
                next_generation(&guard_generations),
            )
        }
    });

    let session_deps = SessionDeps {
        journal: journal_ref.clone(),
        budget: budget_ref.clone(),
        guard: guard_ref.clone(),
        tool: tool_ref,
        outbound: outbound_ref.clone(),
        progress: progress_ref.clone(),
        router: router_ref.clone(),
        gate: safety_gate.clone(),
        model: scripted_model,
        settings: session_settings.clone(),
        evidence: evidence_tx.clone(),
    };
    let router_generations = Arc::new(AtomicU64::new(0));
    let router_spec = router_slot.define({
        let sessions = sessions.clone();
        let deps = session_deps;
        let epochs = Arc::clone(&epochs);
        let removal_gate = removal_gate.clone();
        let evidence = evidence_tx.clone();
        move || {
            SessionRouter::new(
                sessions.clone(),
                deps.clone(),
                Arc::clone(&epochs),
                removal_gate.clone(),
                evidence.clone(),
                next_generation(&router_generations),
            )
        }
    });

    let bridge_generations = Arc::new(AtomicU64::new(0));
    let bridge_spec = ActorSpec::<BridgeMsg>::new("inbound-bridge", {
        let transport = transport.clone();
        let connect_gate = connect_gate.clone();
        let journal = journal_ref;
        let router = router_ref.clone();
        let evidence = evidence_tx.clone();
        move || {
            InboundBridge::new(
                transport.clone(),
                connect_gate.clone(),
                journal.clone(),
                router.clone(),
                evidence.clone(),
                next_generation(&bridge_generations),
            )
        }
    })
    .restart(RestartPolicy::on_failure());

    let mut core_tree = Tree::new();
    core_tree.add_actor_spec(journal_spec);
    core_tree.add_actor_spec(budget_spec);
    core_tree.add_actor_spec(guard_spec);
    core_tree.add_actor_spec(tool_spec);
    core_tree.add_actor_spec(router_spec);
    let core = core_tree.scope();

    let mut gateway_tree = Tree::new().strategy(Strategy::RestForOne);
    gateway_tree.add_actor_spec(outbound_spec);
    gateway_tree.add_actor_spec(progress_spec);
    gateway_tree.add_actor_spec(bridge_spec);
    let gateway = gateway_tree.scope();

    // Reverse-order shutdown now closes gateway intake, drains sessions, and
    // keeps the core journal alive until the end.
    let mut root_tree = Tree::new();
    root_tree.add_subtree("core", core_tree);
    root_tree.add_subtree("sessions", sessions_tree);
    root_tree.add_subtree("gateway", gateway_tree);
    let root = root_tree.scope();
    let lifecycle = root.subscribe_lifecycle();
    let running = root_tree.spawn()?;

    Ok(Acceptance {
        running: Some(running),
        root,
        core,
        sessions,
        gateway,
        router: router_ref,
        budget: budget_ref,
        progress: progress_ref,
        transport,
        connect_gate,
        journal,
        spent,
        tools,
        model_control,
        safety_gate,
        session_settings,
        progress_gate,
        removal_gate,
        evidence,
        evidence_backlog: VecDeque::new(),
        lifecycle,
        notice_a,
        notice_b,
        active_session: None,
    })
}

impl Acceptance {
    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        self.phase_gated_startup().await?;
        self.phase_happy_path().await?;
        self.phase_panic_isolation().await?;
        self.phase_bridge_redelivery().await?;
        self.phase_tool_reconciliation().await?;
        self.phase_flood_cancellation().await?;
        self.phase_pause_probe_budget().await?;
        self.phase_eviction_and_replay().await?;
        self.phase_staged_shutdown().await?;
        Ok(())
    }

    async fn phase_gated_startup(&mut self) -> Result<(), Box<dyn Error>> {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), self.root.wait_started())
                .await
                .is_err(),
            "root readiness must wait for the raw bridge connection"
        );
        let gateway = self.gateway.snapshot();
        assert_eq!(gateway.strategy, Strategy::RestForOne);
        assert!(
            gateway
                .child("outbound-sender")
                .is_some_and(|child| child.state.is_running())
        );
        assert!(
            gateway
                .child("progress-sender")
                .is_some_and(|child| child.state.is_running())
        );
        self.connect_gate.open();
        tokio::time::timeout(WAIT_BOUND, self.root.wait_started()).await??;
        assert!(self.sessions.snapshot().children.is_empty());
        warn!(
            phase = 1,
            "gated startup: bridge readiness released the root"
        );
        Ok(())
    }

    async fn phase_happy_path(&mut self) -> Result<(), Box<dyn Error>> {
        self.publish(Envelope::new(1, "alpha", "hello")).await;
        let mounted = self
            .next_evidence(
                "initial session mount",
                |event| matches!(event, Evidence::Mounted { chat, .. } if chat == "alpha"),
            )
            .await;
        if let Evidence::Mounted {
            epoch, subtree_id, ..
        } = mounted
        {
            self.active_session = Some((epoch, subtree_id));
        }
        self.wait_completed(1).await;

        let entries = self.journal.lock().await.entries().to_vec();
        assert_eq!(
            entries,
            vec![
                JournalEntry::Incoming {
                    envelope_id: 1,
                    chat: "alpha".to_owned(),
                    text: "hello".to_owned(),
                },
                JournalEntry::ModelTurn {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    stage: Stage::Planner,
                    tokens: 11,
                },
                JournalEntry::ModelTurn {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    stage: Stage::Engineer,
                    tokens: 13,
                },
                JournalEntry::ToolIntent {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    key: "alpha:1:1".to_owned(),
                },
                JournalEntry::ToolResult {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    key: "alpha:1:1".to_owned(),
                    reconciled: false,
                },
                JournalEntry::ModelTurn {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    stage: Stage::Reviewer,
                    tokens: 7,
                },
                JournalEntry::Assistant {
                    chat: "alpha".to_owned(),
                    envelope_id: 1,
                    attempt: 1,
                    text: "completed hello".to_owned(),
                },
            ]
        );
        assert_eq!(self.spent.load(Ordering::Acquire), 31);
        warn!(
            phase = 2,
            journal_entries = entries.len(),
            tokens = 31,
            "happy path: exact durable transcript"
        );
        Ok(())
    }

    async fn phase_panic_isolation(&mut self) -> Result<(), Box<dyn Error>> {
        let session_id = self
            .active_session
            .as_ref()
            .expect("session active")
            .1
            .clone();
        let session = self.sessions.subtree(&session_id).expect("session mounted");
        let generation = session
            .snapshot()
            .child("orchestrator")
            .expect("orchestrator present")
            .generation;

        self.publish(Envelope::new(20, "alpha", "slow turn")).await;
        self.next_evidence("deadline-bounded slow turn", |event| {
            matches!(
                event,
                Evidence::RunFailed {
                    envelope_id: 20,
                    attempt: 1,
                    reason,
                    ..
                } if reason.contains("model deadline")
            )
        })
        .await;
        self.wait_completed(20).await;

        self.publish(Envelope::new(2, "alpha", "run panic")).await;
        self.next_evidence("first panicking run start", |event| {
            matches!(
                event,
                Evidence::RunStarted {
                    envelope_id: 2,
                    attempt: 1,
                    ..
                }
            )
        })
        .await;
        let exited = self.wait_run_edge("run-2-a1", true).await;
        assert!(matches!(
            exited.kind,
            LifecycleEventKind::Child(ref child)
                if matches!(child.kind, ChildEventKind::Exited {
                    exit: ExitStatus::Panicked { .. }, ..
                })
        ));
        self.wait_run_edge("run-2-a1", false).await;
        self.wait_completed(2).await;
        assert_eq!(
            session
                .snapshot()
                .child("orchestrator")
                .expect("orchestrator remains")
                .generation,
            generation,
            "a temporary run panic must not recycle its session"
        );

        self.publish(Envelope::new(3, "alpha", "session panic"))
            .await;
        self.next_evidence("session-panic run start", |event| {
            matches!(event, Evidence::RunStarted { envelope_id: 3, .. })
        })
        .await;
        poll_until("OneForAll session restart", || {
            session
                .snapshot()
                .child("orchestrator")
                .is_some_and(|child| child.generation > generation)
        })
        .await;
        self.wait_completed(3).await;
        warn!(
            phase = 3,
            "panic isolation: temporary Exited→Removed; session panic recycled its run scope"
        );
        Ok(())
    }

    async fn phase_bridge_redelivery(&mut self) -> Result<(), Box<dyn Error>> {
        self.transport.disconnect_before_ack(4).await;
        self.transport
            .publish(Envelope::new(4, "alpha", "redeliver"))
            .await;
        self.wait_acked(4).await;
        self.next_evidence("deduplicated bridge redelivery", |event| {
            matches!(
                event,
                Evidence::BridgeJournaled {
                    envelope_id: 4,
                    duplicate: true
                }
            )
        })
        .await;
        self.wait_completed(4).await;
        assert_eq!(self.transport.deliveries(4).await, 2);
        let store = self.journal.lock().await;
        assert_eq!(
            store
                .entries()
                .iter()
                .filter(|entry| matches!(entry, JournalEntry::Incoming { envelope_id: 4, .. }))
                .count(),
            1
        );
        assert_eq!(store.duplicate_envelopes(), 1);
        drop(store);
        let gateway = self.gateway.snapshot();
        assert_eq!(gateway.child("outbound-sender").unwrap().generation, 0);
        assert_eq!(gateway.child("progress-sender").unwrap().generation, 0);
        assert_eq!(gateway.child("inbound-bridge").unwrap().generation, 1);
        warn!(
            phase = 4,
            deliveries = 2,
            "bridge crash: redelivery deduped and only the RestForOne suffix restarted"
        );
        Ok(())
    }

    async fn phase_tool_reconciliation(&mut self) -> Result<(), Box<dyn Error>> {
        self.publish(Envelope::new(5, "alpha", "stall tool")).await;
        let key = "alpha:5:1";
        self.next_evidence(
            "tool reconciliation",
            |event| matches!(event, Evidence::ToolReconciled { key: observed } if observed == key),
        )
        .await;
        self.wait_completed(5).await;
        assert_eq!(self.tools.executions(key).await, 1);
        assert!(self.tools.blocking_runs() > 0);
        assert!(self.journal.lock().await.entries().iter().any(|entry| {
            matches!(
                entry,
                JournalEntry::ToolResult {
                    envelope_id: 5,
                    reconciled: true,
                    ..
                }
            )
        }));
        warn!(
            phase = 5,
            executions = 1,
            "tool deadline: reconciled by idempotency key without a second effect"
        );
        Ok(())
    }

    async fn phase_flood_cancellation(&mut self) -> Result<(), Box<dyn Error>> {
        self.progress_gate.block.store(true, Ordering::Release);
        self.publish(Envelope::new(6, "alpha", "flood")).await;
        self.next_evidence("flood run start", |event| {
            matches!(event, Evidence::RunStarted { envelope_id: 6, .. })
        })
        .await;
        poll_until("progress sender enters its held handler", || {
            self.progress_gate.blocked.load(Ordering::Acquire)
        })
        .await;
        poll_until("progress mailbox visibly conflates", || {
            self.progress.stats().messages_conflated > 0
        })
        .await;

        self.publish(Envelope::new(7, "alpha", "cancel flood"))
            .await;
        self.wait_run_edge("run-6-a1", false).await;
        self.progress_gate.block.store(false, Ordering::Release);
        self.progress_gate.release.notify_waiters();
        self.next_evidence("latest conflated progress becomes visible", |event| {
            matches!(event, Evidence::Progress { envelope_id: 6, sequence } if *sequence > 1)
        })
        .await;
        warn!(
            phase = 6,
            conflated = self.progress.stats().messages_conflated,
            "stream flood: run cancellation stopped production and latest progress survived"
        );
        Ok(())
    }

    async fn phase_pause_probe_budget(&mut self) -> Result<(), Box<dyn Error>> {
        self.model_control.set_rate_limited(true);
        self.publish(Envelope::new(8, "alpha", "rate outage")).await;
        self.wait_gate(false, "failure window").await;
        self.assert_notice_fanout(false).await;

        self.publish(Envelope::new(9, "alpha", "held during pause"))
            .await;
        self.next_evidence("paused message held", |event| {
            matches!(event, Evidence::HeldWhilePaused { envelope_id: 9, .. })
        })
        .await;
        assert!(
            self.journal
                .lock()
                .await
                .entries()
                .iter()
                .any(|entry| { matches!(entry, JournalEntry::Incoming { envelope_id: 9, .. }) })
        );

        self.model_control.set_rate_limited(false);
        self.wait_gate(true, "probe succeeded").await;
        self.wait_completed(8).await;
        self.wait_completed(9).await;

        let status = self.budget.call(BudgetMsg::Status, CALL_BOUND).await?;
        self.budget
            .call(
                |reply| BudgetMsg::SetCap {
                    cap: status.spent + 5,
                    reply,
                },
                CALL_BOUND,
            )
            .await?;
        self.publish(Envelope::new(10, "alpha", "budget breach"))
            .await;
        self.wait_gate(false, "budget cap exceeded").await;
        self.publish(Envelope::new(11, "alpha", "budget-held"))
            .await;
        self.next_evidence("budget-paused message held", |event| {
            matches!(
                event,
                Evidence::HeldWhilePaused {
                    envelope_id: 11,
                    ..
                }
            )
        })
        .await;
        self.budget
            .call(|reply| BudgetMsg::Reset { cap: 10_000, reply }, CALL_BOUND)
            .await?;
        self.wait_gate(true, "probe succeeded").await;
        self.wait_completed(10).await;
        self.wait_completed(11).await;
        assert!(self.safety_gate.is_open());
        warn!(
            phase = 7,
            "guard: failure window and budget breach paused creation; probes released durable work"
        );
        Ok(())
    }

    async fn phase_eviction_and_replay(&mut self) -> Result<(), Box<dyn Error>> {
        let (old_epoch, old_id) = self.active_session.clone().expect("session active");
        self.router.send(RouterMsg::Crash).await?;
        poll_until("router restart", || {
            self.core
                .snapshot()
                .child("session-router")
                .is_some_and(|child| child.generation > 0)
        })
        .await;
        self.next_evidence(
            "orphan sweep after router restart",
            |event| matches!(event, Evidence::OrphanSwept(id) if id == &old_id),
        )
        .await;
        poll_until("orphan subtree absent", || {
            self.sessions.snapshot().child(&old_id).is_none()
        })
        .await;

        self.session_settings.enable_idle_eviction(true);
        self.removal_gate.hold(true);
        self.publish(Envelope::new(12, "alpha", "respawn after router"))
            .await;
        let mounted = self
            .next_evidence("post-router-restart mount", |event| {
                matches!(event, Evidence::Mounted { chat, epoch, .. } if chat == "alpha" && *epoch > old_epoch)
            })
            .await;
        let (epoch, subtree_id) = match mounted {
            Evidence::Mounted {
                epoch, subtree_id, ..
            } => (epoch, subtree_id),
            _ => unreachable!(),
        };
        self.wait_completed(12).await;
        self.next_evidence("idle eviction marker", |event| {
            matches!(event, Evidence::EvictionRequested { chat, epoch: observed } if chat == "alpha" && *observed == epoch)
        })
        .await;
        self.next_evidence("router enters Removing", |event| {
            matches!(event, Evidence::Removing { chat, epoch: observed, .. } if chat == "alpha" && *observed == epoch)
        })
        .await;
        poll_until("removal offload held for race", || {
            self.removal_gate.is_waiting()
        })
        .await;

        self.publish(Envelope::new(13, "alpha", "raced eviction"))
            .await;
        self.removal_gate.hold(false);
        self.next_evidence("old incarnation removed", |event| {
            matches!(event, Evidence::Removed { chat, epoch: observed, .. } if chat == "alpha" && *observed == epoch)
        })
        .await;
        let remounted = self
            .next_evidence("racing message remount", |event| {
                matches!(event, Evidence::Mounted { chat, epoch: observed, .. } if chat == "alpha" && *observed > epoch)
            })
            .await;
        let (new_epoch, new_id) = match remounted {
            Evidence::Mounted {
                epoch, subtree_id, ..
            } => (epoch, subtree_id),
            _ => unreachable!(),
        };
        self.next_evidence("fresh session replay", |event| {
            matches!(event, Evidence::Rehydrated { chat, epoch, messages } if chat == "alpha" && *epoch == new_epoch && *messages >= 13)
        })
        .await;
        self.wait_completed(13).await;
        self.session_settings.enable_idle_eviction(false);
        self.active_session = Some((new_epoch, new_id));
        assert_ne!(subtree_id, self.active_session.as_ref().unwrap().1);
        assert!(self.journal.lock().await.entries().iter().any(|entry| {
            matches!(entry, JournalEntry::Evicted { chat, epoch: observed } if chat == "alpha" && *observed == epoch)
        }));
        assert!(self.journal.lock().await.entries().iter().any(|entry| {
            matches!(entry, JournalEntry::Checkpoint { chat, .. } if chat == "alpha")
        }));
        warn!(
            phase = 8,
            old_epoch,
            raced_epoch = epoch,
            new_epoch,
            "router rebirth swept its orphan; raced eviction remounted and replayed"
        );
        Ok(())
    }

    async fn phase_staged_shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        let conflated = self.progress.stats().messages_conflated;
        let blocking_runs = self.tools.blocking_runs();
        let journal_entries = self.journal.lock().await.entries().len();
        let duplicate_envelopes = self.journal.lock().await.duplicate_envelopes();
        let gateway_restarts = self.gateway.snapshot().total_restarts;
        let core_restarts = self.core.snapshot().total_restarts;

        self.root.request_shutdown();
        let mut stop_order = Vec::new();
        while !stop_order.contains(&"journal") {
            let evidence = tokio::time::timeout(WAIT_BOUND, self.evidence.recv())
                .await
                .expect("shutdown evidence must be bounded")
                .expect("evidence stream remains live");
            if let Evidence::ActorStopped(actor) = evidence {
                stop_order.push(actor);
            }
        }
        let running = self.running.take().expect("running tree owned");
        tokio::time::timeout(WAIT_BOUND, running.wait()).await??;
        let bridge = stop_order
            .iter()
            .position(|actor| *actor == "bridge")
            .expect("bridge stopped");
        let journal = stop_order
            .iter()
            .position(|actor| *actor == "journal")
            .expect("journal stopped");
        assert!(bridge < journal, "intake must close before durable core");
        warn!(
            phase = 9,
            journal_entries,
            duplicate_envelopes,
            blocking_runs,
            conflated,
            gateway_restarts,
            core_restarts,
            ?stop_order,
            "staged drain complete"
        );
        Ok(())
    }

    async fn publish(&self, envelope: Envelope) {
        let id = envelope.id;
        self.transport.publish(envelope).await;
        self.wait_acked(id).await;
    }

    async fn wait_acked(&self, envelope_id: u64) {
        tokio::time::timeout(WAIT_BOUND, async {
            while !self.transport.is_acked(envelope_id).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("envelope {envelope_id} was not acked"));
    }

    async fn wait_completed(&mut self, envelope_id: u64) {
        self.next_evidence("run completion", |event| {
            matches!(event, Evidence::RunCompleted { envelope_id: observed, .. } if *observed == envelope_id)
        })
        .await;
    }

    async fn wait_gate(&mut self, open: bool, reason: &str) {
        self.next_evidence("gate transition", |event| {
            matches!(event, Evidence::GateChanged { open: observed, reason: observed_reason } if *observed == open && observed_reason.contains(reason))
        })
        .await;
    }

    async fn assert_notice_fanout(&mut self, open: bool) {
        let first = tokio::time::timeout(WAIT_BOUND, self.notice_a.recv())
            .await
            .expect("first fan-out receiver bounded")
            .expect("first fan-out receiver live");
        let second = tokio::time::timeout(WAIT_BOUND, self.notice_b.recv())
            .await
            .expect("second fan-out receiver bounded")
            .expect("second fan-out receiver live");
        assert_eq!(first.open, open);
        assert_eq!(second.open, open);
    }

    async fn next_evidence(
        &mut self,
        label: &str,
        mut predicate: impl FnMut(&Evidence) -> bool,
    ) -> Evidence {
        if let Some(index) = self.evidence_backlog.iter().position(&mut predicate) {
            return self
                .evidence_backlog
                .remove(index)
                .expect("located evidence remains in backlog");
        }
        tokio::time::timeout(WAIT_BOUND, async {
            loop {
                let evidence = self.evidence.recv().await.expect("evidence stream live");
                if predicate(&evidence) {
                    return evidence;
                }
                self.evidence_backlog.push_back(evidence);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
    }

    async fn wait_run_edge(&mut self, run_id: &str, exited: bool) -> LifecycleEvent {
        tokio::time::timeout(WAIT_BOUND, async {
            loop {
                let event = self.lifecycle.next().await.expect("lifecycle stream live");
                let LifecycleEventKind::Child(child) = &event.kind else {
                    continue;
                };
                if child.child_id != run_id {
                    continue;
                }
                if (exited && matches!(child.kind, ChildEventKind::Exited { .. }))
                    || (!exited && matches!(child.kind, ChildEventKind::Removed))
                {
                    return event;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for lifecycle edge of {run_id}"))
    }
}

async fn poll_until(label: &str, mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT_BOUND, async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out polling for {label}"));
}
