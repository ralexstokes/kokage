//! A deterministic, in-process control plane for a personal multi-agent assistant.
//!
//! The example is an assertion-driven acceptance script for dynamic actor
//! lifecycles and the runtime surfaces not covered by `trading_engine`:
//! per-conversation subtrees added and removed at runtime via
//! `DynamicRuntime::add_subtree`, never-restarted transient children observed
//! through `ctx.watch`, `continue_with` rehydration, `run_blocking` effects,
//! a readiness-gated `RawActor` bridge, and a `#[derive(Supervision)]` supervision
//! tree with a real budget ↔ guard cycle.
//!
//! # Modules
//!
//! | module      | role                                                       |
//! |-------------|------------------------------------------------------------|
//! | `chat`      | `ChatSim`: in-process chat transport; redelivers until ack |
//! | `model`     | `ModelClient` seam + deterministic `ScriptedModel`         |
//! | `gateway`   | `outbound` (FIFO, drains), `progress` (conflated by chat), |
//! |             | `inbound` (raw readiness-gated bridge; panics on drop)     |
//! | `journal`   | append-only transcript/effect log; envelope dedup; replay  |
//! | `budget`    | token-spend metering; reports `BudgetExceeded` to guard    |
//! | `guard`     | recoverable breaker: closes the shared intake gate, probes |
//! | `tool_host` | idempotent-by-key effect execution under `run_blocking`    |
//! | `router`    | single writer of session membership; mounts one subtree    |
//! |             | per conversation, never awaits the control plane, buffers  |
//! |             | only while a mount or removal offload is in flight            |
//! | `session`   | per-chat orchestrator (static in its subtree's builder);   |
//! |             | owns its transient run children                            |
//! | `run`       | one role run: mailbox-driven state machine, never restarted|
//! | `messages`  | shared ids, protocol enums, reports, timing constants      |
//! | `telemetry` | application-owned latency aggregates for the final dump    |
//!
//! # Supervision shape
//!
//! The shape below is one `#[derive(Supervision)]` declaration (`AgentControl`),
//! so struct nesting is scope nesting and every actor label is qualified by its
//! scope path (`gateway.inbound`, `core.journal`). Supervisor child ids stay
//! local, so the paths read `root.gateway.inbound` and `root.core.journal`.
//!
//! ```text
//! root (OneForOne)                     — struct AgentControl
//! ├── gateway   RestForOne, sequential start: outbound → progress → inbound
//! │             (inbound is last: its panic restarts only inbound; an
//! │              outbound/progress failure also restarts the bridge)
//! ├── core      OneForOne
//! │             journal · budget · guard · tool_host · router
//! │             (budget ─BudgetExceeded→ guard, guard ─UnderCap?→ budget
//! │              is the cycle that justifies the derive)
//! └── sessions  empty subtree mount; per-conversation subtrees at runtime
//!               (a `DynamicScope` field, so the router can capture its mount
//!                handle at wiring time)
//!     └── session:<chat>#<epoch>   add_subtree, OneForAll; the epoch makes
//!         │                        every incarnation's id unique, so respawn
//!         │                        never races a predecessor's removal
//!         ├── session              static in the builder; reborn with the
//!         │                        subtree, rehydrates from the journal
//!         └── run:<task>:<role>:<attempt>
//!                                  add_actor, restart = Never;
//!                                  a session panic tears its runs down with
//!                                  it (OneForAll), while a Never run panic
//!                                  is skipped by the group respawn
//! ```
//!
//! # Data flow
//!
//! ```text
//! ChatSim ──delivery──▶ inbound ──append──▶ journal ──replay──▶ session
//!    ▲                     │ ack only after append       (continue_with)
//!    │                     ▼
//!    │                   router ──forward/spawn──▶ session:<chat>
//!    │                                                │ add/watch/remove
//!    │                                                ▼
//!    │                                    run:<chat>:<task>:<role>
//!    │                                     │ model turn in bounded offload
//!    │                                     │ tool calls: ToolIntent journaled,
//!    │                                     ▼ then Execute under deadline
//!    │                                  tool_host ──(timeout? Query key)──▶ run
//!    │
//!    ├◀─conflated deltas/typing── progress ◀── runs + session heartbeat
//!    └◀─replies and notices────── outbound ◀── session (drains on shutdown)
//!
//! guard inputs: session run failures, budget cap, bridge restart totals
//! guard output: shared intake gate (Arc<AtomicBool>) + PauseChanged fan-out
//!               via router; send_after probes with backoff lift the pause
//! ```
//!
//! # Lifecycles
//!
//! ```text
//! session:<chat>#<epoch>   (dynamic subtree, mounted on first message)
//!   add_subtree ─▶ on_start: mark ready ─▶ continue_with(Rehydrate): replay
//!     ─▶ UserMessage: start planner run, suppress idle timer, heartbeat
//!     ─▶ RunFinished: planner → engineer → reviewer → Reply, re-arm idle
//!     ─▶ IdleSweep (current state timeout, no run): Checkpoint + Evicted, then
//!         Evict naming this subtree, re-sent each sweep until teardown lands
//!         — the router buffers while it drops the subtree; a late arrival is
//!         bounced back and rides the buffer into a replacement subtree
//!     ─▶ removed; next message mounts a fresh epoch, replay restores context
//!
//! run:<task>:<role>:<attempt>   (transient child, restart = Never)
//!   add_actor ─▶ continue_with(Step)
//!     ─▶ model turn in a context-owned offload (cancel token + deadline) ─▶ ModelResult
//!     ─▶ tool loop: journal ToolIntent ─▶ Execute (bounded) ─▶ ToolResult,
//!         reconciling an unknown outcome through an idempotency-key Query
//!     ─▶ RunFinished{output} to the session + ctx.stop(); terminal exit auto-removes
//!     ─▶ on panic: Down(Failure) then Terminated to the session's watch;
//!         the session reports the failure and spawns a fresh attempt
//! ```
//!
//! `main` runs phases 0–8. No socket is opened and no wall-clock sleep is used
//! as a correctness assertion; every asynchronous observation is bounded.

mod budget;
mod chat;
mod gateway;
mod guard;
mod journal;
mod messages;
mod model;
mod router;
mod run;
mod session;
mod telemetry;
mod tool_host;

use std::{
    error::Error,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{
    DownReason, DynamicScope, MailboxMode, MonitorEvent, Supervision, observe::LifecycleWatchGuard,
    prelude::*,
};
use tokio::time::Instant;

use budget::Budget;
use chat::ChatSim;
use gateway::{Inbound, Outbound, Progress};
use guard::Guard;
use journal::Journal;
use messages::*;
use model::{ModelClient, ScriptedModel};
use router::{Router, RouterFactory};
use telemetry::LatencyRecorder;
use tool_host::ToolHost;

type AnyError = Box<dyn Error + Send + Sync>;

/// Gateway actors keep the 32-deep mailbox the separate `agent-gateway` graph
/// gave them before the scopes merged into one graph; core actors keep the
/// graph-wide default.
const GATEWAY_MAILBOX: usize = 32;

fn gateway_options<M: Send + 'static>() -> ActorOptions<M> {
    ActorOptions::new().mailbox_capacity(GATEWAY_MAILBOX)
}

/// Sequential start: outbound → progress → inbound. `RestForOne` puts the
/// bridge last, so an inbound panic restarts only inbound while an
/// outbound/progress failure also recycles the bridge that depends on them.
#[derive(Supervision)]
#[supervision(strategy = Strategy::RestForOne)]
struct Gateway {
    #[supervision(options = gateway_options())]
    outbound: Outbound,
    #[supervision(options = gateway_options()
        .mailbox(MailboxMode::conflate_by_key(|message: &ProgressMsg| message.chat())))]
    progress: Progress,
    #[supervision(options = gateway_options())]
    inbound: Inbound,
}

/// The budget↔guard cycle is what justifies deriving rather than ordering
/// registrations by hand.
#[derive(Supervision)]
struct Core {
    #[supervision(options = ActorOptions::new().message_size(messages::journal_message_size))]
    journal: Journal,
    budget: Budget,
    guard: Guard,
    tool_host: ToolHost,
    router: Router,
}

/// The whole application. `sessions` is a dynamic scope: its builder is wired
/// like any other field, which is what lets the router capture its mount handle
/// before any actor is constructed.
#[derive(Supervision)]
struct AgentControl {
    #[supervision(scope)]
    gateway: Gateway,
    #[supervision(scope)]
    core: Core,
    sessions: DynamicScope,
}

struct App {
    runtime: kokage::Runtime,
    gateway: RuntimeHandle,
    core: RuntimeHandle,
    sessions: RuntimeHandle,
    chat: ChatSim,
    model: ScriptedModel,
    router: ActorRef<RouterMsg>,
    journal: ActorRef<JournalMsg>,
    budget: ActorRef<BudgetMsg>,
    guard: ActorRef<GuardMsg>,
    tool_host: ActorRef<ToolHostMsg>,
    proof: Proof,
    gate: Arc<AtomicBool>,
    lifecycle_watch: LifecycleWatchGuard,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init()?;
    let latency = LatencyRecorder::default();
    let app = build_app().await?;

    phase_0(&app).await?;
    phase_1(&app, &latency).await?;
    phase_2(&app).await?;
    phase_3(&app).await?;
    phase_4(&app, &latency).await?;
    phase_5(&app).await?;
    phase_6(&app).await?;
    phase_7(&app).await?;
    phase_8(app, latency).await?;
    Ok(())
}

async fn build_app() -> Result<App, AnyError> {
    let chat = ChatSim::default();
    let model = ScriptedModel::default();
    let model_client: Arc<dyn ModelClient> = Arc::new(model.clone());
    let gate = Arc::new(AtomicBool::new(true));
    let proof = Proof::default();
    // Router state dies with a router incarnation; the sessions mount handle
    // and the subtree-id allocator must not, or a reborn router could not
    // reach the mount and would re-mint ids that still exist. Both are durable
    // RouterFactory fields instead.
    let sessions_runtime = DynamicTree::new();
    let sessions_mount = sessions_runtime.handle();
    let session_epoch = Arc::new(AtomicU64::new(0));

    // Every actor joins one graph, so the wiring closure sees gateway and core
    // refs together: the bridge captures `core.router` and the router captures
    // `gateway.outbound` in the same literal, with no slot/define split and no
    // ordering between the two scopes.
    let mut builder = kokage::GraphBuilder::new();
    builder.name("agent-control");
    let (tree, refs) = AgentControl::tree_with(builder, |refs| AgentControlFactories {
        gateway: GatewayFactories {
            outbound: {
                let chat = chat.clone();
                move || Outbound::new(chat.clone())
            },
            progress: {
                let chat = chat.clone();
                move || Progress::new(chat.clone())
            },
            inbound: {
                let chat = chat.clone();
                let journal = refs.core.journal.clone();
                let router = refs.core.router.clone();
                move || Inbound::new(chat.clone(), journal.clone(), router.clone())
            },
        },
        core: CoreFactories {
            journal: Journal::default,
            budget: {
                let refs = refs.core.clone();
                move || Budget::new(refs.guard.clone())
            },
            guard: {
                let refs = refs.core.clone();
                let model = model.clone();
                let gate = gate.clone();
                move || {
                    Guard::new(
                        refs.budget.clone(),
                        refs.router.clone(),
                        model.clone(),
                        gate.clone(),
                    )
                }
            },
            tool_host: ToolHost::default,
            router: RouterFactory {
                mount: sessions_mount.clone(),
                journal: refs.core.journal.clone(),
                budget: refs.core.budget.clone(),
                tool_host: refs.core.tool_host.clone(),
                guard: refs.core.guard.clone(),
                outbound: refs.gateway.outbound.clone(),
                progress: refs.gateway.progress.clone(),
                gate: gate.clone(),
                model: model_client.clone(),
                session_epoch: session_epoch.clone(),
                proof: proof.clone(),
            },
        },
        sessions: sessions_runtime,
    })?;
    let CoreRefs {
        journal,
        budget,
        guard,
        tool_host,
        router,
    } = refs.core;

    let runtime = tree.spawn()?;
    let gateway = runtime
        .handle()
        .subtree("gateway")
        .expect("gateway runtime subtree");
    let core = runtime
        .handle()
        .subtree("core")
        .expect("core runtime subtree");
    // `sessions_mount` was issued before the root existed and addresses the
    // same identity the post-spawn `runtime.subtree("sessions")` lookup would
    // return, so the phases below drive it directly.
    let sessions = sessions_mount.clone();
    let lifecycle_watch = gateway.watch_lifecycle_to(&guard, |event| GuardMsg::BridgeRestarts {
        total: event.total_restarts,
    });

    Ok(App {
        runtime,
        gateway,
        core,
        sessions,
        chat,
        model,
        router,
        journal,
        budget,
        guard,
        tool_host,
        proof,
        gate,
        lifecycle_watch,
    })
}

async fn phase_0(app: &App) -> Result<(), AnyError> {
    tokio::time::timeout(INIT_TIMEOUT, app.runtime.handle().wait_started()).await??;
    assert_eq!(app.chat.sessions(), 1);
    assert!(app.sessions.snapshot().children.is_empty());
    assert!(!paused(&app.guard).await?);
    println!("PHASE 0 OK — RawActor readiness_gated + mark_ready; pre-spawn dynamic subtree mount");
    Ok(())
}

async fn phase_1(app: &App, latency: &LatencyRecorder) -> Result<(), AnyError> {
    let replies_before = app.chat.replies(CHAT_A).len();
    let started = Instant::now();
    app.chat.inject_user_message(CHAT_A, "OK");
    await_until(|| async { has_session(&app.sessions, CHAT_A) }).await?;
    await_until(|| async { app.chat.replies(CHAT_A).len() > replies_before }).await?;
    latency.record("message.path", started.elapsed());
    let report = journal_report(&app.journal).await?;
    let kinds = report
        .entries
        .iter()
        .filter(|entry| entry.chat == CHAT_A)
        .map(|entry| &entry.entry)
        .collect::<Vec<_>>();
    assert!(matches!(
        kinds.first(),
        Some(JournalEntry::UserMessage { .. })
    ));
    assert_eq!(
        kinds
            .iter()
            .filter(|entry| matches!(entry, JournalEntry::ToolIntent { .. }))
            .count(),
        2
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|entry| matches!(entry, JournalEntry::ToolEffect { .. }))
            .count(),
        2
    );
    assert!(matches!(kinds.last(), Some(JournalEntry::Reply { .. })));
    assert_eq!(budget_report(&app.budget).await?.total, 40);
    assert_eq!(app.chat.acks(), 1);
    assert_eq!(app.chat.replies(CHAT_A).len(), 1);
    assert!(app.chat.progress_count(CHAT_A) > 0);
    println!(
        "PHASE 1 OK — DynamicRuntime::add_subtree per conversation; continue_with; interval_to"
    );
    Ok(())
}

async fn phase_2(app: &App) -> Result<(), AnyError> {
    let b_replies = app.chat.replies(CHAT_B).len();
    app.chat.inject_user_message(CHAT_B, "OK SLOW");
    await_until(|| async {
        app.proof
            .lock()
            .expect("proof lock poisoned")
            .session_generations
            .contains_key(CHAT_B)
    })
    .await?;
    let b_generation = app
        .proof
        .lock()
        .expect("proof lock poisoned")
        .session_generations[CHAT_B];
    let a_replies = app.chat.replies(CHAT_A).len();
    app.chat.inject_user_message(CHAT_A, "PANIC-MIDRUN");
    await_until(|| async { app.chat.replies(CHAT_A).len() > a_replies }).await?;
    await_until(|| async { app.chat.replies(CHAT_B).len() > b_replies }).await?;
    let proof = app.proof.lock().expect("proof lock poisoned").clone();
    let panic_events = proof
        .monitor_events
        .values()
        .find(|events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MonitorEvent::Down {
                        reason: DownReason::Failure,
                        ..
                    }
                )
            })
        })
        .expect("panic run monitor events");
    let down = panic_events
        .iter()
        .position(|event| matches!(event, MonitorEvent::Down { .. }))
        .expect("Down event");
    let terminated = panic_events
        .iter()
        .position(|event| matches!(event, MonitorEvent::Terminated { .. }))
        .expect("Terminated event");
    assert!(
        down < terminated,
        "never-restart child reports Down then Terminated"
    );
    let tool_report = tool_report(&app.tool_host).await?;
    let panic_key = tool_report
        .effects
        .keys()
        .find(|key| key.starts_with("chat-a:") && key.ends_with(":0"))
        .expect("panic tool key");
    assert_eq!(tool_report.effects[panic_key], 1);
    assert_eq!(guard_report(&app.guard).await?.run_failures, 1);
    assert_eq!(
        app.proof
            .lock()
            .expect("proof lock poisoned")
            .session_generations[CHAT_B],
        b_generation
    );
    println!("PHASE 2 OK — add_actor(RestartPolicy::Never) + ctx.watch Down/Terminated");
    Ok(())
}

async fn phase_3(app: &App) -> Result<(), AnyError> {
    let session_restarts = app.sessions.snapshot().total_restarts;
    let replies = app.chat.replies(CHAT_A).len();
    let envelope = app
        .chat
        .drop_session_and_inject(CHAT_A, "OK bridge-redelivery");
    await_until(|| async { app.chat.sessions() >= 2 }).await?;
    await_until(|| async { app.chat.replies(CHAT_A).len() > replies }).await?;
    let report = journal_report(&app.journal).await?;
    assert_eq!(
        report
            .entries
            .iter()
            .filter(|entry| matches!(entry.entry, JournalEntry::UserMessage { envelope: id, .. } if id == envelope))
            .count(),
        1
    );
    assert!(report.duplicate_appends >= 1);
    assert!(app.chat.presentations() > app.chat.acks());
    await_until(|| async {
        guard_report(&app.guard)
            .await
            .is_ok_and(|report| report.bridge_restarts >= 1)
    })
    .await?;
    // Pinned divergence from the spec's phase-3 wording: RestForOne restarts
    // the failed child plus the children started *after* it, and inbound is
    // last in start order — so an inbound panic restarts inbound alone, never
    // the trio.
    let gateway_children = app.gateway.snapshot().children;
    let restarts = |id: &str| {
        gateway_children
            .iter()
            .find(|child| child.id == id)
            .map(|child| child.restart_count)
    };
    assert_eq!(restarts("inbound"), Some(1));
    assert_eq!(restarts("outbound"), Some(0));
    assert_eq!(restarts("progress"), Some(0));
    assert_eq!(app.sessions.snapshot().total_restarts, session_restarts);
    println!(
        "PHASE 3 OK — ack-after-append redelivery + RestForOne restart watch (only failed final child restarts)"
    );
    Ok(())
}

async fn phase_4(app: &App, latency: &LatencyRecorder) -> Result<(), AnyError> {
    let replies = app.chat.replies(CHAT_A).len();
    let started = Instant::now();
    app.chat.inject_user_message(CHAT_A, "STALL-TOOL");
    await_until(|| async { app.chat.replies(CHAT_A).len() > replies }).await?;
    let elapsed = started.elapsed();
    latency.record("tool.path", elapsed);
    assert!(elapsed < Duration::from_secs(2));
    let report = tool_report(&app.tool_host).await?;
    let key = report
        .queries
        .keys()
        .find(|key| key.starts_with("chat-a:"))
        .expect("stalled tool query");
    assert_eq!(report.queries[key], 1);
    assert_eq!(report.effects[key], 1);
    println!("PHASE 4 OK — bounded call reconciliation + ctx.run_blocking");
    Ok(())
}

async fn phase_5(app: &App) -> Result<(), AnyError> {
    let b_progress = app.chat.progress_count(CHAT_B);
    let cancel_started = Instant::now();
    app.chat.inject_user_message(CHAT_B, "FLOOD");
    await_until(|| async { app.chat.progress_count(CHAT_B) > b_progress }).await?;
    let a_replies = app.chat.replies(CHAT_A).len();
    app.chat
        .inject_user_message(CHAT_A, "OK concurrent-with-flood");
    app.chat.inject_user_message(CHAT_B, "stop");
    await_until_with(CANCEL_BOUND, || async {
        app.proof
            .lock()
            .expect("proof lock poisoned")
            .run_terminal_at
            .get(CHAT_B)
            .is_some_and(|at| *at >= cancel_started)
    })
    .await?;
    await_until(|| async { app.chat.replies(CHAT_A).len() > a_replies }).await?;
    let progress_stats = app
        .gateway
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == "gateway.progress")
        .expect("progress actor stats");
    assert!(progress_stats.messages_received < progress_stats.messages_accepted);
    assert!(progress_stats.messages_conflated > 0);
    assert!(
        journal_report(&app.journal)
            .await?
            .entries
            .iter()
            .any(|entry| {
                entry.chat == CHAT_B && matches!(entry.entry, JournalEntry::TaskCancelled { .. })
            })
    );
    println!("PHASE 5 OK — urgent CancellationToken + keyed conflation + timer cancellation");
    Ok(())
}

async fn phase_6(app: &App) -> Result<(), AnyError> {
    let failures_before = guard_report(&app.guard).await?.run_failures;
    app.model.set_rate_limited(true);
    app.chat.inject_user_message(CHAT_A, "OK outage-a");
    app.chat.inject_user_message(CHAT_B, "OK outage-b");
    await_until(|| async { paused(&app.guard).await.unwrap_or(false) }).await?;
    assert!(!app.gate.load(Ordering::Acquire));
    // Either session's retry loop may supply both failures that trip the
    // breaker before the other session starts. Pin the causal aggregate and
    // verify the cluster-wide pause separately through both chats' notices.
    assert!(guard_report(&app.guard).await?.run_failures > failures_before);
    await_until(|| async {
        [CHAT_A, CHAT_B].iter().all(|chat| {
            app.chat
                .replies(chat)
                .iter()
                .any(|reply| reply.contains("paused"))
        })
    })
    .await?;
    let starts = app
        .proof
        .lock()
        .expect("proof lock poisoned")
        .run_started
        .get(CHAT_A)
        .copied()
        .unwrap_or(0);
    let held_replies = app.chat.replies(CHAT_A).len();
    let acks_before = app.chat.acks();
    app.chat.inject_user_message(CHAT_A, "OK held-while-paused");
    await_until(|| async {
        journal_report(&app.journal).await.is_ok_and(|report| {
            report.entries.iter().any(|entry| {
                entry.chat == CHAT_A
                    && matches!(&entry.entry, JournalEntry::UserMessage { text, .. } if text.contains("held-while-paused"))
            })
        })
    })
    .await?;
    // The pause gates run creation, not intake: the held message is still
    // journaled and acked to the transport.
    await_until(|| async { app.chat.acks() > acks_before }).await?;
    assert_eq!(
        app.proof
            .lock()
            .expect("proof lock poisoned")
            .run_started
            .get(CHAT_A)
            .copied()
            .unwrap_or(0),
        starts
    );
    await_until(|| async {
        guard_report(&app.guard)
            .await
            .is_ok_and(|report| report.failed_probes >= 1)
    })
    .await?;
    app.model.set_rate_limited(false);
    await_until(|| async { !paused(&app.guard).await.unwrap_or(true) }).await?;
    await_until(|| async { app.chat.replies(CHAT_A).len() > held_replies }).await?;

    let total = budget_report(&app.budget).await?.total;
    app.budget
        .send(BudgetMsg::SetGlobalCap {
            tokens: total.saturating_sub(1),
        })
        .await?;
    app.chat.inject_user_message(CHAT_B, "OK budget-trip");
    // No harness-side charge: with the cap below current spend, the very next
    // model-turn charge on the ordinary run path — guaranteed by the message
    // just injected — pushes total past cap and trips the guard.
    await_until(|| async { paused(&app.guard).await.unwrap_or(false) }).await?;
    app.budget
        .send(BudgetMsg::SetGlobalCap { tokens: u64::MAX })
        .await?;
    await_until(|| async { !paused(&app.guard).await.unwrap_or(true) }).await?;
    println!("PHASE 6 OK — #[derive(Supervision)] budget↔guard cycle + recoverable probe backoff");
    Ok(())
}

async fn phase_7(app: &App) -> Result<(), AnyError> {
    if let Err(error) = await_until_with(Duration::from_secs(6), || async {
        !has_session(&app.sessions, CHAT_A)
    })
    .await
    {
        eprintln!("phase 7 eviction timeout: {:#?}", app.sessions.snapshot());
        return Err(error);
    }
    let progress = app.chat.progress_count(CHAT_A);
    await_stable(|| app.chat.progress_count(CHAT_A), progress).await?;
    let generation_before = app
        .proof
        .lock()
        .expect("proof lock poisoned")
        .session_generations[CHAT_A];
    let replies = app.chat.replies(CHAT_A).len();
    app.chat.inject_user_message(CHAT_A, "OK respawn-one");
    app.chat.inject_user_message(CHAT_A, "OK respawn-two");
    await_until(|| async { app.chat.replies(CHAT_A).len() >= replies + 2 }).await?;
    let proof = app.proof.lock().expect("proof lock poisoned").clone();
    assert!(proof.session_generations[CHAT_A] > generation_before);
    assert!(proof.session_ready_at[CHAT_A] < proof.session_rehydrated_at[CHAT_A]);
    assert!(
        app.chat
            .replies(CHAT_A)
            .last()
            .is_some_and(|reply| !reply.contains("prior-context=0"))
    );
    let report = journal_report(&app.journal).await?;
    assert!(report.entries.iter().any(
        |entry| entry.chat == CHAT_A && matches!(entry.entry, JournalEntry::Checkpoint { .. })
    ));
    assert!(
        report
            .entries
            .iter()
            .any(|entry| entry.chat == CHAT_A && matches!(entry.entry, JournalEntry::Evicted))
    );

    // Second eviction cycle, this time injecting the moment the journal
    // shows the next Evicted entry, while the session's Evict may still be
    // in flight to the router. Whichever side wins, the message must be
    // answered with replayed context: the router forwards it to the retiring
    // session (whose bounce lands behind the Evict), buffers it while the
    // removal offload is in flight, or finds no slot and mints the replacement
    // directly.
    let evicted_count = |report: &JournalReport| {
        report
            .entries
            .iter()
            .filter(|entry| entry.chat == CHAT_A && matches!(entry.entry, JournalEntry::Evicted))
            .count()
    };
    let evictions_before = evicted_count(&report);
    let generation_mid = app
        .proof
        .lock()
        .expect("proof lock poisoned")
        .session_generations[CHAT_A];
    let replies = app.chat.replies(CHAT_A).len();
    await_until(|| async {
        journal_report(&app.journal)
            .await
            .is_ok_and(|report| evicted_count(&report) > evictions_before)
    })
    .await?;
    app.chat.inject_user_message(CHAT_A, "OK racing-evict");
    await_until(|| async { app.chat.replies(CHAT_A).len() > replies }).await?;
    let proof = app.proof.lock().expect("proof lock poisoned").clone();
    assert!(proof.session_generations[CHAT_A] > generation_mid);
    assert!(
        app.chat
            .replies(CHAT_A)
            .last()
            .is_some_and(|reply| !reply.contains("prior-context=0"))
    );
    println!(
        "PHASE 7 OK — timeout cancellation + subtree removal/respawn + \
         readiness-before-rehydrate + raced eviction absorbed without membership protocol"
    );
    Ok(())
}

async fn phase_8(app: App, latency: LatencyRecorder) -> Result<(), AnyError> {
    app.gate.store(false, Ordering::Release);
    app.router.send(RouterMsg::Stop { chat: CHAT_A }).await?;
    app.router.send(RouterMsg::Stop { chat: CHAT_B }).await?;
    app.router
        .send(RouterMsg::PauseChanged { paused: true })
        .await?;
    await_until(|| async {
        let report = journal_report(&app.journal).await.ok();
        report.is_some_and(|report| {
            let user_messages = report
                .entries
                .iter()
                .filter(|entry| matches!(entry.entry, JournalEntry::UserMessage { .. }))
                .count();
            user_messages == app.chat.acks() && all_assigned_tasks_terminal(&report)
        })
    })
    .await?;
    let journal_stats = app
        .core
        .actor_stats()
        .into_iter()
        .find(|stats| stats.actor_id == "core.journal")
        .expect("journal stats");
    assert!(
        journal_stats
            .message_bytes_accepted
            .is_some_and(|bytes| bytes > 0)
    );
    let recursive_stats = app.runtime.handle().actor_stats();
    let session_stats = app.sessions.actor_stats();
    let final_snapshot = app.runtime.handle().snapshot();
    drop(app.lifecycle_watch);
    tokio::time::timeout(Duration::from_secs(5), app.runtime.shutdown_and_wait()).await??;
    let latency = latency.snapshot();
    assert!(
        latency
            .get("message.path")
            .is_some_and(|series| series.count > 0)
    );
    assert!(
        latency
            .get("tool.path")
            .is_some_and(|series| series.count > 0)
    );
    println!("latency summary: {latency:#?}");
    println!("journal message-size stats: {journal_stats:#?}");
    println!("recursive actor stats: {recursive_stats:#?}");
    println!("sessions actor stats: {session_stats:#?}");
    println!("final supervisor snapshot: {final_snapshot:#?}");
    println!("PHASE 8 OK — DrainPolicy::Drain staged shutdown + recursive telemetry");
    Ok(())
}

fn all_assigned_tasks_terminal(report: &JournalReport) -> bool {
    let mut assigned = std::collections::HashSet::new();
    let mut terminal = std::collections::HashSet::new();
    for stored in &report.entries {
        let task = match &stored.entry {
            JournalEntry::Plan { task, .. }
            | JournalEntry::ToolIntent { task, .. }
            | JournalEntry::ToolEffect { task, .. }
            | JournalEntry::Review { task, .. } => Some(*task),
            JournalEntry::Reply { task, .. }
            | JournalEntry::Checkpoint { task, .. }
            | JournalEntry::TaskCancelled { task } => {
                terminal.insert(*task);
                Some(*task)
            }
            JournalEntry::UserMessage { .. } | JournalEntry::Evicted => None,
        };
        if let Some(task) = task {
            assigned.insert(task);
        }
    }
    assigned.is_subset(&terminal)
}

fn has_session(handle: &RuntimeHandle, chat: ChatId) -> bool {
    let prefix = format!("session:{chat}#");
    handle
        .snapshot()
        .children
        .iter()
        .any(|child| child.id.starts_with(&prefix))
}

async fn paused(guard: &ActorRef<GuardMsg>) -> Result<bool, AnyError> {
    bounded_call(guard, |reply| GuardMsg::Paused { reply }).await
}

async fn journal_report(journal: &ActorRef<JournalMsg>) -> Result<JournalReport, AnyError> {
    bounded_call(journal, |reply| JournalMsg::Report { reply }).await
}

async fn budget_report(budget: &ActorRef<BudgetMsg>) -> Result<BudgetReport, AnyError> {
    bounded_call(budget, |reply| BudgetMsg::Report { reply }).await
}

async fn guard_report(guard: &ActorRef<GuardMsg>) -> Result<GuardReport, AnyError> {
    bounded_call(guard, |reply| GuardMsg::Report { reply }).await
}

async fn tool_report(tool_host: &ActorRef<ToolHostMsg>) -> Result<ToolReport, AnyError> {
    bounded_call(tool_host, |reply| ToolHostMsg::Report { reply }).await
}

async fn bounded_call<M, T>(
    actor: &ActorRef<M>,
    make: impl FnOnce(Reply<T>) -> M,
) -> Result<T, AnyError>
where
    M: Send + 'static,
    T: Send + 'static,
{
    Ok(actor.call(PHASE_TIMEOUT, make).await?)
}

async fn await_until<F, Fut>(predicate: F) -> Result<(), AnyError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    await_until_with(PHASE_TIMEOUT, predicate).await
}

async fn await_until_with<F, Fut>(duration: Duration, mut predicate: F) -> Result<(), AnyError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(duration, async move {
        loop {
            if predicate().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

async fn await_stable(mut read: impl FnMut() -> usize, expected: usize) -> Result<(), AnyError> {
    tokio::time::timeout(TYPING_PERIOD * 3, async move {
        loop {
            assert_eq!(read(), expected, "progress changed after session eviction");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect_err("stability observation intentionally reaches its bound");
    Ok(())
}
