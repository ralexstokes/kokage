//! Shared identifiers, protocol messages, reports, and timing constants.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;
use tokio_otp::{ActorRef, MonitorEvent, Reply};

pub type ChatId = &'static str;
pub type EnvelopeId = u64;
pub type TaskId = u64;
pub type EffectKey = String;

pub const CHAT_A: ChatId = "chat-a";
pub const CHAT_B: ChatId = "chat-b";
pub const INIT_TIMEOUT: Duration = Duration::from_secs(2);
pub const PHASE_TIMEOUT: Duration = Duration::from_secs(3);
pub const MODEL_DEADLINE: Duration = Duration::from_millis(500);
pub const TOOL_DEADLINE: Duration = Duration::from_millis(200);
pub const IDLE_TIMEOUT: Duration = Duration::from_millis(350);
pub const TYPING_PERIOD: Duration = Duration::from_millis(20);
pub const GUARD_WINDOW: Duration = Duration::from_secs(1);
pub const GUARD_THRESHOLD: usize = 2;
pub const PROBE_BACKOFF_BASE: Duration = Duration::from_millis(40);
pub const CANCEL_BOUND: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Role {
    Planner,
    Engineer,
    Reviewer,
}

#[derive(Clone, Debug)]
pub struct ChatDelivery {
    pub envelope: EnvelopeId,
    pub chat: ChatId,
    pub text: String,
}

#[derive(Debug)]
pub enum InboundMsg {
    Delivery(ChatDelivery),
    Disconnected,
}

#[derive(Debug)]
pub enum OutboundMsg {
    Reply { chat: ChatId, text: String },
    Notice { chat: ChatId, text: String },
}

#[derive(Clone, Debug)]
pub enum ProgressMsg {
    Delta { chat: ChatId, line: String },
    Typing { chat: ChatId },
}

impl ProgressMsg {
    pub fn chat(&self) -> ChatId {
        match self {
            Self::Delta { chat, .. } | Self::Typing { chat } => chat,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalEntry {
    UserMessage {
        envelope: EnvelopeId,
        text: String,
    },
    Plan {
        task: TaskId,
        text: String,
    },
    ToolIntent {
        task: TaskId,
        key: EffectKey,
        call: String,
    },
    ToolEffect {
        task: TaskId,
        key: EffectKey,
        outcome: ToolOutcome,
    },
    Review {
        task: TaskId,
        approved: bool,
    },
    Reply {
        task: TaskId,
        text: String,
    },
    Checkpoint {
        task: TaskId,
        state: String,
    },
    TaskCancelled {
        task: TaskId,
    },
    Evicted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEntry {
    pub seq: u64,
    pub chat: ChatId,
    pub entry: JournalEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendAck {
    pub seq: u64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Default)]
pub struct JournalReport {
    pub entries: Vec<StoredEntry>,
    pub duplicate_appends: usize,
}

#[derive(Debug)]
pub enum JournalMsg {
    Append {
        chat: ChatId,
        entry: JournalEntry,
        reply: Reply<AppendAck>,
    },
    Replay {
        chat: ChatId,
        reply: Reply<Vec<StoredEntry>>,
    },
    Report {
        reply: Reply<JournalReport>,
    },
}

pub fn journal_message_size(message: &JournalMsg) -> usize {
    match message {
        JournalMsg::Append { entry, .. } => match entry {
            JournalEntry::UserMessage { text, .. }
            | JournalEntry::Plan { text, .. }
            | JournalEntry::Reply { text, .. }
            | JournalEntry::Checkpoint { state: text, .. } => text.len(),
            JournalEntry::ToolIntent { key, call, .. } => key.len() + call.len(),
            JournalEntry::ToolEffect { key, outcome, .. } => key.len() + outcome.output.len(),
            JournalEntry::Review { .. }
            | JournalEntry::TaskCancelled { .. }
            | JournalEntry::Evicted => 0,
        },
        JournalMsg::Replay { .. } | JournalMsg::Report { .. } => 0,
    }
}

#[derive(Clone, Debug, Default)]
pub struct BudgetReport {
    pub by_chat: HashMap<ChatId, u64>,
    pub total: u64,
    pub cap: u64,
}

#[derive(Debug)]
pub enum BudgetMsg {
    Charge { chat: ChatId, tokens: u64 },
    SetGlobalCap { tokens: u64 },
    UnderCap { reply: Reply<bool> },
    Report { reply: Reply<BudgetReport> },
}

#[derive(Clone, Debug, Default)]
pub struct GuardReport {
    pub paused: bool,
    pub run_failures: usize,
    pub run_failures_by_chat: HashMap<ChatId, usize>,
    pub bridge_restarts: u64,
    pub probes: usize,
    pub failed_probes: usize,
}

#[derive(Debug)]
pub enum GuardMsg {
    RunFailureObserved { chat: ChatId, task: TaskId },
    BudgetExceeded,
    BridgeRestarts { total: u64 },
    Probe,
    Paused { reply: Reply<bool> },
    Report { reply: Reply<GuardReport> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCall {
    pub name: String,
    pub payload: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolOutcome {
    pub output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectStatus {
    Found(ToolOutcome),
    Missing,
}

#[derive(Clone, Debug, Default)]
pub struct ToolReport {
    pub effects: HashMap<EffectKey, usize>,
    pub queries: HashMap<EffectKey, usize>,
}

#[derive(Debug)]
pub enum ToolHostMsg {
    Execute {
        key: EffectKey,
        call: ToolCall,
        reply: Reply<ToolOutcome>,
    },
    Query {
        key: EffectKey,
        reply: Reply<EffectStatus>,
    },
    Report {
        reply: Reply<ToolReport>,
    },
}

#[derive(Debug)]
pub enum RouterMsg {
    /// Ordered membership transition from the sessions mount alignment watch.
    MountLifecycle(tokio_otp::observe::ChildLifecycleEvent),
    UserMessage {
        envelope: EnvelopeId,
        chat: ChatId,
        text: String,
    },
    /// A session's request to retire the named incarnation. The subtree id
    /// doubles as the incarnation identity, so the router honors it only
    /// against the slot that minted it and sweeps anything else by name.
    Evict {
        chat: ChatId,
        subtree_id: String,
    },
    /// Completion of a pipelined `add_subtree` for the chat's `Mounting` slot.
    Mounted {
        chat: ChatId,
        ok: bool,
    },
    /// Completion of a pipelined `remove_child` for the chat's `Removing`
    /// slot.
    Reaped {
        chat: ChatId,
        subtree_id: String,
        done: bool,
    },
    /// Completion of a slot-less removal (orphan or stale-duplicate sweep).
    Swept {
        subtree_id: String,
        done: bool,
    },
    PauseChanged {
        paused: bool,
    },
    Stop {
        chat: ChatId,
    },
}

#[derive(Clone, Debug)]
pub enum RunOutput {
    Planned(String),
    Engineered(String),
    Reviewed(bool),
    RetryableFailure,
    Cancelled,
}

#[derive(Debug)]
pub enum SessionMsg {
    Rehydrate,
    UserMessage {
        envelope: EnvelopeId,
        text: String,
    },
    RunFinished {
        task: TaskId,
        role: Role,
        output: RunOutput,
    },
    RunEvent {
        task: TaskId,
        role: Role,
        event: MonitorEvent,
    },
    PauseChanged {
        paused: bool,
    },
    Stop,
    IdleSweep,
}

#[derive(Clone, Debug)]
pub enum ModelAction {
    Plan(String),
    Tools(Vec<ToolCall>),
    Complete(String),
    Review(bool),
}

#[derive(Clone, Debug)]
pub struct ModelTurn {
    pub tokens_spent: u64,
    pub action: ModelAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    RateLimited,
    Cancelled,
    Deadline,
}

#[derive(Debug)]
pub enum RunMsg {
    Step,
    ModelResult {
        result: Result<ModelTurn, ModelError>,
    },
    ToolResult {
        index: usize,
        key: EffectKey,
        result: ToolOutcome,
    },
}

#[derive(Clone)]
pub struct TurnRequest {
    pub chat: ChatId,
    pub task: TaskId,
    pub role: Role,
    pub attempt: u64,
    pub turn: u64,
    pub user_text: String,
    pub progress: ActorRef<ProgressMsg>,
}

#[derive(Clone, Debug)]
pub struct PendingInput {
    pub envelope: EnvelopeId,
    pub text: String,
}

#[derive(Clone, Debug, Default)]
pub struct ProofState {
    pub monitor_events: HashMap<TaskId, Vec<MonitorEvent>>,
    pub session_ready_at: HashMap<ChatId, Instant>,
    pub session_rehydrated_at: HashMap<ChatId, Instant>,
    pub session_generations: HashMap<ChatId, u64>,
    pub run_started: HashMap<ChatId, usize>,
    pub run_terminal_at: HashMap<ChatId, Instant>,
}

pub type Proof = Arc<Mutex<ProofState>>;
