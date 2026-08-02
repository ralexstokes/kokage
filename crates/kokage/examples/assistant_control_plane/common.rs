use std::{fmt, sync::Arc};

use tokio::sync::mpsc;

pub const CALL_BOUND: std::time::Duration = std::time::Duration::from_millis(200);
pub const MODEL_BOUND: std::time::Duration = std::time::Duration::from_millis(45);
pub const TOOL_BOUND: std::time::Duration = std::time::Duration::from_millis(35);
pub const WAIT_BOUND: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub id: u64,
    pub chat: String,
    pub text: String,
}

impl Envelope {
    pub fn new(id: u64, chat: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id,
            chat: chat.into(),
            text: text.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Stage {
    Planner,
    Engineer,
    Reviewer,
}

impl Stage {
    pub fn tokens(self) -> u64 {
        match self {
            Self::Planner => 11,
            Self::Engineer => 13,
            Self::Reviewer => 7,
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner => f.write_str("planner"),
            Self::Engineer => f.write_str("engineer"),
            Self::Reviewer => f.write_str("reviewer"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalEntry {
    Incoming {
        envelope_id: u64,
        chat: String,
        text: String,
    },
    ModelTurn {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        stage: Stage,
        tokens: u64,
    },
    ToolIntent {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        key: String,
    },
    ToolResult {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        key: String,
        reconciled: bool,
    },
    Assistant {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        text: String,
    },
    Checkpoint {
        chat: String,
        messages: usize,
    },
    Evicted {
        chat: String,
        epoch: u64,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum Evidence {
    ActorStarted {
        actor: &'static str,
        generation: u64,
    },
    BridgeJournaled {
        envelope_id: u64,
        duplicate: bool,
    },
    BridgeAcked(u64),
    Mounted {
        chat: String,
        epoch: u64,
        subtree_id: String,
    },
    Removing {
        chat: String,
        epoch: u64,
        subtree_id: String,
    },
    Removed {
        chat: String,
        epoch: u64,
        subtree_id: String,
    },
    OrphanSwept(String),
    Rehydrated {
        chat: String,
        epoch: u64,
        messages: usize,
    },
    RunStarted {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        run_id: String,
    },
    RunFailed {
        chat: String,
        envelope_id: u64,
        attempt: u32,
        reason: String,
    },
    RunCompleted {
        chat: String,
        envelope_id: u64,
        attempt: u32,
    },
    ToolReconciled {
        key: String,
    },
    Progress {
        envelope_id: u64,
        sequence: u64,
    },
    GateChanged {
        open: bool,
        reason: String,
    },
    HeldWhilePaused {
        chat: String,
        envelope_id: u64,
    },
    EvictionRequested {
        chat: String,
        epoch: u64,
    },
    ActorStopped(&'static str),
}

#[derive(Clone)]
pub struct EvidenceTx(Arc<mpsc::UnboundedSender<Evidence>>);

impl EvidenceTx {
    pub fn new(sender: mpsc::UnboundedSender<Evidence>) -> Self {
        Self(Arc::new(sender))
    }

    pub fn emit(&self, evidence: Evidence) {
        let _ = self.0.send(evidence);
    }
}
