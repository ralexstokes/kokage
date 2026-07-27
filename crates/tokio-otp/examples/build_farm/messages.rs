//! Typed protocols shared by the build-farm actors.

use std::{collections::BTreeMap, time::Duration};

use tokio_otp::{ActorRef, Reply};

use crate::model::{Action, Digest, TargetId};

pub const CALL_DEADLINE: Duration = Duration::from_secs(3);
pub const CONTROL_DEADLINE: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Artifact {
    pub target: TargetId,
    pub digest: Digest,
    pub bytes: usize,
}

#[derive(Debug)]
pub enum CasMsg {
    Lookup {
        digest: Digest,
        reply: Reply<Option<Artifact>>,
    },
    Store(Artifact),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Queued,
    Running,
    Built,
    Cached,
}

#[derive(Debug)]
pub enum ProgressMsg {
    Update {
        target: TargetId,
        phase: Phase,
    },
    Snapshot {
        reply: Reply<BTreeMap<TargetId, Phase>>,
    },
}

#[derive(Debug)]
pub enum WorkerMsg {
    Execute {
        action: Action,
        digest: Digest,
        reply: Reply<ExecOutcome>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecOutcome {
    Built(Artifact),
    Cached(Artifact),
}

#[derive(Debug)]
pub enum PoolMsg {
    Submit {
        action: Action,
        digest: Digest,
    },
    Pump,
    WorkerAdded {
        label: String,
        actor: Option<ActorRef<WorkerMsg>>,
    },
    WorkerRemoved,
    DispatchFinished {
        label: String,
        action: Action,
        digest: Digest,
        outcome: DispatchOutcome,
    },
    Forwarded,
    Report {
        reply: Reply<PoolReport>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Finished(ExecOutcome),
    Lost,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolReport {
    pub live_workers: usize,
    pub peak_workers: usize,
    pub added_workers: u64,
    pub removed_workers: u64,
    pub dispatches: u64,
    pub lost_dispatches: u64,
}

#[derive(Debug)]
pub enum SchedulerMsg {
    Schedule,
    Finished {
        target: TargetId,
        outcome: ExecOutcome,
    },
    Snapshot {
        reply: Reply<BuildStatus>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetState {
    Blocked,
    Running,
    Built { digest: Digest, cached: bool },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildStatus {
    pub targets: BTreeMap<TargetId, TargetState>,
    pub submissions: u64,
    pub lease_stalls: u64,
    pub complete: bool,
}
