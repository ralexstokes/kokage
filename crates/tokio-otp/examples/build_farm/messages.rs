//! The shared vocabulary: one message enum per actor plus the report types the
//! acceptance script asserts against.

use std::{collections::BTreeMap, time::Duration};

use tokio_otp::{ActorRef, MessageSize, Reply};

use crate::plan::{Action, Digest, TargetId};

/// Bound on every request/reply the acceptance script makes.
pub const CALL_DEADLINE: Duration = Duration::from_secs(2);

/// Bound the pool puts on one worker dispatch.
///
/// A worker that misses it is presumed wedged: the pool retires it and
/// requeues the target rather than waiting for a reply that may never come.
pub const DISPATCH_DEADLINE: Duration = Duration::from_millis(400);

/// Bound on the pool's own control operations against its worker scope.
pub const CONTROL_DEADLINE: Duration = Duration::from_secs(2);

/// A stored build output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Artifact {
    /// Target that produced it.
    pub target: TargetId,
    /// Content address of the producing action.
    pub digest: Digest,
    /// Output size.
    pub bytes: usize,
}

/// Content-addressed artifact store.
#[derive(Debug)]
pub enum CasMsg {
    /// Reads an artifact by content address.
    Lookup {
        /// Address to read.
        digest: Digest,
        /// Answer channel.
        reply: Reply<Option<Artifact>>,
    },
    /// Writes an artifact, keyed by its own address.
    Store {
        /// Artifact to write.
        artifact: Artifact,
    },
    /// Reads cumulative store statistics.
    Report {
        /// Answer channel.
        reply: Reply<CasSnapshot>,
    },
}

/// What the store actor knows, split by how long it lives.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CasSnapshot {
    /// Statistics of the durable backing store.
    pub store: CasReport,
    /// Lookups answered by the *current* incarnation, which resets on restart.
    pub served_by_incarnation: u64,
}

impl MessageSize for CasMsg {
    fn size_hint(&self) -> usize {
        match self {
            Self::Store { artifact } => artifact.bytes,
            Self::Lookup { .. } | Self::Report { .. } => 0,
        }
    }
}

/// Cumulative store statistics, durable across process runs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CasReport {
    /// Distinct addresses held.
    pub entries: usize,
    /// Lookups that found an artifact.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Writes that added a new address.
    pub writes: u64,
}

/// Build-progress display.
#[derive(Debug)]
pub enum ProgressMsg {
    /// Reports the latest known phase of one target.
    ///
    /// This is idempotent latest-wins state, which is why the progress actor
    /// runs a keyed conflating mailbox.
    Update(TargetProgress),
    /// Reads the current per-target phase table.
    Render {
        /// Answer channel.
        reply: Reply<BTreeMap<TargetId, Phase>>,
    },
    /// Reads counts of what the display absorbed.
    Stats {
        /// Answer channel.
        reply: Reply<ProgressStats>,
    },
}

/// One progress observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetProgress {
    /// Target being reported on.
    pub target: TargetId,
    /// Its latest phase.
    pub phase: Phase,
}

/// Where a target is in the build.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Phase {
    /// Waiting for a worker.
    Queued,
    /// Executing, with percent complete.
    Running(u8),
    /// Compiled by a worker.
    Built,
    /// Served from the content-addressed store.
    Cached,
    /// Out of attempts.
    Failed,
    /// A dependency failed.
    Skipped,
}

/// What the progress display absorbed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProgressStats {
    /// Updates the actor handled.
    pub applied: u64,
    /// Updates whose phase differed from the previous one for that target.
    pub transitions: u64,
}

/// Worker-pool leader.
#[derive(Debug)]
pub enum PoolMsg {
    /// Queues an action for execution.
    Submit {
        /// Action to run.
        action: Action,
        /// Its resolved content address.
        digest: Digest,
    },
    /// Re-examines the queue, the worker roster, and the desired pool size.
    Pump,
    /// Reports the total outcome of one pipelined dispatch.
    Dispatched {
        /// Worker the action was sent to.
        worker: String,
        /// Action that was dispatched.
        action: Action,
        /// Its resolved content address.
        digest: Digest,
        /// How the dispatch ended.
        outcome: DispatchOutcome,
    },
    /// Reports the result of a pipelined scale-up.
    WorkerAdded {
        /// Requested worker label.
        label: String,
        /// The new worker's ref, or `None` if the insert failed.
        actor: Option<ActorRef<WorkerMsg>>,
    },
    /// Reports the result of a pipelined scale-down or retirement.
    WorkerRemoved {
        /// Worker label that was removed.
        label: String,
    },
    /// Reports a lifecycle transition of a worker this leader watches.
    WorkerLifecycle {
        /// Worker label.
        label: String,
        /// Transition observed.
        event: WorkerLifecycle,
    },
    /// Acknowledges a pipelined effect that produces no state change.
    ///
    /// `offload` always posts a message back, so a fire-and-forget effect
    /// still needs a variant to land in. See the pool module for why these
    /// sends are pipelined rather than awaited inline.
    Noted,
    /// Reads pool statistics.
    Report {
        /// Answer channel.
        reply: Reply<PoolReport>,
    },
}

/// The total outcome of one bounded dispatch to a worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    /// The worker answered.
    Finished(ExecOutcome),
    /// The worker died with the request in flight, so its reply was dropped.
    Lost,
    /// The dispatch deadline expired with no answer.
    Stalled,
}

/// A worker lifecycle transition, as seen by the pool leader's watch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerLifecycle {
    /// A worker incarnation started.
    Up,
    /// A worker incarnation exited; a restart may follow.
    Down,
    /// The worker is permanently gone.
    Terminated,
}

/// Pool statistics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolReport {
    /// Workers currently in the roster.
    pub workers: usize,
    /// Highest roster size reached.
    pub peak_workers: usize,
    /// Actions still queued.
    pub queued: usize,
    /// Dispatches started.
    pub dispatched: u64,
    /// Dispatches that ended in [`DispatchOutcome::Lost`].
    pub lost: u64,
    /// Dispatches that ended in [`DispatchOutcome::Stalled`].
    pub stalled: u64,
    /// Workers retired because they stalled.
    pub retired: u64,
    /// Worker restarts observed through the leader's watch.
    pub worker_restarts: u64,
}

/// One build executor.
#[derive(Debug)]
pub enum WorkerMsg {
    /// Executes an action, consulting and populating the store.
    Execute {
        /// Action to run.
        action: Action,
        /// Its resolved content address.
        digest: Digest,
        /// Answer channel.
        reply: Reply<ExecOutcome>,
    },
}

/// How one execution ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecOutcome {
    /// Compiled by this worker.
    Built(Artifact),
    /// Served from the store without compiling.
    Cached(Artifact),
    /// Refused: the shared attempt log says the target is out of attempts.
    Quarantined {
        /// Attempts already spent.
        attempts: u32,
    },
}

/// Build scheduler.
#[derive(Debug)]
pub enum SchedulerMsg {
    /// Re-examines the frontier and submits everything that is ready.
    Dispatch,
    /// Reports one target's terminal outcome.
    Finished {
        /// Target that finished.
        target: TargetId,
        /// What happened to it.
        outcome: ExecOutcome,
    },
    /// Reads the current build state.
    Status {
        /// Answer channel.
        reply: Reply<BuildStatus>,
    },
}

/// One target's state in the scheduler's frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetState {
    /// At least one dependency is unresolved.
    Blocked,
    /// Submitted to the pool.
    Running,
    /// Finished successfully.
    Built {
        /// Its content address.
        digest: Digest,
        /// Whether the store answered instead of a worker.
        cached: bool,
    },
    /// Out of attempts.
    Failed {
        /// Attempts spent before giving up.
        attempts: u32,
    },
    /// Not attempted, because a dependency failed or was skipped.
    Skipped,
}

impl TargetState {
    /// Returns whether this state can still change.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Built { .. } | Self::Failed { .. } | Self::Skipped
        )
    }
}

/// The scheduler's view of the build.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildStatus {
    /// Per-target state.
    pub states: BTreeMap<TargetId, TargetState>,
    /// Actions submitted to the pool, including resubmissions.
    pub submitted: u64,
    /// Whether every target has reached a terminal state.
    pub finished: bool,
    /// Dispatch passes that found the lease stale and backed off.
    pub lease_stalls: u64,
}

impl BuildStatus {
    /// Returns the targets in `state`.
    pub fn targets_in(&self, state: &TargetState) -> Vec<TargetId> {
        self.states
            .iter()
            .filter(|(_, current)| *current == state)
            .map(|(target, _)| *target)
            .collect()
    }
}
