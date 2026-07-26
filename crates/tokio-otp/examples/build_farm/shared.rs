//! State that must outlive an actor incarnation.
//!
//! Everything here is held by an [`ActorFactory`](tokio_otp::ActorFactory) —
//! either a derived `*Factory` struct or a registration closure's capture — so
//! it survives restarts by construction, while the actor state built for each
//! incarnation resets. The example leans on that boundary twice:
//!
//! * [`AttemptLog`] is what makes the retry loop terminate. The pool requeues
//!   a target whenever a dispatch is lost, and a poison target loses every
//!   dispatch; only a counter that survives the crash it is counting can stop
//!   the cycle.
//! * [`CasStore`] outlives the whole runtime, not just an incarnation. A build
//!   cache that reset with the process would make the warm-build phase
//!   meaningless.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{
    messages::{Artifact, BuildStatus, CasReport, Phase, PoolReport},
    plan::{Digest, TargetId},
};

/// Per-target attempt accounting shared by every worker incarnation.
#[derive(Debug)]
pub struct AttemptLog {
    max_attempts: u32,
    spent: Mutex<BTreeMap<TargetId, u32>>,
}

impl AttemptLog {
    /// Creates a log that allows `max_attempts` executions per target.
    pub fn new(max_attempts: u32) -> Arc<Self> {
        Arc::new(Self {
            max_attempts,
            spent: Mutex::new(BTreeMap::new()),
        })
    }

    /// Claims an attempt for `target`.
    ///
    /// Returns the 1-based attempt number, or `None` when the target has no
    /// attempts left. Claiming happens *before* the work runs, so an attempt
    /// that panics is still counted.
    pub fn begin(&self, target: TargetId) -> Option<u32> {
        let mut spent = self.spent.lock().expect("attempt log is not poisoned");
        let entry = spent.entry(target).or_insert(0);
        if *entry >= self.max_attempts {
            return None;
        }
        *entry += 1;
        Some(*entry)
    }

    /// Returns the attempts already spent on `target`.
    pub fn spent(&self, target: TargetId) -> u32 {
        self.spent
            .lock()
            .expect("attempt log is not poisoned")
            .get(target)
            .copied()
            .unwrap_or(0)
    }

    /// Returns the configured per-target attempt allowance.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns attempts spent per target.
    pub fn snapshot(&self) -> BTreeMap<TargetId, u32> {
        self.spent
            .lock()
            .expect("attempt log is not poisoned")
            .clone()
    }
}

/// The content-addressed artifact store's backing storage.
///
/// The `cas` actor owns all access to it; this type exists so the bytes
/// survive the runtime that served them, standing in for a shared cache on
/// disk or in object storage.
#[derive(Debug, Default)]
pub struct CasStore {
    inner: Mutex<CasInner>,
}

#[derive(Debug, Default)]
struct CasInner {
    entries: BTreeMap<Digest, Artifact>,
    hits: u64,
    misses: u64,
    writes: u64,
}

impl CasStore {
    /// Creates an empty store.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reads an artifact, counting the hit or miss.
    pub fn lookup(&self, digest: Digest) -> Option<Artifact> {
        let mut inner = self.inner.lock().expect("store is not poisoned");
        match inner.entries.get(&digest).copied() {
            Some(artifact) => {
                inner.hits += 1;
                Some(artifact)
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Writes an artifact at its own address.
    pub fn store(&self, artifact: Artifact) {
        let mut inner = self.inner.lock().expect("store is not poisoned");
        if inner.entries.insert(artifact.digest, artifact).is_none() {
            inner.writes += 1;
        }
    }

    /// Returns cumulative statistics.
    pub fn report(&self) -> CasReport {
        let inner = self.inner.lock().expect("store is not poisoned");
        CasReport {
            entries: inner.entries.len(),
            hits: inner.hits,
            misses: inner.misses,
            writes: inner.writes,
        }
    }
}

/// Where actors write their closing summary.
///
/// The runtime is torn down by its own completion watch, so by the time `main`
/// regains control the actors are gone and cannot be asked anything. Both
/// summaries are therefore pushed from `on_stop` rather than pulled afterwards.
#[derive(Debug, Default)]
pub struct BuildJournal {
    summary: Mutex<Option<BuildStatus>>,
    pool: Mutex<Option<PoolReport>>,
    display: Mutex<BTreeMap<TargetId, Phase>>,
    workers: Mutex<Vec<WorkerExit>>,
}

/// One worker incarnation's closing record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerExit {
    /// Worker label, which is also its dynamic child id.
    pub label: String,
    /// Actions this incarnation compiled.
    pub built: u64,
    /// Actions this incarnation served from the store.
    pub cached: u64,
}

impl BuildJournal {
    /// Creates an empty journal.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records the scheduler's final build state.
    pub fn record_summary(&self, status: BuildStatus) {
        *self.summary.lock().expect("journal is not poisoned") = Some(status);
    }

    /// Records the pool leader's final statistics.
    pub fn record_pool(&self, report: PoolReport) {
        *self.pool.lock().expect("journal is not poisoned") = Some(report);
    }

    /// Records the display's final per-target phase table.
    pub fn record_display(&self, phases: BTreeMap<TargetId, Phase>) {
        *self.display.lock().expect("journal is not poisoned") = phases;
    }

    /// Returns the recorded pool statistics.
    pub fn pool(&self) -> Option<PoolReport> {
        self.pool.lock().expect("journal is not poisoned").clone()
    }

    /// Returns the recorded per-target phase table.
    pub fn display(&self) -> BTreeMap<TargetId, Phase> {
        self.display
            .lock()
            .expect("journal is not poisoned")
            .clone()
    }

    /// Records one worker incarnation's closing counts.
    pub fn record_worker(&self, exit: WorkerExit) {
        self.workers
            .lock()
            .expect("journal is not poisoned")
            .push(exit);
    }

    /// Returns the recorded build state, if the scheduler stopped cleanly.
    pub fn summary(&self) -> Option<BuildStatus> {
        self.summary
            .lock()
            .expect("journal is not poisoned")
            .clone()
    }

    /// Returns every recorded worker exit.
    pub fn workers(&self) -> Vec<WorkerExit> {
        self.workers
            .lock()
            .expect("journal is not poisoned")
            .clone()
    }

    /// Returns the total actions compiled across all worker incarnations.
    pub fn total_built(&self) -> u64 {
        self.workers().iter().map(|worker| worker.built).sum()
    }
}
