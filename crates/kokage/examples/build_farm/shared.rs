//! State whose lifetime deliberately differs from one supervised incarnation.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{
    messages::{Artifact, Phase, StoreReport},
    model::{Digest, TargetId},
};

#[derive(Debug, Default)]
pub struct CasStore {
    inner: Mutex<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner {
    artifacts: BTreeMap<Digest, Artifact>,
    hits: u64,
    misses: u64,
    writes: u64,
}

impl CasStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn lookup(&self, digest: Digest) -> Option<Artifact> {
        let mut inner = self.inner.lock().expect("CAS lock is not poisoned");
        let artifact = inner.artifacts.get(&digest).copied();
        if artifact.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        artifact
    }

    pub fn store(&self, artifact: Artifact) {
        let mut inner = self.inner.lock().expect("CAS lock is not poisoned");
        if inner.artifacts.insert(artifact.digest, artifact).is_none() {
            inner.writes += 1;
        }
    }

    pub fn report(&self) -> StoreReport {
        let inner = self.inner.lock().expect("CAS lock is not poisoned");
        StoreReport {
            entries: inner.artifacts.len(),
            hits: inner.hits,
            misses: inner.misses,
            writes: inner.writes,
        }
    }
}

#[derive(Debug, Default)]
pub struct AttemptBook {
    attempts: Mutex<BTreeMap<TargetId, u32>>,
}

impl AttemptBook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn begin(&self, target: TargetId) -> u32 {
        let mut attempts = self
            .attempts
            .lock()
            .expect("attempt-book lock is not poisoned");
        let attempt = attempts.entry(target).or_default();
        *attempt += 1;
        *attempt
    }

    pub fn snapshot(&self) -> BTreeMap<TargetId, u32> {
        self.attempts
            .lock()
            .expect("attempt-book lock is not poisoned")
            .clone()
    }
}

#[derive(Debug, Default)]
pub struct ProgressBook {
    phases: Mutex<BTreeMap<TargetId, Phase>>,
}

impl ProgressBook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record(&self, target: TargetId, phase: Phase) {
        self.phases
            .lock()
            .expect("progress lock is not poisoned")
            .insert(target, phase);
    }

    pub fn snapshot(&self) -> BTreeMap<TargetId, Phase> {
        self.phases
            .lock()
            .expect("progress lock is not poisoned")
            .clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetState {
    Blocked,
    Built { digest: Digest, cached: bool },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildReport {
    pub targets: BTreeMap<TargetId, TargetState>,
    pub submissions: u64,
    pub cache_hits: u64,
    pub failed_attempts: u64,
    pub retired_workers: u64,
    pub peak_workers: usize,
    pub lease_waits: u64,
    pub complete: bool,
}

#[derive(Debug, Default)]
pub struct BuildJournal {
    report: Mutex<Option<BuildReport>>,
}

impl BuildJournal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record(&self, report: BuildReport) {
        *self.report.lock().expect("journal lock is not poisoned") = Some(report);
    }

    pub fn report(&self) -> Option<BuildReport> {
        self.report
            .lock()
            .expect("journal lock is not poisoned")
            .clone()
    }
}
