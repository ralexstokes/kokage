//! State whose lifetime is deliberately longer than one actor incarnation.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{
    messages::{Artifact, BuildStatus, Phase, PoolReport},
    model::{Digest, TargetId},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreReport {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
}

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
        let mut inner = self.inner.lock().expect("CAS mutex is not poisoned");
        let artifact = inner.artifacts.get(&digest).copied();
        if artifact.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        artifact
    }

    pub fn store(&self, artifact: Artifact) {
        let mut inner = self.inner.lock().expect("CAS mutex is not poisoned");
        if inner.artifacts.insert(artifact.digest, artifact).is_none() {
            inner.writes += 1;
        }
    }

    pub fn report(&self) -> StoreReport {
        let inner = self.inner.lock().expect("CAS mutex is not poisoned");
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
            .expect("attempt-book mutex is not poisoned");
        let attempt = attempts.entry(target).or_default();
        *attempt += 1;
        *attempt
    }

    pub fn snapshot(&self) -> BTreeMap<TargetId, u32> {
        self.attempts
            .lock()
            .expect("attempt-book mutex is not poisoned")
            .clone()
    }
}

#[derive(Debug, Default)]
pub struct BuildJournal {
    status: Mutex<Option<BuildStatus>>,
    pool: Mutex<Option<PoolReport>>,
    progress: Mutex<BTreeMap<TargetId, Phase>>,
}

impl BuildJournal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_status(&self, status: BuildStatus) {
        *self.status.lock().expect("journal mutex is not poisoned") = Some(status);
    }

    pub fn status(&self) -> Option<BuildStatus> {
        self.status
            .lock()
            .expect("journal mutex is not poisoned")
            .clone()
    }

    pub fn record_pool(&self, pool: PoolReport) {
        *self.pool.lock().expect("journal mutex is not poisoned") = Some(pool);
    }

    pub fn pool(&self) -> Option<PoolReport> {
        *self.pool.lock().expect("journal mutex is not poisoned")
    }

    pub fn record_progress(&self, progress: BTreeMap<TargetId, Phase>) {
        *self.progress.lock().expect("journal mutex is not poisoned") = progress;
    }

    pub fn progress(&self) -> BTreeMap<TargetId, Phase> {
        self.progress
            .lock()
            .expect("journal mutex is not poisoned")
            .clone()
    }
}
