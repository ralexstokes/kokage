//! Typed protocols shared by the build-farm actors and tasks.

use kokage::Reply;

use crate::model::{Digest, TargetId};

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
    Store {
        artifact: Artifact,
        reply: Reply<()>,
    },
    Report {
        reply: Reply<StoreReport>,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreReport {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Queued,
    Running,
    Retrying,
    Built,
    Cached,
}

#[derive(Debug)]
pub struct ProgressMsg {
    pub target: TargetId,
    pub phase: Phase,
}
