use std::collections::BTreeMap;

pub type EffectId = u64;
pub type Key = u16;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct KeyRange {
    pub start: Key,
    pub end: Key,
}

impl KeyRange {
    pub const fn new(start: Key, end: Key) -> Self {
        Self { start, end }
    }

    pub fn contains(self, key: Key) -> bool {
        self.start <= key && key < self.end
    }

    pub fn split(self, at: Key) -> (Self, Self) {
        assert!(self.start < at && at < self.end);
        (Self::new(self.start, at), Self::new(at, self.end))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardConfig {
    pub revision: u64,
    pub flush_batch: usize,
}

impl ShardConfig {
    pub const fn initial() -> Self {
        Self {
            revision: 1,
            flush_batch: 8,
        }
    }

    pub const fn reloaded() -> Self {
        Self {
            revision: 2,
            flush_batch: 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Value {
    pub total: i64,
    pub version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableImage {
    pub range: KeyRange,
    pub config: ShardConfig,
    pub values: BTreeMap<Key, Value>,
    pub applied: BTreeMap<EffectId, Key>,
}

impl DurableImage {
    pub fn empty(range: KeyRange, config: ShardConfig) -> Self {
        Self {
            range,
            config,
            values: BTreeMap::new(),
            applied: BTreeMap::new(),
        }
    }

    pub fn partition(self, left: KeyRange, right: KeyRange) -> (Self, Self) {
        let mut left_image = Self::empty(left, self.config);
        let mut right_image = Self::empty(right, self.config);

        for (key, value) in self.values {
            if left.contains(key) {
                left_image.values.insert(key, value);
            } else {
                assert!(right.contains(key));
                right_image.values.insert(key, value);
            }
        }

        for (effect, key) in self.applied {
            if left.contains(key) {
                left_image.applied.insert(effect, key);
            } else {
                assert!(right.contains(key));
                right_image.applied.insert(effect, key);
            }
        }
        (left_image, right_image)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Write {
    pub effect: EffectId,
    pub key: Key,
    pub delta: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReceipt {
    pub shard_id: String,
    pub epoch: u64,
    pub applied: bool,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadReceipt {
    pub shard_id: String,
    pub epoch: u64,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteView {
    pub shard_id: String,
    pub epoch: u64,
    pub range: KeyRange,
    pub config: ShardConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectorySnapshot {
    pub revision: u64,
    pub planned_rebinds: u64,
    pub routes: Vec<RouteView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestartEvidence {
    pub shard_id: String,
    pub generation: u64,
    pub child_restarts: u64,
    pub scope_restarts: u64,
    pub actor_starts: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannedChange {
    Split,
    ConfigReload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionReport {
    pub change: PlannedChange,
    pub sources: Vec<String>,
    pub successors: Vec<String>,
    pub moved_keys: usize,
    pub durable_effects: usize,
    pub buffered_requests: usize,
    pub recovered_crash: bool,
    pub source_restart: RestartEvidence,
}
