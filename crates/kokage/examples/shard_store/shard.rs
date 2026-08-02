use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use kokage::{Actor, Context, ExitResult, Reply};
use tokio::{sync::Notify, time::Instant};

use crate::model::{DurableImage, ReadReceipt, Write, WriteReceipt};

#[derive(Debug)]
pub enum ShardMsg {
    Write {
        command: Write,
        reply: Reply<Result<WriteReceipt, String>>,
    },
    Read {
        key: u16,
        reply: Reply<Result<ReadReceipt, String>>,
    },
    PrepareHandoff {
        handoff_id: String,
        crash_once: bool,
        reply: Reply<Result<DurableImage, String>>,
    },
    Snapshot {
        reply: Reply<DurableImage>,
    },
}

#[derive(Debug)]
pub struct DurableShard {
    inner: Mutex<DurableInner>,
    starts: AtomicU64,
    starts_changed: Notify,
    handoff_changed: Notify,
}

#[derive(Debug)]
struct DurableInner {
    image: DurableImage,
    prepared: BTreeMap<String, DurableImage>,
    crashed_handoffs: BTreeSet<String>,
    active_handoff: Option<String>,
}

impl DurableShard {
    pub fn new(image: DurableImage) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(DurableInner {
                image,
                prepared: BTreeMap::new(),
                crashed_handoffs: BTreeSet::new(),
                active_handoff: None,
            }),
            starts: AtomicU64::new(0),
            starts_changed: Notify::new(),
            handoff_changed: Notify::new(),
        })
    }

    pub fn starts(&self) -> u64 {
        self.starts.load(Ordering::SeqCst)
    }

    fn record_start(&self) {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.starts_changed.notify_waiters();
    }

    pub async fn wait_for_start_after(
        &self,
        baseline: u64,
        deadline: Instant,
    ) -> Result<(), String> {
        loop {
            let changed = self.starts_changed.notified();
            if self.starts() > baseline {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| {
                    "source actor did not recover before the handoff deadline".to_owned()
                })?;
        }
    }

    fn write(&self, shard_id: &str, epoch: u64, command: Write) -> Result<WriteReceipt, String> {
        let mut inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        if let Some(handoff_id) = &inner.active_handoff {
            return Err(format!(
                "{shard_id} is fenced for active handoff {handoff_id}"
            ));
        }
        if !inner.image.range.contains(command.key) {
            return Err(format!("key {} is outside {shard_id}", command.key));
        }

        let applied = !inner.image.applied.contains_key(&command.effect);
        if applied {
            inner.image.applied.insert(command.effect, command.key);
        }
        let value = inner.image.values.entry(command.key).or_default();
        if applied {
            value.total += command.delta;
            value.version += 1;
        }
        Ok(WriteReceipt {
            shard_id: shard_id.to_owned(),
            epoch,
            applied,
            value: *value,
        })
    }

    fn read(&self, shard_id: &str, epoch: u64, key: u16) -> Result<ReadReceipt, String> {
        let inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        if let Some(handoff_id) = &inner.active_handoff {
            return Err(format!(
                "{shard_id} is fenced for active handoff {handoff_id}"
            ));
        }
        if !inner.image.range.contains(key) {
            return Err(format!("key {key} is outside {shard_id}"));
        }
        Ok(ReadReceipt {
            shard_id: shard_id.to_owned(),
            epoch,
            value: inner.image.values.get(&key).copied().unwrap_or_default(),
        })
    }

    fn prepare(&self, handoff_id: &str) -> Result<(DurableImage, bool), String> {
        let mut inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        if inner
            .active_handoff
            .as_deref()
            .is_some_and(|active| active != handoff_id)
        {
            return Err(format!(
                "another handoff is already active for {}",
                inner.image.range.start
            ));
        }
        inner.active_handoff = Some(handoff_id.to_owned());
        let current = inner.image.clone();
        let image = inner
            .prepared
            .entry(handoff_id.to_owned())
            .or_insert(current)
            .clone();
        let should_crash = inner.crashed_handoffs.insert(handoff_id.to_owned());
        drop(inner);
        self.handoff_changed.notify_waiters();
        Ok((image, should_crash))
    }

    pub fn prepared_image(&self, handoff_id: &str) -> Option<DurableImage> {
        let inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        (inner.active_handoff.as_deref() == Some(handoff_id))
            .then(|| inner.prepared.get(handoff_id).cloned())
            .flatten()
    }

    pub async fn wait_for_prepared(
        &self,
        handoff_id: &str,
        deadline: Instant,
    ) -> Result<DurableImage, String> {
        loop {
            let changed = self.handoff_changed.notified();
            if let Some(image) = self.prepared_image(handoff_id) {
                return Ok(image);
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| format!("handoff {handoff_id} remained unresolved at its deadline"))?;
        }
    }

    pub fn abort_handoff(&self, handoff_id: &str) -> Result<(), String> {
        let mut inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        match inner.active_handoff.as_deref() {
            Some(active) if active != handoff_id => {
                return Err(format!("cannot abort {handoff_id}; {active} is active"));
            }
            Some(_) => inner.active_handoff = None,
            None => {}
        }
        inner.prepared.remove(handoff_id);
        drop(inner);
        self.handoff_changed.notify_waiters();
        Ok(())
    }

    fn snapshot(&self) -> DurableImage {
        self.inner
            .lock()
            .expect("durable shard lock is not poisoned")
            .image
            .clone()
    }
}

pub struct Shard {
    id: String,
    epoch: u64,
    durable: Arc<DurableShard>,
}

impl Shard {
    pub fn new(id: String, epoch: u64, durable: Arc<DurableShard>) -> Self {
        Self { id, epoch, durable }
    }
}

impl Actor for Shard {
    type Msg = ShardMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.durable.record_start();
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ShardMsg::Write { command, reply } => {
                reply.send(self.durable.write(&self.id, self.epoch, command));
            }
            ShardMsg::Read { key, reply } => {
                reply.send(self.durable.read(&self.id, self.epoch, key));
            }
            ShardMsg::PrepareHandoff {
                handoff_id,
                crash_once,
                reply,
            } => {
                let prepared = self.durable.prepare(&handoff_id);
                if prepared
                    .as_ref()
                    .is_ok_and(|(_, first_attempt)| crash_once && *first_attempt)
                {
                    panic!("scripted shard handoff crash: {handoff_id}");
                }
                reply.send(prepared.map(|(image, _)| image));
            }
            ShardMsg::Snapshot { reply } => reply.send(self.durable.snapshot()),
        }
        Ok(())
    }
}
