use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use kokage::{Actor, Context, ExitResult, Reply};

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
        reply: Reply<DurableImage>,
    },
    Snapshot {
        reply: Reply<DurableImage>,
    },
}

#[derive(Debug)]
pub struct DurableShard {
    inner: Mutex<DurableInner>,
    starts: AtomicU64,
}

#[derive(Debug)]
struct DurableInner {
    image: DurableImage,
    prepared: BTreeMap<String, DurableImage>,
    crashed_handoffs: BTreeSet<String>,
}

impl DurableShard {
    pub fn new(image: DurableImage) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(DurableInner {
                image,
                prepared: BTreeMap::new(),
                crashed_handoffs: BTreeSet::new(),
            }),
            starts: AtomicU64::new(0),
        })
    }

    pub fn starts(&self) -> u64 {
        self.starts.load(Ordering::SeqCst)
    }

    fn record_start(&self) {
        self.starts.fetch_add(1, Ordering::SeqCst);
    }

    fn write(&self, shard_id: &str, epoch: u64, command: Write) -> Result<WriteReceipt, String> {
        let mut inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
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
        if !inner.image.range.contains(key) {
            return Err(format!("key {key} is outside {shard_id}"));
        }
        Ok(ReadReceipt {
            shard_id: shard_id.to_owned(),
            epoch,
            value: inner.image.values.get(&key).copied().unwrap_or_default(),
        })
    }

    fn prepare(&self, handoff_id: &str) -> (DurableImage, bool) {
        let mut inner = self
            .inner
            .lock()
            .expect("durable shard lock is not poisoned");
        let current = inner.image.clone();
        let image = inner
            .prepared
            .entry(handoff_id.to_owned())
            .or_insert(current)
            .clone();
        let should_crash = inner.crashed_handoffs.insert(handoff_id.to_owned());
        (image, should_crash)
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
    drained: bool,
}

impl Shard {
    pub fn new(id: String, epoch: u64, durable: Arc<DurableShard>) -> Self {
        Self {
            id,
            epoch,
            durable,
            drained: false,
        }
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
                let result = if self.drained {
                    Err(format!("{} is drained", self.id))
                } else {
                    self.durable.write(&self.id, self.epoch, command)
                };
                reply.send(result);
            }
            ShardMsg::Read { key, reply } => {
                let result = if self.drained {
                    Err(format!("{} is drained", self.id))
                } else {
                    self.durable.read(&self.id, self.epoch, key)
                };
                reply.send(result);
            }
            ShardMsg::PrepareHandoff {
                handoff_id,
                crash_once,
                reply,
            } => {
                let (image, first_attempt) = self.durable.prepare(&handoff_id);
                if crash_once && first_attempt {
                    panic!("scripted shard handoff crash: {handoff_id}");
                }
                self.drained = true;
                reply.send(image);
            }
            ShardMsg::Snapshot { reply } => reply.send(self.durable.snapshot()),
        }
        Ok(())
    }
}
