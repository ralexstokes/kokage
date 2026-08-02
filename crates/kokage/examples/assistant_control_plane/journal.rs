use std::{collections::HashSet, sync::Arc};

use kokage::{Actor, Context, ExitResult, Reply, StopContext};
use tokio::sync::Mutex;

use crate::common::{Evidence, EvidenceTx, JournalEntry};

#[derive(Default)]
pub struct JournalStore {
    entries: Vec<JournalEntry>,
    envelopes: HashSet<u64>,
    duplicate_envelopes: u64,
}

impl JournalStore {
    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    pub fn duplicate_envelopes(&self) -> u64 {
        self.duplicate_envelopes
    }
}

pub type SharedJournal = Arc<Mutex<JournalStore>>;

pub enum JournalMsg {
    AppendIncoming {
        envelope_id: u64,
        chat: String,
        text: String,
        reply: Reply<bool>,
    },
    Append(JournalEntry, Reply<()>),
    Replay {
        chat: String,
        reply: Reply<Vec<JournalEntry>>,
    },
}

pub struct Journal {
    store: SharedJournal,
    evidence: EvidenceTx,
}

impl Journal {
    pub fn new(store: SharedJournal, evidence: EvidenceTx) -> Self {
        Self { store, evidence }
    }
}

impl Actor for Journal {
    type Msg = JournalMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            JournalMsg::AppendIncoming {
                envelope_id,
                chat,
                text,
                reply,
            } => {
                let mut store = self.store.lock().await;
                let inserted = store.envelopes.insert(envelope_id);
                if inserted {
                    store.entries.push(JournalEntry::Incoming {
                        envelope_id,
                        chat,
                        text,
                    });
                } else {
                    store.duplicate_envelopes += 1;
                }
                reply.send(inserted);
            }
            JournalMsg::Append(entry, reply) => {
                self.store.lock().await.entries.push(entry);
                reply.send(());
            }
            JournalMsg::Replay { chat, reply } => {
                let entries = self
                    .store
                    .lock()
                    .await
                    .entries
                    .iter()
                    .filter(|entry| belongs_to(entry, &chat))
                    .cloned()
                    .collect();
                reply.send(entries);
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), kokage::BoxError> {
        self.evidence.emit(Evidence::ActorStopped("journal"));
        Ok(())
    }
}

fn belongs_to(entry: &JournalEntry, chat: &str) -> bool {
    match entry {
        JournalEntry::Incoming { chat: owner, .. }
        | JournalEntry::Checkpoint { chat: owner, .. }
        | JournalEntry::Evicted { chat: owner, .. }
        | JournalEntry::ModelTurn { chat: owner, .. }
        | JournalEntry::ToolIntent { chat: owner, .. }
        | JournalEntry::ToolResult { chat: owner, .. }
        | JournalEntry::Assistant { chat: owner, .. } => owner == chat,
    }
}
