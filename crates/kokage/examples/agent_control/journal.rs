//! Append-only transcript and effect journal, including envelope deduplication.
//!
//! This example deliberately serializes appends and every session replay
//! through one mailbox. For production alternatives, see the book's
//! [rehydration scaling patterns].
//!
//! [rehydration scaling patterns]: https://stokes.io/kokage/ownership-transitions.html#scale-rehydration-without-blocking-appends

use std::collections::HashSet;

use kokage::{Actor, ActorResult, DrainPolicy, MessageContext};

use crate::messages::{AppendAck, JournalEntry, JournalMsg, JournalReport, StoredEntry};

#[derive(Default)]
pub struct Journal {
    entries: Vec<StoredEntry>,
    envelopes: HashSet<u64>,
    duplicates: usize,
}

impl Actor for Journal {
    type Msg = JournalMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            JournalMsg::Append { chat, entry, reply } => {
                let duplicate = matches!(
                    &entry,
                    JournalEntry::UserMessage { envelope, .. } if !self.envelopes.insert(*envelope)
                );
                if duplicate {
                    self.duplicates += 1;
                } else {
                    let seq = self.entries.len() as u64 + 1;
                    self.entries.push(StoredEntry { seq, chat, entry });
                }
                reply.send(AppendAck {
                    seq: self.entries.len() as u64,
                    duplicate,
                });
            }
            JournalMsg::Replay { chat, reply } => {
                // A small deterministic replay delay proves that readiness is
                // reported before `continue_with(Rehydrate)` finishes.
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                reply.send(
                    self.entries
                        .iter()
                        .filter(|entry| entry.chat == chat)
                        .cloned()
                        .collect(),
                );
            }
            JournalMsg::Report { reply } => reply.send(JournalReport {
                entries: self.entries.clone(),
                duplicate_appends: self.duplicates,
            }),
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}
