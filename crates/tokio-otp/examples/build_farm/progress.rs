//! The build display.
//!
//! Progress is idempotent latest-wins state — nobody needs to see that a
//! target passed 25% on its way to 75% — so this actor runs a keyed conflating
//! mailbox and lets a redraw storm collapse into one update per target.
//!
//! The keying has a sharp edge worth naming. A conflating mailbox conflates
//! *whatever the key function says is the same message*, including replies, so
//! request/reply traffic sharing this mailbox has to be carved out by hand: see
//! [`progress_key`]. There is no way to mark an individual message
//! "never conflate" — the key is a pure function of the message — so the
//! control keys are only safe because a single caller has at most one of each
//! request outstanding at a time.

use std::{collections::BTreeMap, sync::Arc};

use tokio_otp::{Actor, ActorResult, BoxError, MessageContext, StopContext, prelude::Continue};

use crate::{
    messages::{Phase, ProgressMsg, ProgressStats, TargetProgress},
    plan::TargetId,
    shared::BuildJournal,
};

/// Mailbox conflation key for [`ProgressMsg`].
///
/// Every target conflates against itself and nothing else; the two control
/// variants get keys of their own so a progress update can never displace a
/// pending reply.
#[derive(Eq, PartialEq)]
pub enum ProgressKey {
    /// One target's latest phase.
    Target(TargetId),
    /// A pending render request.
    Render,
    /// A pending stats request.
    Stats,
}

/// Extracts the conflation key of a progress message.
pub fn progress_key(message: &ProgressMsg) -> ProgressKey {
    match message {
        ProgressMsg::Update(update) => ProgressKey::Target(update.target),
        ProgressMsg::Render { .. } => ProgressKey::Render,
        ProgressMsg::Stats { .. } => ProgressKey::Stats,
    }
}

/// Renders the latest phase of every target.
pub struct Progress {
    journal: Arc<BuildJournal>,
    phases: BTreeMap<TargetId, Phase>,
    stats: ProgressStats,
}

impl Progress {
    /// Creates an empty display that writes its final table to `journal`.
    pub fn new(journal: Arc<BuildJournal>) -> Self {
        Self {
            journal,
            phases: BTreeMap::new(),
            stats: ProgressStats::default(),
        }
    }
}

impl Actor for Progress {
    type Msg = ProgressMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            ProgressMsg::Update(TargetProgress { target, phase }) => {
                self.stats.applied += 1;
                if self.phases.insert(target, phase) != Some(phase) {
                    self.stats.transitions += 1;
                }
            }
            ProgressMsg::Render { reply } => reply.send(self.phases.clone()),
            ProgressMsg::Stats { reply } => reply.send(self.stats),
        }
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_display(self.phases.clone());
        Ok(())
    }
}
