//! Latest-wins build progress.

use std::{collections::BTreeMap, sync::Arc};

use tokio_otp::{Actor, ActorResult, BoxError, MessageContext, StopContext, prelude::Continue};

use crate::{
    messages::{Phase, ProgressMsg},
    model::TargetId,
    shared::BuildJournal,
};

#[derive(Eq, PartialEq)]
pub enum ProgressKey {
    Target(TargetId),
    Snapshot,
}

pub fn progress_key(message: &ProgressMsg) -> ProgressKey {
    match message {
        ProgressMsg::Update { target, .. } => ProgressKey::Target(target),
        ProgressMsg::Snapshot { .. } => ProgressKey::Snapshot,
    }
}

pub struct Progress {
    journal: Arc<BuildJournal>,
    phases: BTreeMap<TargetId, Phase>,
}

impl Progress {
    pub fn new(journal: Arc<BuildJournal>) -> Self {
        Self {
            journal,
            phases: BTreeMap::new(),
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
            ProgressMsg::Update { target, phase } => {
                self.phases.insert(target, phase);
            }
            ProgressMsg::Snapshot { reply } => reply.send(self.phases.clone()),
        }
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_progress(self.phases.clone());
        Ok(())
    }
}
