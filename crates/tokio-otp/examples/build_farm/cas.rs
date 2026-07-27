//! Content-addressed artifact storage actor.

use std::sync::Arc;

use tokio_otp::{Actor, ActorFactory, ActorResult, MessageContext, prelude::Continue};

use crate::{messages::CasMsg, shared::CasStore};

#[derive(ActorFactory)]
pub struct Cas {
    store: Arc<CasStore>,
}

impl Actor for Cas {
    type Msg = CasMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            CasMsg::Lookup { digest, reply } => reply.send(self.store.lookup(digest)),
            CasMsg::Store(artifact) => self.store.store(artifact),
        }
        Ok(Continue)
    }
}
