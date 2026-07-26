//! The content-addressed artifact store.
//!
//! The bytes live in an [`Arc<CasStore>`] held by the generated `CasFactory`,
//! so they survive both actor restarts and the runtime itself — which is what
//! makes the warm build in phase 5 a pure cache hit. The actor exists to
//! serialize access and to give the store a place in the supervision tree, not
//! to own the data.

use std::sync::Arc;

use tokio_otp::{Actor, ActorFactory, ActorResult, MessageContext, prelude::Continue};

use crate::{
    messages::{CasMsg, CasSnapshot},
    shared::CasStore,
};

/// Serializes access to the shared artifact store.
#[derive(ActorFactory)]
pub struct Cas {
    /// Durable backing storage, cloned into every incarnation.
    store: Arc<CasStore>,
    /// Lookups this incarnation answered; resets on restart.
    #[factory(default)]
    served: u64,
}

impl Actor for Cas {
    type Msg = CasMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            CasMsg::Lookup { digest, reply } => {
                self.served += 1;
                reply.send(self.store.lookup(digest));
            }
            CasMsg::Store { artifact } => self.store.store(artifact),
            CasMsg::Report { reply } => reply.send(CasSnapshot {
                store: self.store.report(),
                served_by_incarnation: self.served,
            }),
        }
        Ok(Continue)
    }
}
