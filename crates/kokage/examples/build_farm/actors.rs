//! The durable cache and latest-wins progress actors.

use std::sync::Arc;

use kokage::prelude::*;

use crate::{
    messages::{CasMsg, ProgressMsg},
    shared::{CasStore, ProgressBook},
};

pub struct Cas {
    store: Arc<CasStore>,
}

impl Cas {
    pub fn new(store: Arc<CasStore>) -> Self {
        Self { store }
    }
}

impl Actor for Cas {
    type Msg = CasMsg;

    async fn handle(&mut self, message: CasMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CasMsg::Lookup { digest, reply } => reply.send(self.store.lookup(digest)),
            CasMsg::Store { artifact, reply } => {
                self.store.store(artifact);
                reply.send(());
            }
            CasMsg::Report { reply } => reply.send(self.store.report()),
        }
        Ok(())
    }
}

pub struct Progress {
    book: Arc<ProgressBook>,
}

impl Progress {
    pub fn new(book: Arc<ProgressBook>) -> Self {
        Self { book }
    }
}

impl Actor for Progress {
    type Msg = ProgressMsg;

    async fn handle(&mut self, message: ProgressMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        // Rendering or exporting progress is commonly slower than producers.
        // Yielding models that boundary and makes replacement visible in the
        // example's actor statistics without introducing wall-clock sleeps.
        tokio::task::yield_now().await;
        self.book.record(message.target, message.phase);
        Ok(())
    }
}
