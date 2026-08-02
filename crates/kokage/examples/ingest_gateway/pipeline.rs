use std::{io, sync::Arc};

use kokage::{Actor, ActorRef, Context, ExitResult, Reply};

use crate::model::{EnrichedEvent, Evidence, PipelineGate, TelemetryEvent};

pub const BATCH_SIZE: usize = 2;

pub struct Enricher {
    batcher: ActorRef<BatchMsg>,
}

impl Enricher {
    pub fn new(batcher: ActorRef<BatchMsg>) -> Self {
        Self { batcher }
    }
}

impl Actor for Enricher {
    type Msg = TelemetryEvent;

    async fn handle(&mut self, event: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.batcher
            .send(BatchMsg::Event(EnrichedEvent {
                event,
                route: "telemetry/default",
            }))
            .await
            .map_err(kokage::SendError::into_boxed)
    }
}

pub enum BatchMsg {
    Event(EnrichedEvent),
    Flush { reply: Reply<usize> },
}

pub struct Batcher {
    pending: Vec<EnrichedEvent>,
    shipper: ActorRef<ShipBatch>,
}

impl Batcher {
    pub fn new(shipper: ActorRef<ShipBatch>) -> Self {
        Self {
            pending: Vec::with_capacity(BATCH_SIZE),
            shipper,
        }
    }

    async fn ship_pending(&mut self) -> ExitResult {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch = ShipBatch(std::mem::take(&mut self.pending));
        self.shipper
            .send(batch)
            .await
            .map_err(kokage::SendError::into_boxed)
    }
}

impl Actor for Batcher {
    type Msg = BatchMsg;

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            BatchMsg::Event(event) => {
                self.pending.push(event);
                if self.pending.len() == BATCH_SIZE {
                    self.ship_pending().await?;
                }
            }
            BatchMsg::Flush { reply } => {
                let flushed = self.pending.len();
                self.ship_pending().await?;
                reply.send(flushed);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShipBatch(pub Vec<EnrichedEvent>);

pub struct ScriptedSink {
    attempt: u64,
    evidence: Evidence,
    gate: PipelineGate,
    first_batch: bool,
}

impl ScriptedSink {
    pub fn new(attempt: u64, evidence: Evidence, gate: PipelineGate) -> Self {
        Self {
            attempt,
            evidence,
            gate,
            first_batch: true,
        }
    }
}

impl Actor for ScriptedSink {
    type Msg = ShipBatch;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.evidence.sink_connect_attempt();
        if self.attempt <= 2 {
            return Err(io::Error::other("scripted sink connect failure").into());
        }
        Ok(())
    }

    async fn handle(&mut self, batch: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.first_batch {
            self.first_batch = false;
            self.gate.block_first_batch().await;
        }
        assert!(
            batch
                .0
                .iter()
                .all(|event| event.route == "telemetry/default")
        );
        self.evidence
            .shipped(batch.0.into_iter().map(|event| event.event.id));
        Ok(())
    }
}

pub fn ship_batch_size(batch: &ShipBatch) -> usize {
    batch.0.len()
}

pub fn event_size(event: &TelemetryEvent) -> usize {
    event.source.len() + size_of::<u64>() + size_of::<i64>()
}

pub fn shared_attempt_counter() -> Arc<std::sync::atomic::AtomicU64> {
    Arc::new(std::sync::atomic::AtomicU64::new(0))
}
