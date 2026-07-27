use tokio::time::Instant;
use tokio_otp::prelude::*;

use crate::{
    messages::{LedgerMsg, LedgerReport},
    telemetry::LatencyRecorder,
};

#[derive(Clone, Default)]
pub struct Ledger {
    report: LedgerReport,
    latency: LatencyRecorder,
}

impl Ledger {
    pub fn new(latency: LatencyRecorder) -> Self {
        Self {
            report: LedgerReport::default(),
            latency,
        }
    }
}

impl Actor for Ledger {
    type Msg = LedgerMsg;

    async fn handle(
        &mut self,
        message: LedgerMsg,
        _ctx: &mut MessageContext<'_, LedgerMsg>,
    ) -> ActorResult {
        match message {
            LedgerMsg::Ack { key, venue } => {
                tracing::debug!(venue, order_key = key, "order acknowledged");
                self.report.effects.entry(key).or_default().acknowledgements += 1;
            }
            LedgerMsg::Fill {
                key,
                venue,
                qty,
                enqueued_at,
            } => {
                tracing::debug!(venue, qty, order_key = key, "order filled");
                self.latency.record(
                    "queue.fill",
                    Instant::now().saturating_duration_since(enqueued_at),
                );
                self.report.effects.entry(key).or_default().fills += 1;
            }
            LedgerMsg::Cancelled { key, venue } => {
                tracing::debug!(venue, order_key = key, "order cancelled");
                self.report.effects.entry(key).or_default().cancellations += 1;
            }
            LedgerMsg::Report { reply } => reply.send(self.report.clone()),
        }
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}
