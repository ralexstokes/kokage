use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TelemetryEvent {
    pub id: u64,
    pub source: String,
    pub value: i64,
}

impl TelemetryEvent {
    pub fn scripted(id: u64) -> Self {
        Self {
            id,
            source: "loopback-client".to_owned(),
            value: i64::try_from(id).expect("scripted ids fit in i64"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichedEvent {
    pub event: TelemetryEvent,
    pub route: &'static str,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GatewayReport {
    pub connections_accepted: u64,
    pub clean_disconnects: u64,
    pub malformed_clients: u64,
    pub malformed_partial_headers: u64,
    pub malformed_truncated_bodies: u64,
    pub malformed_oversized_lengths: u64,
    pub malformed_json_frames: u64,
    pub valid_frames: u64,
    pub frames_accepted: u64,
    pub frames_shed_full: u64,
    pub degraded_connections: u64,
    pub sink_connect_attempts: u64,
    pub shipped_batches: u64,
    pub shipped_ids: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MalformedKind {
    PartialHeader,
    TruncatedBody,
    OversizedLength,
    InvalidJson,
}

impl MalformedKind {
    pub const ALL: [Self; 4] = [
        Self::PartialHeader,
        Self::TruncatedBody,
        Self::OversizedLength,
        Self::InvalidJson,
    ];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressOutcome {
    Accepted,
    ShedFull,
    Degraded,
}

#[derive(Clone, Default)]
pub struct Evidence {
    report: Arc<Mutex<GatewayReport>>,
    changed: Arc<Notify>,
}

impl Evidence {
    pub fn snapshot(&self) -> GatewayReport {
        self.report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub async fn wait_for(&self, predicate: impl Fn(&GatewayReport) -> bool) -> GatewayReport {
        loop {
            let changed = self.changed.notified();
            let report = self.snapshot();
            if predicate(&report) {
                return report;
            }
            changed.await;
        }
    }

    pub fn connection_accepted(&self) {
        self.update(|report| report.connections_accepted += 1);
    }

    pub fn clean_disconnect(&self) {
        self.update(|report| report.clean_disconnects += 1);
    }

    pub fn malformed_client(&self, kind: MalformedKind) {
        self.update(|report| {
            report.malformed_clients += 1;
            match kind {
                MalformedKind::PartialHeader => report.malformed_partial_headers += 1,
                MalformedKind::TruncatedBody => report.malformed_truncated_bodies += 1,
                MalformedKind::OversizedLength => report.malformed_oversized_lengths += 1,
                MalformedKind::InvalidJson => report.malformed_json_frames += 1,
            }
        });
    }

    pub fn valid_frame(&self, outcome: IngressOutcome) {
        self.update(|report| {
            report.valid_frames += 1;
            match outcome {
                IngressOutcome::Accepted => report.frames_accepted += 1,
                IngressOutcome::ShedFull => report.frames_shed_full += 1,
                IngressOutcome::Degraded => report.degraded_connections += 1,
            }
        });
    }

    pub fn sink_connect_attempt(&self) {
        self.update(|report| report.sink_connect_attempts += 1);
    }

    pub fn shipped(&self, ids: impl Iterator<Item = u64>) {
        self.update(|report| {
            report.shipped_batches += 1;
            report.shipped_ids.extend(ids);
        });
    }

    fn update(&self, update: impl FnOnce(&mut GatewayReport)) {
        update(&mut self.report.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_waiters();
    }
}

#[derive(Clone, Default)]
pub struct PipelineGate {
    state: Arc<Mutex<GateState>>,
    entered: Arc<Notify>,
    changed: Arc<Notify>,
}

#[derive(Default)]
struct GateState {
    entered: bool,
    open: bool,
}

impl PipelineGate {
    pub async fn block_first_batch(&self) {
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.entered = true;
                self.entered.notify_waiters();
                if state.open {
                    return;
                }
            }
            changed.await;
        }
    }

    pub async fn wait_entered(&self) {
        loop {
            let entered = self.entered.notified();
            if self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entered
            {
                return;
            }
            entered.await;
        }
    }

    pub fn open(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open = true;
        self.changed.notify_waiters();
    }
}
