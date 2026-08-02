use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use kokage::{CancellationToken, ScopeRef};
use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

#[derive(Clone, Debug, Default)]
pub struct LatencyRecorder(Arc<Mutex<HashMap<&'static str, LatencySeries>>>);

#[derive(Clone, Copy, Debug, Default)]
pub struct LatencySeries {
    pub count: u64,
    pub total: Duration,
    pub min: Option<Duration>,
    pub max: Option<Duration>,
}

impl LatencyRecorder {
    pub fn record(&self, name: &'static str, latency: Duration) {
        let mut series = self.0.lock().expect("latency recorder lock poisoned");
        let series = series.entry(name).or_default();
        series.count += 1;
        series.total += latency;
        series.min = Some(series.min.map_or(latency, |current| current.min(latency)));
        series.max = Some(series.max.map_or(latency, |current| current.max(latency)));
    }

    pub fn snapshot(&self) -> HashMap<&'static str, LatencySeries> {
        self.0
            .lock()
            .expect("latency recorder lock poisoned")
            .clone()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SamplerReport {
    pub samples: u64,
    pub actors_seen: usize,
    pub root_restarts: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SamplerEvidence(Arc<Mutex<SamplerReport>>);

impl SamplerEvidence {
    fn observe(&self, scope: &ScopeRef) -> SamplerReport {
        let snapshot = scope.snapshot();
        let mut report = self.0.lock().expect("sampler evidence lock poisoned");
        report.samples += 1;
        report.actors_seen = scope.actor_stats().len();
        report.root_restarts = snapshot.total_restarts;
        *report
    }

    pub fn report(&self) -> SamplerReport {
        *self.0.lock().expect("sampler evidence lock poisoned")
    }
}

pub fn install_metrics() -> Result<Snapshotter, metrics::SetRecorderError<DebuggingRecorder>> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder)?;
    Ok(snapshotter)
}

pub async fn sample_snapshots(scope: ScopeRef, stop: CancellationToken, evidence: SamplerEvidence) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => {
                let report = evidence.observe(&scope);
                tracing::warn!(?report, snapshot = ?scope.snapshot(), "final supervisor sample");
                return;
            }
            _ = interval.tick() => {
                let report = evidence.observe(&scope);
                tracing::info!(?report, snapshot = ?scope.snapshot(), "supervisor sample");
            }
        }
    }
}
