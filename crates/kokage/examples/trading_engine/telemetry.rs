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
    pub fn record(&self, name: &'static str, value: Duration) {
        let mut all = self.0.lock().expect("latency lock poisoned");
        let series = all.entry(name).or_default();
        series.count += 1;
        series.total += value;
        series.min = Some(series.min.map_or(value, |current| current.min(value)));
        series.max = Some(series.max.map_or(value, |current| current.max(value)));
    }

    pub fn snapshot(&self) -> HashMap<&'static str, LatencySeries> {
        self.0.lock().expect("latency lock poisoned").clone()
    }
}

pub fn install_metrics() -> Result<Snapshotter, metrics::SetRecorderError<DebuggingRecorder>> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder)?;
    Ok(snapshotter)
}

pub async fn sample(runtime: ScopeRef, stop: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.cancelled() => {
                // The periodic INFO samples stay quiet under the example's
                // WARN filter; emit one visible sample as acceptance evidence.
                tracing::warn!(
                    snapshot = ?runtime.snapshot(),
                    actor_stats = ?runtime.actor_stats(),
                    "final trading engine telemetry sample"
                );
                break;
            },
            _ = interval.tick() => {
                tracing::info!(
                    snapshot = ?runtime.snapshot(),
                    actor_stats = ?runtime.actor_stats(),
                    "trading engine telemetry sample"
                );
            }
        }
    }
}
