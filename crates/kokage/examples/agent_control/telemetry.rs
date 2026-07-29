//! Small application-owned latency aggregator used by the proving harness.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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
