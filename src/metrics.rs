use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};
#[derive(Clone, Default, Serialize)]
pub struct SourceMetric {
    pub requests: u64,
    pub elapsed_ms: f64,
    pub response_bytes: Option<u64>,
    pub arrow_bytes: u64,
    pub rows: u64,
    pub remote_bytes_processed: Option<u64>,
}
#[derive(Clone, Default)]
pub struct Metrics(pub Arc<Mutex<BTreeMap<String, SourceMetric>>>);
impl Metrics {
    pub fn update(&self, name: &str, f: impl FnOnce(&mut SourceMetric)) {
        f(self.0.lock().unwrap().entry(name.into()).or_default());
    }
    pub fn snapshot(&self) -> BTreeMap<String, SourceMetric> {
        self.0.lock().unwrap().clone()
    }
    pub fn timer(&self, name: &str) -> Timer {
        self.update(name, |m| m.requests += 1);
        Timer {
            metrics: self.clone(),
            name: name.into(),
            start: Instant::now(),
        }
    }
}
pub struct Timer {
    metrics: Metrics,
    name: String,
    start: Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.metrics.update(&self.name, |m| {
            m.elapsed_ms += self.start.elapsed().as_secs_f64() * 1000.
        });
    }
}
