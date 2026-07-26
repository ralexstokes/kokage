use std::time::Duration;

#[cfg(feature = "metrics")]
use metrics::{Label, counter, gauge, histogram};
use tracing::{debug, info, trace, warn};

use crate::{
    event::{ExitStatusView, SupervisorEvent},
    strategy::Strategy,
};

pub(crate) const ROOT_SUPERVISOR_NAME: &str = "root";

#[derive(Clone)]
pub(crate) struct SupervisorObservability {
    path_prefix: Vec<String>,
    supervisor_name: String,
    supervisor_path: String,
    strategy_label: &'static str,
}

impl SupervisorObservability {
    pub(crate) fn new(path_prefix: Vec<String>, strategy: Strategy) -> Self {
        let supervisor_name = supervisor_name_for_path(&path_prefix).to_owned();
        let supervisor_path = format_path(&path_prefix);

        Self {
            path_prefix,
            supervisor_name,
            supervisor_path,
            strategy_label: strategy_label(strategy),
        }
    }

    pub(crate) fn supervisor_name(&self) -> &str {
        &self.supervisor_name
    }

    pub(crate) fn supervisor_path(&self) -> &str {
        &self.supervisor_path
    }

    pub(crate) fn child_path(&self, child_id: &str) -> String {
        format_child_path(&self.path_prefix, child_id)
    }

    pub(crate) fn emit_event(
        &self,
        event: &SupervisorEvent,
        running_children: usize,
        child_path: Option<&str>,
    ) {
        self.emit_tracing_event(event, child_path);
        self.emit_metrics(event, running_children, child_path);
    }

    pub(crate) fn emit_nested_event_forwarding_lag(&self, dropped_events: u64) {
        warn!(
            supervisor_name = %self.supervisor_name,
            supervisor_path = %self.supervisor_path,
            dropped_events,
            "nested supervisor event forwarding lagged"
        );

        #[cfg(feature = "metrics")]
        counter!(
            "supervisor.events.dropped",
            "supervisor" => self.supervisor_name.clone(),
            "path" => self.supervisor_path.clone(),
            "strategy" => self.strategy_label,
        )
        .increment(dropped_events);
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn record_shutdown_timeout(&self, operation: &'static str, child_id: Option<&str>) {
        let labels = self.shutdown_metric_labels(operation, child_id);
        counter!("supervisor.shutdown_timeouts", labels).increment(1);
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn record_shutdown_timeout(
        &self,
        _operation: &'static str,
        _child_id: Option<&str>,
    ) {
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn record_shutdown_duration(
        &self,
        operation: &'static str,
        duration: Duration,
        child_id: Option<&str>,
    ) {
        let labels = self.shutdown_metric_labels(operation, child_id);
        histogram!("supervisor.child_shutdown.duration", labels).record(duration.as_secs_f64());
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn record_shutdown_duration(
        &self,
        _operation: &'static str,
        _duration: Duration,
        _child_id: Option<&str>,
    ) {
    }

    #[cfg(feature = "metrics")]
    fn shutdown_metric_labels(
        &self,
        operation: &'static str,
        child_id: Option<&str>,
    ) -> Vec<Label> {
        let mut labels = vec![
            Label::new("supervisor", self.supervisor_name.clone()),
            Label::new(
                "path",
                child_id.map_or_else(|| self.supervisor_path.clone(), |id| self.child_path(id)),
            ),
            Label::new("strategy", self.strategy_label),
            Label::new("operation", operation),
        ];
        if let Some(child_id) = child_id {
            labels.push(Label::new("child_id", child_id.to_owned()));
        }
        labels
    }

    fn resolve_child_path<'a>(&'a self, child_id: &str, precomputed: Option<&'a str>) -> String {
        match precomputed {
            Some(path) => path.to_owned(),
            None => self.child_path(child_id),
        }
    }

    fn emit_tracing_event(&self, event: &SupervisorEvent, child_path: Option<&str>) {
        match event {
            SupervisorEvent::SupervisorStarted => info!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                strategy = self.strategy_label,
                "supervisor started"
            ),
            SupervisorEvent::SupervisorStopping => debug!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                strategy = self.strategy_label,
                "supervisor stopping"
            ),
            SupervisorEvent::SupervisorStopped => info!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                strategy = self.strategy_label,
                "supervisor stopped"
            ),
            SupervisorEvent::Nested {
                id,
                generation,
                event,
            } => trace!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                nested_id = %id,
                nested_generation = *generation,
                nested_path = %self.resolve_child_path(id, child_path),
                leaf_kind = event_kind(event.leaf()),
                "nested event forwarded"
            ),
            SupervisorEvent::ChildStarted { id, generation } => info!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                child_id = %id,
                child_path = %self.resolve_child_path(id, child_path),
                generation = *generation,
                strategy = self.strategy_label,
                "child started"
            ),
            SupervisorEvent::ChildRemoved { id } => debug!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                child_id = %id,
                child_path = %self.resolve_child_path(id, child_path),
                strategy = self.strategy_label,
                "child removed"
            ),
            SupervisorEvent::ChildExited {
                id,
                generation,
                status,
            } => {
                let status_label = exit_status_label(status);
                match status {
                    ExitStatusView::Completed => trace!(
                        supervisor_name = %self.supervisor_name,
                        supervisor_path = %self.supervisor_path,
                        child_id = %id,
                        child_path = %self.resolve_child_path(id, child_path),
                        generation = *generation,
                        status = status_label,
                        strategy = self.strategy_label,
                        "child exited"
                    ),
                    ExitStatusView::Failed(message) => warn!(
                        supervisor_name = %self.supervisor_name,
                        supervisor_path = %self.supervisor_path,
                        child_id = %id,
                        child_path = %self.resolve_child_path(id, child_path),
                        generation = *generation,
                        status = status_label,
                        error = %message,
                        strategy = self.strategy_label,
                        "child exited"
                    ),
                    ExitStatusView::Panicked
                    | ExitStatusView::Aborted
                    | ExitStatusView::ShutdownTimedOut => warn!(
                        supervisor_name = %self.supervisor_name,
                        supervisor_path = %self.supervisor_path,
                        child_id = %id,
                        child_path = %self.resolve_child_path(id, child_path),
                        generation = *generation,
                        status = status_label,
                        strategy = self.strategy_label,
                        "child exited"
                    ),
                }
            }
            SupervisorEvent::AutoShutdownTriggered { id, mode } => info!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                child_id = %id,
                child_path = %self.resolve_child_path(id, child_path),
                mode = ?mode,
                strategy = self.strategy_label,
                "automatic supervisor shutdown triggered"
            ),
            SupervisorEvent::ChildRestartScheduled {
                id,
                generation,
                delay,
            } => warn!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                child_id = %id,
                child_path = %self.resolve_child_path(id, child_path),
                generation = *generation,
                delay_ms = delay.as_millis() as u64,
                strategy = self.strategy_label,
                "child restart scheduled"
            ),
            SupervisorEvent::ChildRestarted {
                id,
                old_generation,
                new_generation,
            } => info!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                child_id = %id,
                child_path = %self.resolve_child_path(id, child_path),
                old_generation = *old_generation,
                new_generation = *new_generation,
                strategy = self.strategy_label,
                "child restarted"
            ),
            SupervisorEvent::RestartIntensityExceeded => warn!(
                supervisor_name = %self.supervisor_name,
                supervisor_path = %self.supervisor_path,
                strategy = self.strategy_label,
                "restart intensity exceeded"
            ),
        }
    }

    #[cfg(feature = "metrics")]
    fn emit_metrics(
        &self,
        event: &SupervisorEvent,
        running_children: usize,
        child_path: Option<&str>,
    ) {
        gauge!(
            "supervisor.children.running",
            "supervisor" => self.supervisor_name.clone(),
            "path" => self.supervisor_path.clone(),
            "strategy" => self.strategy_label,
        )
        .set(running_children as f64);

        match event {
            SupervisorEvent::ChildStarted { id, .. } => {
                counter!(
                    "supervisor.children.started",
                    "supervisor" => self.supervisor_name.clone(),
                    "path" => self.resolve_child_path(id, child_path),
                    "child_id" => id.clone(),
                    "strategy" => self.strategy_label,
                )
                .increment(1);
            }
            SupervisorEvent::ChildExited { id, status, .. } => {
                counter!(
                    "supervisor.children.exited",
                    "supervisor" => self.supervisor_name.clone(),
                    "path" => self.resolve_child_path(id, child_path),
                    "child_id" => id.clone(),
                    "strategy" => self.strategy_label,
                    "status" => exit_status_label(status),
                )
                .increment(1);
            }
            SupervisorEvent::ChildRestarted { id, .. } => {
                counter!(
                    "supervisor.restarts",
                    "supervisor" => self.supervisor_name.clone(),
                    "path" => self.resolve_child_path(id, child_path),
                    "child_id" => id.clone(),
                    "strategy" => self.strategy_label,
                )
                .increment(1);
            }
            SupervisorEvent::RestartIntensityExceeded => {
                counter!(
                    "supervisor.restart_intensity_exceeded",
                    "supervisor" => self.supervisor_name.clone(),
                    "path" => self.supervisor_path.clone(),
                    "strategy" => self.strategy_label,
                )
                .increment(1);
            }
            SupervisorEvent::AutoShutdownTriggered { id, mode } => {
                counter!(
                    "supervisor.auto_shutdowns",
                    "supervisor" => self.supervisor_name.clone(),
                    "path" => self.supervisor_path.clone(),
                    "child_id" => id.clone(),
                    "mode" => auto_shutdown_label(*mode),
                    "strategy" => self.strategy_label,
                )
                .increment(1);
            }
            SupervisorEvent::SupervisorStarted
            | SupervisorEvent::SupervisorStopping
            | SupervisorEvent::SupervisorStopped
            | SupervisorEvent::Nested { .. }
            | SupervisorEvent::ChildRemoved { .. }
            | SupervisorEvent::ChildRestartScheduled { .. } => {}
        }
    }

    #[cfg(not(feature = "metrics"))]
    fn emit_metrics(
        &self,
        _event: &SupervisorEvent,
        _running_children: usize,
        _child_path: Option<&str>,
    ) {
    }
}

pub(crate) fn format_path(path_prefix: &[String]) -> String {
    if path_prefix.is_empty() {
        ROOT_SUPERVISOR_NAME.to_owned()
    } else {
        format!("{ROOT_SUPERVISOR_NAME}.{}", path_prefix.join("."))
    }
}

pub(crate) fn format_child_path(path_prefix: &[String], child_id: &str) -> String {
    let mut path = path_prefix.to_vec();
    path.push(child_id.to_owned());
    format_path(&path)
}

pub(crate) fn supervisor_name_for_path(path_prefix: &[String]) -> &str {
    path_prefix
        .last()
        .map(std::string::String::as_str)
        .unwrap_or(ROOT_SUPERVISOR_NAME)
}

pub(crate) fn strategy_label(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::OneForOne => "one_for_one",
        Strategy::OneForAll => "one_for_all",
        Strategy::RestForOne => "rest_for_one",
    }
}

fn exit_status_label(status: &ExitStatusView) -> &'static str {
    match status {
        ExitStatusView::Completed => "completed",
        ExitStatusView::Failed(_) => "failed",
        ExitStatusView::Panicked => "panicked",
        ExitStatusView::Aborted => "aborted",
        ExitStatusView::ShutdownTimedOut => "shutdown_timed_out",
    }
}

fn event_kind(event: &SupervisorEvent) -> &'static str {
    match event {
        SupervisorEvent::SupervisorStarted => "supervisor_started",
        SupervisorEvent::SupervisorStopping => "supervisor_stopping",
        SupervisorEvent::SupervisorStopped => "supervisor_stopped",
        SupervisorEvent::Nested { .. } => "nested",
        SupervisorEvent::ChildStarted { .. } => "child_started",
        SupervisorEvent::ChildRemoved { .. } => "child_removed",
        SupervisorEvent::ChildExited { .. } => "child_exited",
        SupervisorEvent::AutoShutdownTriggered { .. } => "auto_shutdown_triggered",
        SupervisorEvent::ChildRestartScheduled { .. } => "child_restart_scheduled",
        SupervisorEvent::ChildRestarted { .. } => "child_restarted",
        SupervisorEvent::RestartIntensityExceeded => "restart_intensity_exceeded",
    }
}

#[cfg(feature = "metrics")]
fn auto_shutdown_label(mode: crate::AutoShutdown) -> &'static str {
    match mode {
        crate::AutoShutdown::Never => "never",
        crate::AutoShutdown::AnySignificant => "any_significant",
        crate::AutoShutdown::AllSignificant => "all_significant",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[cfg(feature = "metrics")]
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tracing::Level;
    use tracing_subscriber::{
        fmt::{self, MakeWriter},
        prelude::*,
    };

    use super::*;

    #[test]
    fn path_formatting_uses_root_prefix() {
        assert_eq!(format_path(&[]), "root");
        assert_eq!(format_path(&["nested".to_owned()]), "root.nested");
        assert_eq!(
            format_child_path(&["nested".to_owned()], "leaf"),
            "root.nested.leaf"
        );
    }

    #[test]
    fn tracing_output_covers_event_variants_and_nested_paths() {
        let root = SupervisorObservability::new(Vec::new(), Strategy::OneForOne);
        let nested = SupervisorObservability::new(vec!["nested".to_owned()], Strategy::OneForAll);

        assert_tracing_output(
            || root.emit_event(&SupervisorEvent::SupervisorStarted, 0, None),
            &["supervisor started", r#""supervisor_path":"root""#],
        );
        assert_tracing_output(
            || root.emit_event(&SupervisorEvent::SupervisorStopping, 0, None),
            &["supervisor stopping", r#""supervisor_path":"root""#],
        );
        assert_tracing_output(
            || root.emit_event(&SupervisorEvent::SupervisorStopped, 0, None),
            &["supervisor stopped", r#""supervisor_path":"root""#],
        );
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::Nested {
                        id: "inner".to_owned(),
                        generation: 2,
                        event: Box::new(SupervisorEvent::SupervisorStarted),
                    },
                    0,
                    None,
                )
            },
            &["nested event forwarded", r#""nested_path":"root.inner""#],
        );
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::ChildStarted {
                        id: "worker".to_owned(),
                        generation: 0,
                    },
                    1,
                    None,
                )
            },
            &["child started", r#""child_path":"root.worker""#],
        );
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::ChildRemoved {
                        id: "worker".to_owned(),
                    },
                    0,
                    None,
                )
            },
            &["child removed", r#""child_path":"root.worker""#],
        );
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::ChildExited {
                        id: "worker".to_owned(),
                        generation: 0,
                        status: ExitStatusView::Failed("boom".to_owned()),
                    },
                    0,
                    None,
                )
            },
            &[
                "child exited",
                r#""child_path":"root.worker""#,
                r#""status":"failed""#,
            ],
        );
        for status in [
            ExitStatusView::Panicked,
            ExitStatusView::Aborted,
            ExitStatusView::ShutdownTimedOut,
        ] {
            let output = capture_tracing_output(|| {
                root.emit_event(
                    &SupervisorEvent::ChildExited {
                        id: "worker".to_owned(),
                        generation: 0,
                        status,
                    },
                    0,
                    None,
                );
            });
            assert!(output.contains("child exited"), "got: {output}");
            assert!(output.contains("\"status\""), "got: {output}");
            assert!(!output.contains("\"error\""), "got: {output}");
        }
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::ChildRestartScheduled {
                        id: "worker".to_owned(),
                        generation: 0,
                        delay: Duration::from_millis(10),
                    },
                    0,
                    None,
                )
            },
            &["child restart scheduled", r#""child_path":"root.worker""#],
        );
        assert_tracing_output(
            || {
                root.emit_event(
                    &SupervisorEvent::ChildRestarted {
                        id: "worker".to_owned(),
                        old_generation: 0,
                        new_generation: 1,
                    },
                    1,
                    None,
                )
            },
            &["child restarted", r#""child_path":"root.worker""#],
        );
        assert_tracing_output(
            || root.emit_event(&SupervisorEvent::RestartIntensityExceeded, 0, None),
            &["restart intensity exceeded", r#""supervisor_path":"root""#],
        );
        assert_tracing_output(
            || {
                nested.emit_event(
                    &SupervisorEvent::ChildStarted {
                        id: "leaf".to_owned(),
                        generation: 3,
                    },
                    1,
                    None,
                )
            },
            &[
                "child started",
                r#""supervisor_path":"root.nested""#,
                r#""child_path":"root.nested.leaf""#,
            ],
        );
        assert_tracing_output(
            || nested.emit_nested_event_forwarding_lag(7),
            &[
                "nested supervisor event forwarding lagged",
                r#""supervisor_path":"root.nested""#,
                r#""dropped_events":7"#,
            ],
        );
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn metrics_cover_event_counters_timeout_and_duration_helpers() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let observability =
                SupervisorObservability::new(vec!["nested".to_owned()], Strategy::OneForOne);

            observability.emit_event(
                &SupervisorEvent::ChildStarted {
                    id: "leaf".to_owned(),
                    generation: 1,
                },
                1,
                None,
            );
            observability.emit_event(
                &SupervisorEvent::ChildExited {
                    id: "leaf".to_owned(),
                    generation: 1,
                    status: ExitStatusView::Failed("boom".to_owned()),
                },
                0,
                None,
            );
            observability.emit_event(
                &SupervisorEvent::ChildRestarted {
                    id: "leaf".to_owned(),
                    old_generation: 1,
                    new_generation: 2,
                },
                1,
                None,
            );
            observability.emit_event(&SupervisorEvent::RestartIntensityExceeded, 0, None);
            observability.emit_nested_event_forwarding_lag(3);
            observability.record_shutdown_timeout("remove_child", Some("leaf"));
            observability.record_shutdown_duration(
                "remove_child",
                Duration::from_millis(25),
                Some("leaf"),
            );
        });

        let metrics = snapshotter.snapshot().into_vec();

        assert_counter(
            &metrics,
            "supervisor.children.started",
            &[("child_id", "leaf"), ("path", "root.nested.leaf")],
            1,
        );
        assert_counter(
            &metrics,
            "supervisor.children.exited",
            &[
                ("child_id", "leaf"),
                ("path", "root.nested.leaf"),
                ("status", "failed"),
            ],
            1,
        );
        assert_counter(
            &metrics,
            "supervisor.restarts",
            &[("child_id", "leaf"), ("path", "root.nested.leaf")],
            1,
        );
        assert_counter(
            &metrics,
            "supervisor.restart_intensity_exceeded",
            &[("path", "root.nested")],
            1,
        );
        assert_counter(
            &metrics,
            "supervisor.events.dropped",
            &[("path", "root.nested")],
            3,
        );
        assert_counter(
            &metrics,
            "supervisor.shutdown_timeouts",
            &[
                ("child_id", "leaf"),
                ("operation", "remove_child"),
                ("path", "root.nested.leaf"),
            ],
            1,
        );
        assert_gauge(
            &metrics,
            "supervisor.children.running",
            &[("path", "root.nested")],
            0.0,
        );
        assert_histogram_len(
            &metrics,
            "supervisor.child_shutdown.duration",
            &[
                ("child_id", "leaf"),
                ("operation", "remove_child"),
                ("path", "root.nested.leaf"),
            ],
            1,
        );
    }

    fn capture_tracing_output(f: impl FnOnce()) -> String {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .json()
                .with_writer(buffer.clone())
                .with_current_span(false)
                .with_span_list(false)
                .without_time()
                .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
                    Level::TRACE,
                )),
        );

        tracing::subscriber::with_default(subscriber, f);
        buffer.to_string_output()
    }

    fn assert_tracing_output(f: impl FnOnce(), expected_fragments: &[&str]) {
        let output = capture_tracing_output(f);
        for expected in expected_fragments {
            assert!(
                output.contains(expected),
                "expected tracing output to contain `{expected}`, got: {output}"
            );
        }
    }

    #[derive(Clone, Default)]
    struct SharedBuffer {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedBuffer {
        fn to_string_output(&self) -> String {
            String::from_utf8(self.inner.lock().expect("buffer poisoned").clone())
                .expect("tracing output should be utf-8")
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedWriter {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    struct SharedWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner
                .lock()
                .expect("buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "metrics")]
    fn assert_counter(
        metrics: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
        labels: &[(&str, &str)],
        expected: u64,
    ) {
        let value = find_metric(metrics, name, labels);
        match value {
            DebugValue::Counter(actual) => assert_eq!(*actual, expected),
            other => panic!("expected counter for `{name}`, got {other:?}"),
        }
    }

    #[cfg(feature = "metrics")]
    fn assert_gauge(
        metrics: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
        labels: &[(&str, &str)],
        expected: f64,
    ) {
        let value = find_metric(metrics, name, labels);
        match value {
            DebugValue::Gauge(actual) => assert_eq!(actual.into_inner(), expected),
            other => panic!("expected gauge for `{name}`, got {other:?}"),
        }
    }

    #[cfg(feature = "metrics")]
    fn assert_histogram_len(
        metrics: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
        labels: &[(&str, &str)],
        expected: usize,
    ) {
        let value = find_metric(metrics, name, labels);
        match value {
            DebugValue::Histogram(values) => assert_eq!(values.len(), expected),
            other => panic!("expected histogram for `{name}`, got {other:?}"),
        }
    }

    #[cfg(feature = "metrics")]
    fn find_metric<'a>(
        metrics: &'a [(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
        labels: &[(&str, &str)],
    ) -> &'a DebugValue {
        metrics
            .iter()
            .find_map(|(key, _, _, value)| {
                let metric_key = key.key();
                if metric_key.name() != name {
                    return None;
                }

                let actual_labels: Vec<(&str, &str)> = metric_key
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect();
                if labels
                    .iter()
                    .all(|expected| actual_labels.contains(expected))
                {
                    Some(value)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("missing metric `{name}` with labels {labels:?}"))
    }
}
