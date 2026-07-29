use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "metrics")]
use std::sync::OnceLock;

use tracing::{Span, debug, info, info_span, trace, warn};

#[cfg(feature = "metrics")]
use metrics::{Counter, Histogram, counter, histogram};

fn saturating_decrement(counter: &AtomicUsize) -> usize {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let next = current.saturating_sub(1);
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

/// Per-scope lifecycle tracing state retained by actor runners.
#[derive(Clone, Debug)]
pub(crate) struct ScopeObservability {
    running_actors: Arc<AtomicUsize>,
}

impl ScopeObservability {
    pub(crate) fn new() -> Self {
        Self {
            running_actors: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn actor_span(&self, actor_id: &str) -> Span {
        info_span!("actor", actor_id = %actor_id)
    }

    pub(crate) fn emit_actor_started(&self, actor_id: &Arc<str>) {
        let running = self.running_actors.fetch_add(1, Ordering::AcqRel) + 1;
        info!(
            actor_id = %actor_id,
            running_actors = running,
            "actor started"
        );
    }

    pub(crate) fn emit_actor_exited(
        &self,
        actor_id: &Arc<str>,
        status: ActorExitStatus,
        error: Option<&str>,
    ) {
        let running = saturating_decrement(&self.running_actors);
        if status == ActorExitStatus::Shutdown {
            debug!(
                actor_id = %actor_id,
                status = status.as_str(),
                running_actors = running,
                "actor exited"
            );
        } else if let Some(error) = error {
            warn!(
                actor_id = %actor_id,
                status = status.as_str(),
                error = %error,
                running_actors = running,
                "actor exited"
            );
        } else {
            warn!(
                actor_id = %actor_id,
                status = status.as_str(),
                running_actors = running,
                "actor exited"
            );
        }
    }

    pub(crate) fn emit_mailbox_bound(&self, actor_id: &Arc<str>) {
        info!(actor_id = %actor_id, "actor mailbox bound");
    }

    pub(crate) fn emit_mailbox_cleared(&self, actor_id: &Arc<str>) {
        debug!(actor_id = %actor_id, "actor mailbox cleared");
    }

    /// Reports continuations that were queued but never taken, because the
    /// actor was already stopping when they were pushed.
    ///
    /// The stage split turns most no-op `continue_with` calls into compile
    /// errors. This is the case it cannot reach: the drain loop hands a
    /// draining handler the same `MessageContext` as the ordinary loop, so the
    /// call type-checks and the message is dropped with the incarnation. The
    /// phase is readable at runtime through `LiveContext::status`, so a
    /// handler that wants to avoid the warning can check before queueing.
    pub(crate) fn emit_continuations_dropped(&self, actor_id: &Arc<str>, dropped: usize) {
        warn!(
            actor_id = %actor_id,
            dropped_continuations = dropped,
            "continuations queued while the actor was stopping were dropped"
        );
    }

    pub(crate) fn emit_message_received(&self, actor_id: &Arc<str>) {
        trace!(actor_id = %actor_id, "message received");
    }
}

/// Emits the per-message send trace without attaching scope lifecycle state to
/// restart-stable actor references.
pub(crate) fn trace_actor_message(
    source_actor_id: Option<&str>,
    target_actor_id: &Arc<str>,
    operation: MessageOperation,
    rejection: Option<SendRejection>,
) {
    match (source_actor_id, rejection) {
        (Some(source_actor_id), Some(rejection)) => trace!(
            source_actor_id = %source_actor_id,
            actor_id = %target_actor_id,
            operation = operation.as_str(),
            reason = rejection.as_str(),
            "message rejected"
        ),
        (Some(source_actor_id), None) => trace!(
            source_actor_id = %source_actor_id,
            actor_id = %target_actor_id,
            operation = operation.as_str(),
            "message sent"
        ),
        (None, Some(rejection)) => trace!(
            actor_id = %target_actor_id,
            operation = operation.as_str(),
            reason = rejection.as_str(),
            "message rejected"
        ),
        (None, None) => trace!(
            actor_id = %target_actor_id,
            operation = operation.as_str(),
            "message sent"
        ),
    }
}

#[derive(Debug)]
pub(crate) struct MessageSizeMetrics {
    #[cfg(feature = "metrics")]
    actor_id: Arc<str>,
    #[cfg(feature = "metrics")]
    handles: OnceLock<MessageSizeMetricHandles>,
}

#[cfg(feature = "metrics")]
#[derive(Debug)]
struct MessageSizeMetricHandles {
    bytes_accepted: Counter,
    size: Histogram,
}

impl MessageSizeMetrics {
    pub(crate) fn new(actor_id: &Arc<str>) -> Self {
        #[cfg(feature = "metrics")]
        {
            Self {
                actor_id: actor_id.clone(),
                handles: OnceLock::new(),
            }
        }

        #[cfg(not(feature = "metrics"))]
        {
            let _ = actor_id;
            Self {}
        }
    }

    pub(crate) fn record(&self, size: usize) {
        #[cfg(feature = "metrics")]
        {
            let handles = self.handles.get_or_init(|| {
                let actor_id = self.actor_id.to_string();
                MessageSizeMetricHandles {
                    bytes_accepted: counter!(
                        "actor.message.bytes_accepted",
                        "actor_id" => actor_id.clone()
                    ),
                    size: histogram!("actor.message.size", "actor_id" => actor_id),
                }
            });
            handles.bytes_accepted.increment(size as u64);
            handles.size.record(size as f64);
        }

        #[cfg(not(feature = "metrics"))]
        let _ = size;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorExitStatus {
    Shutdown,
    Stopped,
    Failed,
    Panicked,
    Cancelled,
    ShutdownTimedOut,
}

impl ActorExitStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Panicked => "panicked",
            Self::Cancelled => "cancelled",
            Self::ShutdownTimedOut => "shutdown_timed_out",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MessageOperation {
    Send,
    TrySend,
}

impl MessageOperation {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::TrySend => "try_send",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SendRejection {
    MailboxFull,
    MailboxClosed,
    NotRunning,
    ActorTerminated,
}

impl SendRejection {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MailboxFull => "mailbox_full",
            Self::MailboxClosed => "mailbox_closed",
            Self::NotRunning => "not_running",
            Self::ActorTerminated => "actor_terminated",
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "metrics")]
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[test]
    fn actor_exit_running_count_saturates_at_zero() {
        let observability = ScopeObservability::new();
        observability.emit_actor_exited(&Arc::from("worker"), ActorExitStatus::Cancelled, None);
        assert_eq!(observability.running_actors.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn message_size_metrics_record_bytes_and_histogram_samples() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let metrics = MessageSizeMetrics::new(&Arc::from("worker"));
            metrics.record(4);
            metrics.record(7);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name| {
            snapshot
                .iter()
                .find(|(key, _, _, _)| {
                    key.key().name() == name
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "actor_id" && label.value() == "worker")
                })
                .map(|(_, _, _, value)| value)
                .unwrap_or_else(|| panic!("metric `{name}` not found"))
        };

        assert!(matches!(
            find("actor.message.bytes_accepted"),
            DebugValue::Counter(11)
        ));
        assert!(matches!(
            find("actor.message.size"),
            DebugValue::Histogram(values) if values == &[4.0, 7.0]
        ));
    }
}
