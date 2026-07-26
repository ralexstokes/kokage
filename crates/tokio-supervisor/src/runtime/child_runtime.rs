use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};

use tokio::{task::AbortHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    child::ChildDefinition, restart::RestartIntensity, runtime::intensity::RestartTracker,
};

const COMPLETION_PENDING: u8 = 0;
const COMPLETION_CANCELLED: u8 = 1;
const COMPLETION_CLEAN: u8 = 2;

/// Orders a child's natural clean return against supervisor-driven
/// cancellation. The winner of the transition out of `PENDING` determines
/// whether a completed join is treated as natural or cancellation-induced.
#[derive(Clone)]
pub(crate) struct CompletionFlag(Arc<AtomicU8>);

impl CompletionFlag {
    pub(crate) fn pending() -> Self {
        Self(Arc::new(AtomicU8::new(COMPLETION_PENDING)))
    }

    pub(crate) fn mark_cancelled(&self) {
        let _ = self.0.compare_exchange(
            COMPLETION_PENDING,
            COMPLETION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn mark_clean(&self) {
        let _ = self.0.compare_exchange(
            COMPLETION_PENDING,
            COMPLETION_CLEAN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn is_clean(&self) -> bool {
        self.0.load(Ordering::Acquire) == COMPLETION_CLEAN
    }
}

/// Mutable per-child state managed by the supervisor runtime.
///
/// Tracks the child's current lifecycle state, its restart history, and the
/// handles needed to cancel or abort the running Tokio task.
pub(crate) struct ChildRuntime {
    pub(crate) definition: Arc<ChildDefinition>,
    pub(crate) restart_tracker: RestartTracker,
    pub(crate) generation: u64,
    pub(crate) state: RuntimeChildState,
    pub(crate) active_token: Option<CancellationToken>,
    pub(crate) active_abort_token: Option<CancellationToken>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) nested_abort_cascades: Arc<AtomicBool>,
    pub(crate) has_started: bool,
    pub(crate) has_reported_ready: bool,
    pub(crate) startup_aborted: bool,
    pub(crate) next_restart_deadline: Option<Instant>,
    pub(crate) completion: CompletionFlag,
    pub(crate) shutdown_timed_out: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeChildState {
    StartQueued,
    Starting,
    Running,
    Stopping,
    Stopped,
}

impl RuntimeChildState {
    pub(crate) fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }
}

impl ChildRuntime {
    pub(crate) fn new(
        definition: Arc<ChildDefinition>,
        default_restart_intensity: RestartIntensity,
    ) -> Self {
        let restart_intensity = definition
            .restart_intensity
            .unwrap_or(default_restart_intensity);
        Self {
            definition,
            restart_tracker: RestartTracker::new(restart_intensity),
            generation: 0,
            state: RuntimeChildState::Stopped,
            active_token: None,
            active_abort_token: None,
            abort_handle: None,
            nested_abort_cascades: Arc::new(AtomicBool::new(true)),
            has_started: false,
            has_reported_ready: false,
            startup_aborted: false,
            next_restart_deadline: None,
            completion: CompletionFlag::pending(),
            shutdown_timed_out: false,
        }
    }
}
