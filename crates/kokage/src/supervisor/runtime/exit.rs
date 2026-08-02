use crate::{
    actor::{ActorRunError, ExitResult},
    supervisor::{child::BoxError, event::ExitKind},
};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("task `{task_id}` did not report readiness within {timeout:?}")]
pub(crate) struct TaskReadinessTimedOut {
    pub(crate) task_id: String,
    pub(crate) timeout: Duration,
}

/// Internal exit classification used by the runtime before public projection
/// into [`ExitKind`].
#[derive(Debug)]
pub(crate) enum RuntimeExitStatus {
    Completed,
    Failed(BoxError),
    ReadinessTimedOut(Duration),
    Panicked,
    Aborted,
    ShutdownTimedOut,
}

impl RuntimeExitStatus {
    pub(crate) fn from_child_result(result: ExitResult) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(err) => match err.downcast::<TaskReadinessTimedOut>() {
                Ok(timeout) => Self::ReadinessTimedOut(timeout.timeout),
                Err(err) => match err.downcast::<ActorRunError>() {
                    Ok(error) => match *error {
                        ActorRunError::ReadinessTimedOut { timeout, .. } => {
                            Self::ReadinessTimedOut(timeout)
                        }
                        error => Self::Failed(Box::new(error)),
                    },
                    Err(err) => Self::Failed(err),
                },
            },
        }
    }

    pub(crate) fn is_failure(&self) -> bool {
        !matches!(self, Self::Completed)
    }

    pub(crate) fn view(&self) -> ExitKind {
        match self {
            Self::Completed => ExitKind::Completed,
            Self::Failed(err) => ExitKind::Failed(err.to_string()),
            Self::ReadinessTimedOut(timeout) => ExitKind::ReadinessTimedOut(*timeout),
            Self::Panicked => ExitKind::Panicked,
            Self::Aborted => ExitKind::Aborted { after_grace: false },
            Self::ShutdownTimedOut => ExitKind::Aborted { after_grace: true },
        }
    }
}
