use crate::{
    actor::ExitResult,
    supervisor::{child::BoxError, event::ExitKind},
};

/// Internal exit classification used by the runtime before public projection
/// into [`ExitKind`].
#[derive(Debug)]
pub(crate) enum RuntimeExitStatus {
    Completed,
    Failed(BoxError),
    Panicked,
    Aborted,
    ShutdownTimedOut,
}

impl RuntimeExitStatus {
    pub(crate) fn from_child_result(result: ExitResult) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(err) => Self::Failed(err),
        }
    }

    pub(crate) fn is_failure(&self) -> bool {
        !matches!(self, Self::Completed)
    }

    pub(crate) fn view(&self) -> ExitKind {
        match self {
            Self::Completed => ExitKind::Completed,
            Self::Failed(err) => ExitKind::Failed(err.to_string()),
            Self::Panicked => ExitKind::Panicked,
            Self::Aborted => ExitKind::Aborted { after_grace: false },
            Self::ShutdownTimedOut => ExitKind::Aborted { after_grace: true },
        }
    }
}
