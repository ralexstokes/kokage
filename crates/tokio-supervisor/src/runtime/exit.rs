use crate::{
    child::{BoxError, ChildResult},
    event::ExitStatusView,
};

/// Internal exit classification used by the runtime before public projection
/// into [`ExitStatusView`].
#[derive(Debug)]
pub(crate) enum ExitStatus {
    Completed,
    Failed(BoxError),
    Panicked,
    Aborted,
    ShutdownTimedOut,
}

impl ExitStatus {
    pub(crate) fn from_child_result(result: ChildResult) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(err) => Self::Failed(err),
        }
    }

    pub(crate) fn is_failure(&self) -> bool {
        !matches!(self, Self::Completed)
    }

    pub(crate) fn view(&self) -> ExitStatusView {
        match self {
            Self::Completed => ExitStatusView::Completed,
            Self::Failed(err) => ExitStatusView::Failed(err.to_string()),
            Self::Panicked => ExitStatusView::Panicked,
            Self::Aborted => ExitStatusView::Aborted { after_grace: false },
            Self::ShutdownTimedOut => ExitStatusView::Aborted { after_grace: true },
        }
    }
}
