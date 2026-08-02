//! Private supervisor runtime implementation.

mod attachment;
mod builder;
mod cancellation;
mod child;
#[cfg(test)]
mod completion;
mod context;
mod error;
mod event;
mod guard;
mod handle;
mod lifecycle;
mod observability;
mod owner;
mod restart;
mod runtime;
mod scope;
mod shutdown;
mod snapshot;
mod strategy;

pub(crate) mod private {
    use std::any::Any;

    pub(crate) use crate::supervisor::attachment::{AttachedChild, AttachedChildIdentity};
    use crate::supervisor::{CancellationToken, DynamicSupervisorHandle, Guard, SupervisorHandle};

    pub(crate) fn attached_children<T>(handle: &SupervisorHandle) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        handle.attached_children()
    }

    pub(crate) fn dynamic_attached_children<T>(
        handle: &DynamicSupervisorHandle,
    ) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        handle.attached_children()
    }

    pub(crate) fn guard_from_tokens(
        cancellation: CancellationToken,
        finished: CancellationToken,
    ) -> Guard {
        Guard::from_tokens(cancellation, finished)
    }

    pub(crate) fn guard_from_tokens_with_cancel(
        cancellation: CancellationToken,
        finished: CancellationToken,
        cancel_action: impl Fn() + Send + Sync + 'static,
    ) -> Guard {
        Guard::from_tokens_with_cancel(cancellation, finished, cancel_action)
    }
}

pub(crate) use builder::{DynamicSupervisorBuilder, OrderedSupervisorBuilder};
pub use cancellation::CancellationToken;
pub(crate) use cancellation::{CancelOnDrop, CompletionOnDrop};
pub(crate) use child::ChildSpec;
pub use child::{BoxError, OneShotTaskSpec, TaskSpec};
#[cfg(test)]
pub(crate) use completion::CompletionError;
pub use context::TaskContext;
pub use error::{BuildError, ControlError, SupervisorError};
pub use guard::Guard;
pub(crate) use handle::{DynamicSupervisorHandle, SupervisorHandle};
pub use lifecycle::{
    ChildEvent, ChildEventKind, ChildObservationUpdate, ChildObservationWatch, LifecycleEvent,
    LifecycleEventKind, LifecycleObservation, LifecycleWatch,
};
pub(crate) use owner::{RunningSupervisor, Supervisor};
pub use restart::{Backoff, RestartPolicy};
pub(crate) use runtime::exit::ActorChildReadinessTimedOut;
pub use scope::{ScopeKind, ScopePathSegment};
pub use shutdown::{MailboxShutdown, Shutdown};
pub use snapshot::{
    ChildMembershipView, ChildSnapshot, ChildStateView, ExitStatus, SnapshotRecvError,
    SupervisorSnapshot, SupervisorSnapshotReceiver, SupervisorStateView,
};
pub use strategy::Strategy;

// Keep the low-level runtime's behavioral suite next to its private
// implementation so unit tests can exercise its internal contracts directly.
#[cfg(test)]
mod tests;
