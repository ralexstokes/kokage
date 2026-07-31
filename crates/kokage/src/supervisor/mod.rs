//! Private supervisor runtime implementation.

mod attachment;
mod builder;
mod cancellation;
mod child;
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

/// Internal bridge used by the actor-aware tree layer to attach process-local
/// metadata without exposing attachment machinery as public API.
#[doc(hidden)]
pub mod __private {
    use std::any::Any;

    pub use crate::supervisor::attachment::{AttachedChild, AttachedChildIdentity};
    use crate::supervisor::{CancellationToken, DynamicSupervisorHandle, Guard, SupervisorHandle};

    /// Returns process-local metadata from the current supervision tree.
    pub fn attached_children<T>(handle: &SupervisorHandle) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        handle.attached_children()
    }

    /// Returns process-local metadata from a dynamic supervision tree.
    pub fn dynamic_attached_children<T>(handle: &DynamicSupervisorHandle) -> Vec<AttachedChild<T>>
    where
        T: Any + Send + Sync,
    {
        handle.attached_children()
    }

    /// Builds a guard around cancellation and completion tokens for the actor layer.
    pub fn guard_from_tokens(
        cancellation: CancellationToken,
        finished: CancellationToken,
    ) -> Guard {
        Guard::from_tokens(cancellation, finished)
    }

    /// Builds a token-backed guard with a custom cancellation hook.
    pub fn guard_from_tokens_with_cancel(
        cancellation: CancellationToken,
        finished: CancellationToken,
        cancel_action: impl Fn() + Send + Sync + 'static,
    ) -> Guard {
        Guard::from_tokens_with_cancel(cancellation, finished, cancel_action)
    }
}

pub use builder::{DynamicSupervisorBuilder, OrderedSupervisorBuilder};
pub use cancellation::CancellationToken;
pub(crate) use cancellation::{CancelOnDrop, CompletionOnDrop};
pub(crate) use child::ChildSpec;
pub use child::{BoxError, TaskSpec};
pub use completion::CompletionError;
pub use context::TaskContext;
pub use error::{BuildError, ControlError, SupervisorError};
pub use guard::Guard;
pub use handle::{DynamicSupervisorHandle, SupervisorHandle};
pub use lifecycle::{LifecycleEvent, LifecycleEventKind, LifecycleObservation, LifecycleWatch};
pub use owner::{RunningSupervisor, Supervisor};
#[cfg(feature = "serde")]
pub(crate) use restart::RestartWire;
pub use restart::{Backoff, Restart};
pub use scope::{ScopeKind, ScopePathSegment};
pub use shutdown::Shutdown;
pub use snapshot::{
    ChildMembershipView, ChildSnapshot, ChildStateView, ExitStatus, SnapshotRecvError,
    SupervisorSnapshot, SupervisorSnapshotReceiver, SupervisorStateView,
};
pub use strategy::Strategy;

// Keep the former standalone crate's behavioral suite next to the private
// implementation. These are unit tests now so they can exercise the low-level
// runtime without restoring that layer to the public API.
#[cfg(test)]
mod tests;
