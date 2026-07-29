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
    use crate::supervisor::{
        ChildSpec, DynamicSupervisorHandle, Restart, Shutdown, SupervisorHandle,
    };

    /// Adds process-local metadata to a child specification.
    pub fn attach<T>(child: ChildSpec, attachment: T) -> ChildSpec
    where
        T: Any + Send + Sync,
    {
        child.attachment(attachment)
    }

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

    /// Resolves one child's explicit policy overrides against scope defaults.
    pub fn child_policies(
        child: &ChildSpec,
        default_restart: Restart,
        default_shutdown: Shutdown,
    ) -> (Restart, Shutdown) {
        child.resolved_policies(default_restart, default_shutdown)
    }
}

pub use builder::{DynamicSupervisorBuilder, OrderedSupervisorBuilder};
pub use cancellation::CancellationToken;
pub use child::{BoxError, ChildResult, ChildSpec};
pub use completion::{CompletionError, CompletionOutcome};
pub use context::ChildContext;
pub use error::{BuildError, ControlError, SupervisorError};
pub use guard::Guard;
pub use handle::{DynamicSupervisorHandle, SupervisorHandle};
pub use lifecycle::{LifecycleEvent, LifecycleEventKind, LifecyclePathSegment, LifecycleWatch};
pub use owner::{RunningSupervisor, Supervisor};
pub use restart::{Backoff, Restart};
pub use scope::ScopeKind;
pub use shutdown::Shutdown;
pub use snapshot::{
    ChildExitView, ChildMembershipView, ChildSnapshot, ChildStateView, SnapshotRecvError,
    SupervisorSnapshot, SupervisorSnapshotReceiver, SupervisorStateView,
};
pub use strategy::Strategy;

// Keep the former standalone crate's behavioral suite next to the private
// implementation. These are unit tests now so they can exercise the low-level
// runtime without restoring that layer to the public API.
#[cfg(test)]
#[path = "tests/attachments.rs"]
mod attachments_integration;
#[cfg(test)]
#[path = "tests/builder.rs"]
mod builder_integration;
#[cfg(test)]
#[path = "tests/completion.rs"]
mod completion_integration;
#[cfg(test)]
#[path = "tests/context_supervisor.rs"]
mod context_supervisor_integration;
#[cfg(test)]
#[path = "tests/dynamic.rs"]
mod dynamic_integration;
#[cfg(test)]
#[path = "tests/intensity.rs"]
mod intensity_integration;
#[cfg(test)]
#[path = "tests/lifecycle.rs"]
mod lifecycle_integration;
#[cfg(test)]
#[path = "tests/nested.rs"]
mod nested_integration;
#[cfg(test)]
#[path = "tests/observation_api.rs"]
mod observation_api_integration;
#[cfg(test)]
#[path = "tests/one_for_all.rs"]
mod one_for_all_integration;
#[cfg(test)]
#[path = "tests/one_for_one.rs"]
mod one_for_one_integration;
#[cfg(test)]
#[path = "tests/panic.rs"]
mod panic_integration;
#[cfg(test)]
#[path = "tests/pre_spawn_handles.rs"]
mod pre_spawn_handles_integration;
#[cfg(test)]
#[path = "tests/rest_for_one.rs"]
mod rest_for_one_integration;
#[cfg(test)]
#[path = "tests/shutdown.rs"]
mod shutdown_integration;
#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod snapshot_integration;
#[cfg(test)]
#[path = "tests/startup.rs"]
mod startup_integration;
#[cfg(test)]
#[path = "tests/common/mod.rs"]
mod supervisor_test_common;
