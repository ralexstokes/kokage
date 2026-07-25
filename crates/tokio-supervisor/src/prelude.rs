//! Common imports and extension traits for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

use tokio::sync::{broadcast, watch};

/// Extension trait for `broadcast::Receiver<SupervisorEvent>` that adds a
/// convenience method for waiting until a specific event arrives.
#[allow(async_fn_in_trait)]
pub trait SupervisorEventReceiverExt {
    /// Receives events in a loop, returning the first one for which
    /// `predicate` returns `true`. Intermediate events are discarded.
    async fn wait_for_event<P>(
        &mut self,
        predicate: P,
    ) -> Result<crate::SupervisorEvent, broadcast::error::RecvError>
    where
        P: FnMut(&crate::SupervisorEvent) -> bool;
}

impl SupervisorEventReceiverExt for broadcast::Receiver<crate::SupervisorEvent> {
    async fn wait_for_event<P>(
        &mut self,
        mut predicate: P,
    ) -> Result<crate::SupervisorEvent, broadcast::error::RecvError>
    where
        P: FnMut(&crate::SupervisorEvent) -> bool,
    {
        loop {
            let event = self.recv().await?;
            if predicate(&event) {
                return Ok(event);
            }
        }
    }
}

/// Extension trait for `watch::Receiver<SupervisorSnapshot>` that adds a
/// convenience method for waiting until the snapshot satisfies a condition.
#[allow(async_fn_in_trait)]
pub trait SupervisorSnapshotReceiverExt {
    /// Checks the current snapshot and, if it does not match, waits for
    /// subsequent updates until `predicate` returns `true`.
    async fn wait_for_snapshot<P>(
        &mut self,
        predicate: P,
    ) -> Result<crate::SupervisorSnapshot, watch::error::RecvError>
    where
        P: FnMut(&crate::SupervisorSnapshot) -> bool;
}

impl SupervisorSnapshotReceiverExt for watch::Receiver<crate::SupervisorSnapshot> {
    async fn wait_for_snapshot<P>(
        &mut self,
        mut predicate: P,
    ) -> Result<crate::SupervisorSnapshot, watch::error::RecvError>
    where
        P: FnMut(&crate::SupervisorSnapshot) -> bool,
    {
        let current = self.borrow().clone();
        if predicate(&current) {
            return Ok(current);
        }

        loop {
            self.changed().await?;
            let snapshot = self.borrow().clone();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
        }
    }
}

// Keep this list mirrored by tokio_otp::prelude; its prelude test guards drift.
pub use crate::{
    AttachedChild, AttachedChildIdentity, AutoShutdown, BackoffPolicy, BoxError, ChildContext,
    ChildMembershipView, ChildResult, ChildSnapshot, ChildSpec, ChildStateView, ControlError,
    DynamicSupervisorBuilder, EventPathSegment, ExitStatusView, LifecycleEvent, LifecycleEventKind,
    LifecycleWatch, RestartIntensity, RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy,
    Strategy, Supervisor, SupervisorBuildError, SupervisorBuilder, SupervisorError,
    SupervisorEvent, SupervisorHandle, SupervisorSnapshot, SupervisorSpec, SupervisorStateView,
    SupervisorToken,
};
