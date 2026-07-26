//! Common imports and extension traits for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

use tokio::sync::watch;

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
    AttachedChild, AttachedChildIdentity, BackoffPolicy, BoxError, ChildContext,
    ChildMembershipView, ChildResult, ChildSnapshot, ChildSpec, ChildStateView, CompletionGuard,
    CompletionOutcome, ControlError, ControlOperation, DynamicSupervisorBuilder, ExitStatusView,
    LifecycleEvent, LifecycleEventKind, LifecyclePathSegment, LifecycleWatch,
    RecursiveLifecycleEvent, RecursiveLifecycleEventKind, RecursiveLifecycleWatch,
    RestartIntensity, RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, Supervisor,
    SupervisorBuildError, SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSnapshot,
    SupervisorSpec, SupervisorStateView, SupervisorToken,
};
