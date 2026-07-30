//! Day-one construction and control imports for `kokage-supervisor` consumers.
//!
//! ```
//! use kokage_supervisor::prelude::*;
//! ```

pub use crate::{
    BoxError, ChildContext, ChildResult, ChildSpec, ControlError, DynamicSupervisorBuilder,
    DynamicSupervisorHandle, OrderedSupervisorBuilder, Restart, RestartMode, RunningSupervisor,
    Shutdown, ShutdownMode, Strategy, Supervisor, SupervisorBuildError, SupervisorError,
    SupervisorHandle,
};
