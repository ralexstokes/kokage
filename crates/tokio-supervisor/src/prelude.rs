//! Day-one construction and control imports for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

pub use crate::{
    BoxError, ChildContext, ChildResult, ChildSpec, ControlError, DynamicSupervisorBuilder,
    RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy, Supervisor, SupervisorBuildError,
    SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSpec,
};
