//! Day-one construction and control imports for `tokio-supervisor` consumers.
//!
//! ```
//! use tokio_supervisor::prelude::*;
//! ```

pub use crate::{
    BoxError, ChildContext, ChildResult, ChildSpec, ControlError, DynamicSupervisorBuilder,
    OrderedSupervisorBuilder, RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy,
    Supervisor, SupervisorBuildError, SupervisorError, SupervisorHandle,
};
