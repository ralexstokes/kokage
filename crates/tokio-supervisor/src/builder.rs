use std::{collections::HashSet, sync::Arc};

use crate::{
    child::{ChildDefinition, ChildSpec, SupervisorSpec},
    error::SupervisorBuildError,
    restart::RestartIntensity,
    shutdown::AutoShutdown,
    strategy::Strategy,
    supervisor::{Supervisor, SupervisorConfig},
};

/// Builder for constructing a [`Supervisor`] with validated configuration.
///
/// A supervisor may be built with zero children and populated dynamically.
///
/// # Example
///
/// ```no_run
/// use tokio_supervisor::{ChildSpec, SupervisorBuilder, Strategy};
///
/// let supervisor = SupervisorBuilder::new()
///     .strategy(Strategy::OneForOne)
///     .child(ChildSpec::new("worker", |ctx| async move {
///         ctx.shutdown_token().cancelled().await;
///         Ok(())
///     }))
///     .build()
///     .expect("valid config");
/// ```
pub struct SupervisorBuilder {
    strategy: Strategy,
    start_mode: StartMode,
    restart_intensity: RestartIntensity,
    auto_shutdown: AutoShutdown,
    children: Vec<Arc<ChildDefinition>>,
    control_channel_capacity: usize,
    event_channel_capacity: usize,
}

/// How a supervisor starts a set of children.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum StartMode {
    /// Spawn every child without waiting for readiness. This is the default.
    #[default]
    Concurrent,
    /// Start children in declaration order, waiting for each child to report
    /// readiness before spawning the next. Children added while a sequence is
    /// in progress are inserted immediately and queued behind its existing
    /// members; the add operation does not wait for readiness. Shutdown and
    /// control commands remain responsive while the sequence is gated. There
    /// is no built-in readiness timeout; children that use explicit readiness
    /// should arrange their own initialization timeout.
    Sequential,
}

const DEFAULT_CONTROL_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 256;

impl Default for SupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisorBuilder {
    /// Creates a new builder with default settings: [`OneForOne`](Strategy::OneForOne)
    /// strategy, default [`RestartIntensity`], and no children.
    pub fn new() -> Self {
        Self {
            strategy: Strategy::default(),
            start_mode: StartMode::default(),
            restart_intensity: RestartIntensity::default(),
            auto_shutdown: AutoShutdown::default(),
            children: Vec::new(),
            control_channel_capacity: DEFAULT_CONTROL_CHANNEL_CAPACITY,
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
        }
    }

    /// Sets the restart strategy. See [`Strategy`] for options.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets how children are started initially and during group restarts.
    #[must_use]
    pub fn start_mode(mut self, start_mode: StartMode) -> Self {
        self.start_mode = start_mode;
        self
    }

    /// Sets the default restart intensity for all children that do not have a
    /// per-child override.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = intensity;
        self
    }

    /// Sets when clean exits from significant children stop the supervisor.
    #[must_use]
    pub fn auto_shutdown(mut self, auto_shutdown: AutoShutdown) -> Self {
        self.auto_shutdown = auto_shutdown;
        self
    }

    /// Appends a child to the supervisor. Declaration order determines
    /// sequential startup and group-restart order.
    #[must_use]
    pub fn child(mut self, child: ChildSpec) -> Self {
        self.children.push(child.inner);
        self
    }

    /// Appends a nested supervisor child.
    ///
    /// Pass a [`Supervisor`](crate::Supervisor) for the standard policies, or
    /// a [`SupervisorSpec`] to customize its restart, shutdown, or restart
    /// intensity policy.
    #[must_use]
    pub fn supervisor(
        mut self,
        id: impl Into<String>,
        supervisor: impl Into<SupervisorSpec>,
    ) -> Self {
        self.children.push(Arc::new(ChildDefinition::supervisor(
            id.into(),
            supervisor.into(),
        )));
        self
    }

    /// Sets the bounded capacity of the internal control channel used for
    /// runtime commands (add/remove child). Defaults to 64.
    #[must_use]
    pub fn control_channel_capacity(mut self, capacity: usize) -> Self {
        self.control_channel_capacity = capacity;
        self
    }

    /// Sets the bounded capacity of the event broadcast channel. Slow
    /// subscribers that fall behind this limit will receive a
    /// [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
    /// error. Defaults to 256.
    #[must_use]
    pub fn event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    /// Validates the configuration and returns a ready-to-run [`Supervisor`].
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorBuildError`] if:
    /// - Two children share the same id.
    /// - Any channel capacity is zero.
    /// - Any restart intensity or backoff configuration is invalid.
    /// - A significant child uses [`RestartPolicy::Always`](crate::RestartPolicy::Always).
    /// - A child is significant while automatic shutdown is disabled.
    pub fn build(self) -> Result<Supervisor, SupervisorBuildError> {
        self.restart_intensity.validate()?;
        if self.control_channel_capacity == 0 {
            return Err(SupervisorBuildError::InvalidConfig(
                "control channel capacity must be non-zero",
            ));
        }
        if self.event_channel_capacity == 0 {
            return Err(SupervisorBuildError::InvalidConfig(
                "event channel capacity must be non-zero",
            ));
        }

        let mut ids = HashSet::new();
        for child in &self.children {
            if child.id.is_empty() {
                return Err(SupervisorBuildError::InvalidConfig(
                    "child id must not be empty",
                ));
            }
            if let Some(restart_intensity) = child.restart_intensity {
                restart_intensity.validate()?;
            }
            if child.significant && matches!(child.restart, crate::RestartPolicy::Always) {
                return Err(SupervisorBuildError::InvalidConfig(
                    "significant children cannot use RestartPolicy::Always",
                ));
            }
            if child.significant && matches!(self.auto_shutdown, AutoShutdown::Never) {
                return Err(SupervisorBuildError::InvalidConfig(
                    "significant children require automatic shutdown",
                ));
            }
            if !ids.insert(child.id.as_str()) {
                return Err(SupervisorBuildError::DuplicateChildId(child.id.clone()));
            }
        }

        Ok(Supervisor::new(SupervisorConfig {
            strategy: self.strategy,
            start_mode: self.start_mode,
            restart_intensity: self.restart_intensity,
            auto_shutdown: self.auto_shutdown,
            children: self.children,
            control_channel_capacity: self.control_channel_capacity,
            event_channel_capacity: self.event_channel_capacity,
        }))
    }
}
