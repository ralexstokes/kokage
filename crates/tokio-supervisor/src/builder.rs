use std::{collections::HashSet, sync::Arc};

use crate::{
    child::{ChildDefinition, ChildSpec, SupervisorSpec},
    error::SupervisorBuildError,
    handle::{StableSupervisorChannels, SupervisorHandle},
    restart::{RestartIntensity, RestartPolicy},
    shutdown::{AutoShutdown, ShutdownPolicy},
    strategy::Strategy,
    supervisor::{
        Supervisor, SupervisorConfig, reset_channels_for_config, stable_channels_for_config,
    },
};

/// Builder for constructing a [`Supervisor`] with validated configuration.
///
/// An ordered supervisor may be built with zero declared children, but its
/// membership remains immutable. Use [`DynamicSupervisorBuilder`] for a scope
/// populated at runtime.
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
    restart_intensity: RestartIntensity,
    auto_shutdown: AutoShutdown,
    children: Vec<Arc<ChildDefinition>>,
    control_channel_capacity: usize,
    event_channel_capacity: usize,
    channels: Option<Arc<StableSupervisorChannels>>,
}

/// Builder for a dynamic supervisor whose membership is written at runtime.
///
/// Dynamic supervisors start empty, use [`Strategy::OneForOne`], and start
/// and stop children concurrently. Children can be added and removed through
/// the resulting supervisor's handle.
pub struct DynamicSupervisorBuilder {
    restart_intensity: RestartIntensity,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
    control_channel_capacity: usize,
    event_channel_capacity: usize,
    channels: Option<Arc<StableSupervisorChannels>>,
}

/// The immutable membership and ordering model of a supervisor scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ScopeKind {
    /// A declared sequence with readiness-gated startup and reverse-order
    /// teardown. Runtime membership operations are unsupported.
    #[default]
    Ordered,
    /// A runtime-written membership set with concurrent startup and teardown.
    Dynamic,
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
        let mut builder = Self {
            strategy: Strategy::default(),
            restart_intensity: RestartIntensity::default(),
            auto_shutdown: AutoShutdown::default(),
            children: Vec::new(),
            control_channel_capacity: DEFAULT_CONTROL_CHANNEL_CAPACITY,
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
            channels: None,
        };
        let config = builder.config();
        builder.channels = Some(stable_channels_for_config(&config, false));
        builder
    }

    fn config(&self) -> SupervisorConfig {
        SupervisorConfig {
            kind: ScopeKind::Ordered,
            strategy: self.strategy,
            restart_intensity: self.restart_intensity,
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            auto_shutdown: self.auto_shutdown,
            children: self.children.clone(),
            control_channel_capacity: self.control_channel_capacity,
            event_channel_capacity: self.event_channel_capacity,
        }
    }

    fn refresh_channels(&self) {
        reset_channels_for_config(
            &self.config(),
            self.channels
                .as_deref()
                .expect("live supervisor builder owns channels"),
        );
    }

    /// Returns the stable handle reserved for this scope.
    pub fn handle(&self) -> SupervisorHandle {
        self.channels
            .as_ref()
            .expect("live supervisor builder owns channels")
            .handle()
    }

    #[doc(hidden)]
    pub fn project_declared_children(&self, ids: Vec<String>) {
        self.channels
            .as_ref()
            .expect("live supervisor builder owns channels")
            .project_declared_children(ids);
    }

    /// Sets the restart strategy. See [`Strategy`] for options.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self.refresh_channels();
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
        self.refresh_channels();
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
        self.refresh_channels();
        self
    }

    /// Sets the bounded capacity of the internal control channel used for
    /// runtime commands (add/remove child). Defaults to 64.
    #[must_use]
    pub fn control_channel_capacity(mut self, capacity: usize) -> Self {
        self.control_channel_capacity = capacity;
        if capacity != 0 {
            self.refresh_channels();
        }
        self
    }

    /// Sets the bounded capacity of the event broadcast channel. Slow
    /// subscribers that fall behind this limit will receive a
    /// [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged)
    /// error. Defaults to 256.
    #[must_use]
    pub fn event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        if capacity != 0 {
            self.refresh_channels();
        }
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
    pub fn build(mut self) -> Result<Supervisor, SupervisorBuildError> {
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

        let config = self.config();
        self.refresh_channels();
        Ok(Supervisor::with_channels(
            config,
            self.channels
                .take()
                .expect("valid supervisor builder owns channels"),
        ))
    }
}

impl Default for DynamicSupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicSupervisorBuilder {
    /// Creates an empty dynamic supervisor with the default restart intensity
    /// and channel capacities.
    pub fn new() -> Self {
        let mut builder = Self {
            restart_intensity: RestartIntensity::default(),
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            control_channel_capacity: DEFAULT_CONTROL_CHANNEL_CAPACITY,
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
            channels: None,
        };
        let config = builder.config();
        builder.channels = Some(stable_channels_for_config(&config, false));
        builder
    }

    fn config(&self) -> SupervisorConfig {
        SupervisorConfig {
            kind: ScopeKind::Dynamic,
            strategy: Strategy::OneForOne,
            restart_intensity: self.restart_intensity,
            default_restart: self.default_restart,
            default_shutdown: self.default_shutdown,
            auto_shutdown: AutoShutdown::Never,
            children: Vec::new(),
            control_channel_capacity: self.control_channel_capacity,
            event_channel_capacity: self.event_channel_capacity,
        }
    }

    fn refresh_channels(&self) {
        reset_channels_for_config(
            &self.config(),
            self.channels
                .as_deref()
                .expect("live dynamic supervisor builder owns channels"),
        );
    }

    /// Returns the stable handle reserved for this scope.
    pub fn handle(&self) -> SupervisorHandle {
        self.channels
            .as_ref()
            .expect("live dynamic supervisor builder owns channels")
            .handle()
    }

    /// Sets the default restart intensity for dynamically added children that
    /// do not carry a per-child override.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = intensity;
        self
    }

    /// Sets the restart policy inherited by dynamically added task and
    /// supervisor specs that do not carry an explicit override.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.default_restart = restart;
        self
    }

    /// Sets the shutdown policy inherited by dynamically added task and
    /// supervisor specs that do not carry an explicit override.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.default_shutdown = shutdown;
        self
    }

    /// Sets the bounded capacity of the internal control channel.
    #[must_use]
    pub fn control_channel_capacity(mut self, capacity: usize) -> Self {
        self.control_channel_capacity = capacity;
        if capacity != 0 {
            self.refresh_channels();
        }
        self
    }

    /// Sets the bounded capacity of the event broadcast channel.
    #[must_use]
    pub fn event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        if capacity != 0 {
            self.refresh_channels();
        }
        self
    }

    /// Validates the configuration and returns an empty dynamic supervisor.
    pub fn build(mut self) -> Result<Supervisor, SupervisorBuildError> {
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

        let config = self.config();
        self.refresh_channels();
        Ok(Supervisor::with_channels(
            config,
            self.channels
                .take()
                .expect("valid dynamic supervisor builder owns channels"),
        ))
    }
}

impl Drop for SupervisorBuilder {
    fn drop(&mut self) {
        if let Some(channels) = self.channels.take() {
            channels.terminal();
        }
    }
}

impl Drop for DynamicSupervisorBuilder {
    fn drop(&mut self) {
        if let Some(channels) = self.channels.take() {
            channels.terminal();
        }
    }
}
