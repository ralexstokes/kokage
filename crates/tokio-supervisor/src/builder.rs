use std::{collections::HashSet, sync::Arc};

use crate::{
    child::{ChildDefinition, ChildSpec},
    error::SupervisorBuildError,
    handle::{StableSupervisorChannels, SupervisorHandle},
    restart::{RestartConfig, RestartPolicy},
    shutdown::ShutdownPolicy,
    strategy::Strategy,
    supervisor::{
        Supervisor, SupervisorConfig, refresh_declaration_for_config, reset_channels_for_config,
        stable_channels_for_config,
    },
};

/// Builder for constructing a [`Supervisor`] with validated configuration.
///
/// An ordered supervisor may be built with zero declared children, but its
/// membership remains immutable. Create one with [`Supervisor::ordered`].
///
/// # Example
///
/// ```no_run
/// use tokio_supervisor::{ChildSpec, Strategy, Supervisor};
///
/// let supervisor = Supervisor::ordered()
///     .strategy(Strategy::OneForOne)
///     .child(ChildSpec::task("worker", |ctx| async move {
///         ctx.shutdown_token().cancelled().await;
///         Ok(())
///     }))
///     .build()
///     .expect("valid config");
/// ```
pub struct OrderedSupervisorBuilder {
    strategy: Strategy,
    restart_intensity: RestartConfig,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
    children: Vec<Arc<ChildDefinition>>,
    channels: Option<Arc<StableSupervisorChannels>>,
}

/// Builder for a dynamic supervisor whose membership is written at runtime.
///
/// Dynamic supervisors start empty, use [`Strategy::OneForOne`], and start
/// and stop children concurrently. Children can be added and removed through
/// the resulting supervisor's handle. Create one with [`Supervisor::dynamic`].
pub struct DynamicSupervisorBuilder {
    restart_intensity: RestartConfig,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
    channels: Option<Arc<StableSupervisorChannels>>,
}

/// The immutable membership and ordering model of a supervisor scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ScopeKind {
    /// A declared sequence with readiness-gated startup and reverse-order
    /// teardown. Runtime membership operations are unsupported.
    #[default]
    Ordered,
    /// A runtime-written membership set with concurrent startup and teardown.
    Dynamic,
}

const DEFAULT_CONTROL_CHANNEL_CAPACITY: usize = 64;

impl OrderedSupervisorBuilder {
    pub(crate) fn new() -> Self {
        let mut builder = Self {
            strategy: Strategy::default(),
            restart_intensity: RestartConfig::default(),
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            children: Vec::new(),
            channels: None,
        };
        let config = builder.config();
        builder.channels = Some(stable_channels_for_config(&config));
        builder
    }

    fn config(&self) -> SupervisorConfig {
        SupervisorConfig {
            kind: ScopeKind::Ordered,
            strategy: self.strategy,
            restart_intensity: self.restart_intensity,
            default_restart: self.default_restart,
            default_shutdown: self.default_shutdown,
            children: self.children.clone(),
            control_channel_capacity: DEFAULT_CONTROL_CHANNEL_CAPACITY,
        }
    }

    fn channels(&self) -> &Arc<StableSupervisorChannels> {
        self.channels
            .as_ref()
            .expect("live supervisor builder owns channels")
    }

    fn refresh_declaration(&self) {
        refresh_declaration_for_config(&self.config(), self.channels());
    }

    /// Returns the stable handle reserved for this scope.
    pub fn handle(&self) -> SupervisorHandle {
        self.channels().handle()
    }

    /// Projects `ids` as this scope's declared children in its pre-spawn
    /// snapshot.
    ///
    /// Not part of the public contract: this exists so `tokio-otp` can project
    /// the membership its own higher-level builders will lower to, and is
    /// superseded by the real declaration once the scope is built.
    #[doc(hidden)]
    pub fn project_declared_children(&self, ids: Vec<String>) {
        self.channels().project_declared_children(ids);
    }

    /// Sets the restart strategy. See [`Strategy`] for options.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self.refresh_declaration();
        self
    }

    /// Sets the default restart intensity for all children that do not have a
    /// per-child override.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartConfig) -> Self {
        self.restart_intensity = intensity;
        self
    }

    /// Sets the restart policy inherited by declared children that do not
    /// carry an explicit override, including nested-supervisor edges.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.default_restart = restart;
        self
    }

    /// Sets the shutdown policy inherited by declared children that do not
    /// carry an explicit override, including nested-supervisor edges.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.default_shutdown = shutdown;
        self
    }

    /// Appends a child to the supervisor. Declaration order determines
    /// sequential startup and group-restart order.
    #[must_use]
    pub fn child(mut self, child: ChildSpec) -> Self {
        self.children.push(child.inner);
        self.refresh_declaration();
        self
    }

    /// Validates the configuration and returns a ready-to-run [`Supervisor`].
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorBuildError`] if:
    /// - Two children share the same id.
    /// - Any restart intensity or backoff configuration is invalid.
    pub fn build(mut self) -> Result<Supervisor, SupervisorBuildError> {
        self.restart_intensity.validate()?;
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
            if !ids.insert(child.id.as_str()) {
                return Err(SupervisorBuildError::DuplicateChildId(child.id.clone()));
            }
        }

        for child in &mut self.children {
            ChildDefinition::make_mut_preserving_supervisor_identity(child)
                .apply_defaults(self.default_restart, self.default_shutdown);
        }

        let config = self.config();
        let channels = self
            .channels
            .take()
            .expect("valid supervisor builder owns channels");
        reset_channels_for_config(&config, &channels);
        Ok(Supervisor::with_channels(config, channels))
    }
}

impl DynamicSupervisorBuilder {
    pub(crate) fn new() -> Self {
        let mut builder = Self {
            restart_intensity: RestartConfig::default(),
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            channels: None,
        };
        let config = builder.config();
        builder.channels = Some(stable_channels_for_config(&config));
        builder
    }

    fn config(&self) -> SupervisorConfig {
        SupervisorConfig {
            kind: ScopeKind::Dynamic,
            strategy: Strategy::OneForOne,
            restart_intensity: self.restart_intensity,
            default_restart: self.default_restart,
            default_shutdown: self.default_shutdown,
            children: Vec::new(),
            control_channel_capacity: DEFAULT_CONTROL_CHANNEL_CAPACITY,
        }
    }

    /// Returns the stable handle reserved for this scope.
    ///
    /// A dynamic scope declares no children, so nothing a mutator can change
    /// is visible in the pre-build view; the configuration is applied once, at
    /// [`build`](Self::build).
    pub fn handle(&self) -> SupervisorHandle {
        self.channels
            .as_ref()
            .expect("live dynamic supervisor builder owns channels")
            .handle()
    }

    /// Sets the default restart intensity for dynamically added children that
    /// do not carry a per-child override.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartConfig) -> Self {
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

    /// Validates the configuration and returns an empty dynamic supervisor.
    pub fn build(mut self) -> Result<Supervisor, SupervisorBuildError> {
        self.restart_intensity.validate()?;
        let config = self.config();
        let channels = self
            .channels
            .take()
            .expect("valid dynamic supervisor builder owns channels");
        reset_channels_for_config(&config, &channels);
        Ok(Supervisor::with_channels(config, channels))
    }
}

impl Drop for OrderedSupervisorBuilder {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::child::ChildKind;

    #[test]
    fn ordered_defaults_apply_without_overriding_child_policies() {
        let inherited_shutdown = ShutdownPolicy::cooperative(Duration::from_millis(20));
        let explicit_shutdown = ShutdownPolicy::cooperative(Duration::from_secs(2));
        let supervisor = Supervisor::ordered()
            .restart(RestartPolicy::Always)
            .shutdown(inherited_shutdown)
            .child(ChildSpec::task("inherited", |_| async { Ok(()) }))
            .child(
                ChildSpec::task("explicit", |_| async { Ok(()) })
                    .restart(RestartPolicy::Never)
                    .shutdown(explicit_shutdown),
            )
            .build()
            .expect("valid ordered supervisor");

        let inherited = &supervisor.config.children[0];
        assert_eq!(inherited.restart, RestartPolicy::Always);
        assert_eq!(inherited.shutdown_policy, inherited_shutdown);
        let explicit = &supervisor.config.children[1];
        assert_eq!(explicit.restart, RestartPolicy::Never);
        assert_eq!(explicit.shutdown_policy, explicit_shutdown);
    }

    #[test]
    fn cloning_a_nested_child_spec_reserves_a_fresh_supervisor_identity() {
        let nested = Supervisor::ordered()
            .build()
            .expect("valid nested supervisor");
        let original = ChildSpec::supervisor("nested", nested);
        let cloned = original.clone();

        let ChildKind::Supervisor(original) = &original.inner.kind else {
            panic!("nested child keeps its kind");
        };
        let ChildKind::Supervisor(cloned) = &cloned.inner.kind else {
            panic!("cloned nested child keeps its kind");
        };
        assert!(!Arc::ptr_eq(&original.channels, &cloned.channels));
    }

    #[test]
    fn ordered_defaults_apply_to_nested_children_without_replacing_their_identity() {
        let inherited_shutdown = ShutdownPolicy::cooperative(Duration::from_millis(20));
        let nested = Supervisor::ordered()
            .build()
            .expect("valid nested supervisor");
        let nested_channels = Arc::clone(&nested.channels);

        let supervisor = Supervisor::ordered()
            .restart(RestartPolicy::Always)
            .shutdown(inherited_shutdown)
            .child(ChildSpec::supervisor("nested", nested))
            .build()
            .expect("valid parent supervisor");

        let definition = &supervisor.config.children[0];
        assert_eq!(definition.restart, RestartPolicy::Always);
        assert_eq!(definition.shutdown_policy, inherited_shutdown);
        let ChildKind::Supervisor(nested) = &definition.kind else {
            panic!("nested child keeps its kind");
        };
        assert!(Arc::ptr_eq(&nested.channels, &nested_channels));
    }
}
