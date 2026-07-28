use tokio_supervisor::{RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy};

use crate::{Graph, ReservedSupervisionTree, Runtime, RuntimeHandle, SupervisionTree};

/// Thin graph-in-one-scope convenience over [`SupervisionTree`].
///
/// This builder keeps the common case compact: every actor in one [`Graph`]
/// becomes a direct child of one ordered scope. Use [`SupervisionTree`]
/// directly for nested scopes, arbitrary task children, actor-owned scopes, or
/// per-actor policy overrides.
///
/// The ordered scope identity is reserved when the builder is created, so
/// [`handle`](Self::handle) is available before build. Converting with
/// [`into_tree`](Self::into_tree) returns the non-cloneable
/// [`ReservedSupervisionTree`] that owns that identity.
///
/// # Example
///
/// ```no_run
/// use tokio_otp::prelude::*;
///
/// struct Echo;
///
/// impl Actor for Echo {
///     type Msg = String;
///
///     async fn handle(&mut self, message: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
///         println!("{message}");
///         Ok(Continue)
///     }
/// }
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut graph = GraphBuilder::new();
/// let (echo_slot, echo) = graph.slot("echo");
/// graph.define(echo_slot, || Echo);
///
/// let runtime = Runtime::builder()
///     .graph(graph.build()?)
///     .default_restart(RestartPolicy::OnFailure)
///     .build()?;
/// let handle = runtime.spawn();
/// # drop((echo, handle));
/// # Ok(())
/// # }
/// ```
pub struct RuntimeBuilder {
    tree: ReservedSupervisionTree,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self {
            tree: SupervisionTree::new()
                .reserve()
                .expect("ordered scope root can be reserved"),
        }
    }
}

impl RuntimeBuilder {
    /// Creates an empty ordered graph runtime with standard defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the stable actor-aware handle reserved for this scope.
    pub fn handle(&self) -> RuntimeHandle {
        self.tree.handle()
    }

    /// Replaces the graph whose actors occupy this one ordered scope.
    #[must_use]
    pub fn graph(mut self, graph: Graph) -> Self {
        self.tree = self.tree.replace_graph(graph);
        self
    }

    /// Sets the scope's restart strategy.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.tree = self.tree.strategy(strategy);
        self
    }

    /// Sets the restart policy inherited by every graph actor.
    #[must_use]
    pub fn default_restart(mut self, restart: RestartPolicy) -> Self {
        self.tree = self.tree.default_restart(restart);
        self
    }

    /// Sets the shutdown policy inherited by every graph actor.
    #[must_use]
    pub fn default_shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.tree = self.tree.default_shutdown(shutdown);
        self
    }

    /// Sets the scope's default restart-intensity policy.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.tree = self.tree.restart_intensity(intensity);
        self
    }

    /// Returns the reserved tree declaration underlying this convenience.
    pub fn into_tree(self) -> ReservedSupervisionTree {
        self.tree
    }

    /// Validates the declaration and returns a ready-to-run runtime.
    pub fn build(self) -> Result<Runtime, tokio_supervisor::SupervisorBuildError> {
        self.tree.build()
    }
}

impl From<RuntimeBuilder> for ReservedSupervisionTree {
    fn from(builder: RuntimeBuilder) -> Self {
        builder.into_tree()
    }
}

/// Thin empty-dynamic-scope convenience over [`SupervisionTree`].
///
/// The scope identity is reserved immediately so a mount handle can be wired
/// into actor factories before the scope is built or inserted.
pub struct DynamicRuntimeBuilder {
    tree: ReservedSupervisionTree,
}

impl Default for DynamicRuntimeBuilder {
    fn default() -> Self {
        Self {
            tree: SupervisionTree::dynamic()
                .reserve()
                .expect("dynamic scope root can be reserved"),
        }
    }
}

impl DynamicRuntimeBuilder {
    /// Creates an empty dynamic runtime builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the stable actor-aware handle reserved for this scope.
    pub fn handle(&self) -> RuntimeHandle {
        self.tree.handle()
    }

    /// Sets the default restart-intensity policy.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.tree = self.tree.restart_intensity(intensity);
        self
    }

    /// Sets the restart policy inherited by runtime-added actors.
    #[must_use]
    pub fn default_restart(mut self, restart: RestartPolicy) -> Self {
        self.tree = self.tree.default_restart(restart);
        self
    }

    /// Sets the shutdown policy inherited by runtime-added actors.
    #[must_use]
    pub fn default_shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.tree = self.tree.default_shutdown(shutdown);
        self
    }

    /// Returns the reserved dynamic tree underlying this convenience.
    pub fn into_tree(self) -> ReservedSupervisionTree {
        self.tree
    }

    /// Validates the declaration and returns an empty dynamic runtime.
    pub fn build(self) -> Result<Runtime, tokio_supervisor::SupervisorBuildError> {
        self.tree.build()
    }
}

impl From<DynamicRuntimeBuilder> for ReservedSupervisionTree {
    fn from(builder: DynamicRuntimeBuilder) -> Self {
        builder.into_tree()
    }
}

impl std::fmt::Debug for RuntimeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RuntimeBuilder").field(&self.tree).finish()
    }
}

impl std::fmt::Debug for DynamicRuntimeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DynamicRuntimeBuilder")
            .field(&self.tree)
            .finish()
    }
}
