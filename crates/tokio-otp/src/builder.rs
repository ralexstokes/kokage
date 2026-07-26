use std::collections::HashMap;

use crate::{ActorChild, ActorRef, Graph, RunnableActorFactory, RuntimeHandle, SupervisionTree};
use std::sync::Arc;
use tokio_supervisor::{
    ChildSpec, DynamicSupervisorBuilder, RestartIntensity, RestartPolicy, ShutdownPolicy, Strategy,
    SupervisorBuilder,
};

use crate::runtime::{ActorOverrides, ActorRuntimeState, Runtime};

/// One-stop builder for the common supervised-actor setup.
///
/// Wires an actor [`Graph`] into a [`Runtime`] where every actor runs as its
/// own supervised child. Nested scopes added with [`subtree`](Self::subtree)
/// preserve the same actor-aware runtime behavior recursively. These ordered
/// scopes have static membership. Use [`DynamicRuntimeBuilder`] for a
/// graph-less runtime whose membership grows through
/// [`RuntimeHandle::add_actor`](crate::RuntimeHandle::add_actor). Create the
/// two builders with [`Runtime::builder`] and [`Runtime::dynamic`],
/// respectively.
///
/// Per-actor policies can be overridden with [`actor_restart`](Self::actor_restart),
/// [`actor_restart_intensity`](Self::actor_restart_intensity), and
/// [`actor_shutdown`](Self::actor_shutdown). Arbitrary non-actor children can
/// be mixed into the same supervisor with [`child`](Self::child).
///
/// # Example
///
/// ```no_run
/// use tokio_otp::prelude::*;
///
/// #[derive(Clone)]
/// struct Echo;
///
/// impl Actor for Echo {
///     type Msg = String;
///
///     async fn handle(&mut self, message: String, _ctx: &mut ActorContext<String>) -> ActorResult {
///         println!("{message}");
///         Ok(tokio_otp::prelude::Continue)
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut graph = GraphBuilder::new();
/// let echo = graph.add(|| Echo);
///
/// let runtime = Runtime::builder()
///     .graph(graph.build()?)
///     .strategy(Strategy::OneForOne)
///     .restart(RestartPolicy::OnFailure)
///     .build()?;
/// let handle = runtime.spawn();
///
/// echo.send("hello".to_owned()).await?;
/// handle.shutdown_and_wait().await?;
/// # Ok(())
/// # }
/// ```
///
pub struct RuntimeBuilder {
    graph: Option<Graph>,
    subtrees: Vec<(String, SupervisionTree)>,
    children: Vec<ChildSpec>,
    strategy: Strategy,
    restart: RestartPolicy,
    shutdown: ShutdownPolicy,
    restart_intensity: Option<RestartIntensity>,
    actor_overrides: HashMap<String, ActorOverrides>,
    /// Always `Some` while the builder is alive; `Option` only so that
    /// `strategy` can move the reserved builder through `SupervisorBuilder`'s
    /// by-value setter without minting a throwaway identity to swap in.
    supervisor: Option<SupervisorBuilder>,
    actors: Arc<ActorRuntimeState>,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        let actors = Arc::new(ActorRuntimeState::new(
            RunnableActorFactory::new(),
            RestartPolicy::default(),
            ShutdownPolicy::default(),
        ));
        Self {
            graph: None,
            subtrees: Vec::new(),
            children: Vec::new(),
            strategy: Strategy::default(),
            restart: RestartPolicy::default(),
            shutdown: ShutdownPolicy::default(),
            restart_intensity: None,
            actor_overrides: HashMap::new(),
            supervisor: Some(SupervisorBuilder::new()),
            actors,
        }
    }
}

impl RuntimeBuilder {
    /// Creates a builder with default settings: [`OneForOne`](Strategy::OneForOne)
    /// strategy, [`OnFailure`](RestartPolicy::OnFailure) restart, default shutdown
    /// policy, no graph, no subtrees, and no non-actor children.
    pub fn new() -> Self {
        Self::default()
    }

    fn supervisor(&self) -> &SupervisorBuilder {
        self.supervisor
            .as_ref()
            .expect("live runtime builder owns its reserved scope builder")
    }

    /// Returns the stable actor-aware handle reserved for this scope.
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle::new(self.supervisor().handle(), Arc::clone(&self.actors))
    }

    fn refresh_runtime_state(&self) {
        self.actors.configure(
            self.graph
                .as_ref()
                .map_or_else(RunnableActorFactory::new, Graph::dynamic_factory),
            self.restart,
            self.shutdown,
        );
    }

    fn refresh_snapshot(&self) {
        let mut ids = self
            .subtrees
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ids.extend(self.children.iter().map(|child| child.id().to_owned()));
        if let Some(graph) = &self.graph {
            ids.extend(graph.actors().iter().map(|actor| actor.label().to_owned()));
        }
        self.supervisor().project_declared_children(ids);
    }

    /// Sets the actor graph to run. If omitted, the runtime starts empty.
    #[must_use]
    pub fn graph(mut self, graph: Graph) -> Self {
        self.graph = Some(graph);
        self.refresh_runtime_state();
        self.refresh_snapshot();
        self
    }

    /// Adds an actor-aware nested runtime subtree.
    ///
    /// Subtrees retain their graphs' actor metadata, so
    /// [`RuntimeHandle::actor_stats`](crate::RuntimeHandle::actor_stats)
    /// recursively includes their actors and
    /// [`RuntimeHandle::subtree`](crate::RuntimeHandle::subtree) can create a
    /// scoped actor-aware handle. Subtrees are inserted before this builder's
    /// graph actors, in declaration order, which also determines sequential
    /// startup order.
    #[must_use]
    pub fn subtree(mut self, id: impl Into<String>, subtree: impl Into<SupervisionTree>) -> Self {
        self.subtrees.push((id.into(), subtree.into()));
        self.refresh_snapshot();
        self
    }

    /// Adds an arbitrary non-actor child to the runtime's supervisor.
    ///
    /// Runtime subtrees are inserted first, followed by these children and
    /// then the graph actors. Declaration order within each group is
    /// preserved and determines sequential startup order.
    #[must_use]
    pub fn child(mut self, child: ChildSpec) -> Self {
        self.children.push(child);
        self.refresh_snapshot();
        self
    }

    /// Sets the supervisor restart strategy. See [`Strategy`] for options.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        let supervisor = self
            .supervisor
            .take()
            .expect("live runtime builder owns its reserved scope builder");
        self.supervisor = Some(supervisor.strategy(strategy));
        self.refresh_snapshot();
        self
    }

    /// Sets the restart policy applied to every actor child.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self.refresh_runtime_state();
        self
    }

    /// Sets the shutdown policy applied to every actor child.
    ///
    /// This grace is the actor's only runtime-owned shutdown clock. On expiry
    /// the supervisor asks the actor wrapper to abort its inner task and finish
    /// accounting, then hard-aborts the wrapper after a short fixed beat.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = shutdown;
        self.refresh_runtime_state();
        self
    }

    /// Sets the supervisor's default restart intensity.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = Some(intensity);
        self
    }

    /// Overrides the restart policy for the actor identified by this typed ref.
    ///
    /// Graph-declared actors remain registered after a terminal exit, including
    /// actors configured with [`RestartPolicy::Never`]. Automatic terminal
    /// removal is scoped to actors added at runtime with
    /// [`DynamicActorOptions`](crate::DynamicActorOptions).
    #[must_use]
    pub fn actor_restart<M>(mut self, actor: &ActorRef<M>, restart: RestartPolicy) -> Self {
        self.actor_overrides
            .entry(actor.id().to_owned())
            .or_default()
            .restart = Some(restart);
        self
    }

    /// Overrides restart intensity for the actor identified by this typed ref.
    #[must_use]
    pub fn actor_restart_intensity<M>(
        mut self,
        actor: &ActorRef<M>,
        intensity: RestartIntensity,
    ) -> Self {
        self.actor_overrides
            .entry(actor.id().to_owned())
            .or_default()
            .restart_intensity = Some(intensity);
        self
    }

    /// Overrides the shutdown policy for the actor identified by this typed
    /// ref.
    #[must_use]
    pub fn actor_shutdown<M>(mut self, actor: &ActorRef<M>, shutdown: ShutdownPolicy) -> Self {
        self.actor_overrides
            .entry(actor.id().to_owned())
            .or_default()
            .shutdown = Some(shutdown);
        self
    }

    /// Converts this builder into its equivalent inspectable declaration.
    pub fn into_tree(self) -> SupervisionTree {
        let RuntimeBuilder {
            graph,
            subtrees,
            children,
            strategy,
            restart,
            shutdown,
            restart_intensity,
            actor_overrides,
            supervisor,
            actors,
        } = self;
        let mut tree = SupervisionTree::new()
            .strategy(strategy)
            .default_restart(restart)
            .default_shutdown(shutdown);
        if let Some(intensity) = restart_intensity {
            tree = tree.restart_intensity(intensity);
        }
        if let Some(graph) = &graph {
            tree = tree.dynamic_defaults(graph);
        }
        for (id, subtree) in subtrees {
            tree = tree.subtree(id, subtree);
        }
        for child in children {
            tree = tree.task(child);
        }
        if let Some(graph) = &graph {
            for actor in graph.actors() {
                let overrides = actor_overrides
                    .get(actor.label())
                    .copied()
                    .unwrap_or_default();
                let mut child = ActorChild::new(actor.clone());
                if let Some(restart) = overrides.restart {
                    child = child.restart(restart);
                }
                if let Some(shutdown) = overrides.shutdown {
                    child = child.shutdown(shutdown);
                }
                if let Some(intensity) = overrides.restart_intensity {
                    child = child.restart_intensity(intensity);
                }
                tree = tree.actor(child);
            }
        }
        tree.with_ordered_builder(
            supervisor.expect("live runtime builder owns its reserved scope builder"),
            actors,
        )
    }

    /// Validates the configuration and returns a ready-to-run [`Runtime`].
    pub fn build(self) -> Result<Runtime, tokio_supervisor::SupervisorBuildError> {
        self.into_tree().build()
    }
}

impl From<RuntimeBuilder> for SupervisionTree {
    fn from(builder: RuntimeBuilder) -> Self {
        builder.into_tree()
    }
}

/// Builder for a graph-less actor runtime with dynamic membership.
pub struct DynamicRuntimeBuilder {
    restart_intensity: Option<RestartIntensity>,
    restart: RestartPolicy,
    shutdown: ShutdownPolicy,
    supervisor: DynamicSupervisorBuilder,
    actors: Arc<ActorRuntimeState>,
}

impl Default for DynamicRuntimeBuilder {
    fn default() -> Self {
        let actors = Arc::new(ActorRuntimeState::new(
            RunnableActorFactory::new(),
            RestartPolicy::default(),
            ShutdownPolicy::default(),
        ));
        Self {
            restart_intensity: None,
            restart: RestartPolicy::default(),
            shutdown: ShutdownPolicy::default(),
            supervisor: DynamicSupervisorBuilder::new(),
            actors,
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
        RuntimeHandle::new(self.supervisor.handle(), Arc::clone(&self.actors))
    }

    fn refresh_runtime_state(&self) {
        self.actors
            .configure(RunnableActorFactory::new(), self.restart, self.shutdown);
    }

    /// Sets the default supervisor restart intensity.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = Some(intensity);
        self
    }

    /// Sets the restart policy inherited by runtime-added actors whose
    /// [`DynamicActorOptions`](crate::DynamicActorOptions) do not override it.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self.refresh_runtime_state();
        self
    }

    /// Sets the shutdown policy inherited by runtime-added actors whose
    /// [`DynamicActorOptions`](crate::DynamicActorOptions) do not override it.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = shutdown;
        self.refresh_runtime_state();
        self
    }

    /// Converts this builder into an inspectable dynamic tree leaf.
    pub fn into_tree(self) -> SupervisionTree {
        let DynamicRuntimeBuilder {
            restart_intensity,
            restart,
            shutdown,
            supervisor,
            actors,
        } = self;
        let mut tree = SupervisionTree::dynamic()
            .default_restart(restart)
            .default_shutdown(shutdown);
        if let Some(intensity) = restart_intensity {
            tree = tree.restart_intensity(intensity);
        }
        tree.with_dynamic_builder(supervisor, actors)
    }

    /// Validates the configuration and returns an empty dynamic runtime.
    pub fn build(self) -> Result<Runtime, tokio_supervisor::SupervisorBuildError> {
        self.into_tree().build()
    }
}

impl From<DynamicRuntimeBuilder> for SupervisionTree {
    fn from(builder: DynamicRuntimeBuilder) -> Self {
        builder.into_tree()
    }
}

impl std::fmt::Debug for RuntimeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeBuilder")
            .field("strategy", &self.strategy)
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .field("restart_intensity", &self.restart_intensity)
            .field("actor_overrides", &self.actor_overrides.len())
            .field(
                "subtrees",
                &self.subtrees.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            )
            .field("children", &self.children.len())
            .finish_non_exhaustive()
    }
}
