use std::{collections::HashMap, sync::Arc};

use crate::{ActorRef, Graph, RunnableActorFactory};
use tokio_supervisor::{
    ChildSpec, RestartIntensity, RestartPolicy, ShutdownPolicy, StartMode, Strategy,
    SupervisorBuilder, SupervisorSpec,
};

use crate::runtime::{
    ActorOverrides, ActorRuntimeState, Runtime, RuntimeAttachment, actor_children,
};

/// One-stop builder for the common supervised-actor setup.
///
/// Wires an actor [`Graph`] into a [`Runtime`] where every actor runs as its
/// own supervised child. Nested builders added with [`subtree`](Self::subtree)
/// preserve the same actor-aware runtime behavior recursively. It can also
/// build a graph-less runtime that starts empty and grows through
/// [`RuntimeHandle::add_actor`](crate::RuntimeHandle::add_actor). Created via
/// [`Runtime::builder`].
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
/// Dynamic-only runtimes can start without a graph:
///
/// ```no_run
/// use tokio_otp::prelude::*;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let runtime = Runtime::builder().build()?;
/// let handle = runtime.spawn();
/// handle.shutdown_and_wait().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct RuntimeBuilder {
    graph: Option<Graph>,
    subtrees: Vec<(String, RuntimeBuilder)>,
    children: Vec<ChildSpec>,
    strategy: Strategy,
    start_mode: StartMode,
    restart: RestartPolicy,
    shutdown: ShutdownPolicy,
    restart_intensity: Option<RestartIntensity>,
    actor_overrides: HashMap<String, ActorOverrides>,
}

impl RuntimeBuilder {
    /// Creates a builder with default settings: [`OneForOne`](Strategy::OneForOne)
    /// strategy, [`OnFailure`](RestartPolicy::OnFailure) restart, default shutdown
    /// policy, no graph, no subtrees, and no non-actor children.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the actor graph to run. If omitted, the runtime starts empty.
    #[must_use]
    pub fn graph(mut self, graph: Graph) -> Self {
        self.graph = Some(graph);
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
    pub fn subtree(mut self, id: impl Into<String>, subtree: RuntimeBuilder) -> Self {
        self.subtrees.push((id.into(), subtree));
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
        self
    }

    /// Sets the supervisor restart strategy. See [`Strategy`] for options.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets whether actors start concurrently or wait for `on_start` in
    /// declaration order.
    #[must_use]
    pub fn start_mode(mut self, start_mode: StartMode) -> Self {
        self.start_mode = start_mode;
        self
    }

    /// Sets the restart policy applied to every actor child.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self
    }

    /// Sets the outer supervisor shutdown policy applied to every actor child.
    ///
    /// The graph's
    /// [`actor_shutdown_timeout`](crate::GraphBuilder::actor_shutdown_timeout)
    /// independently governs each inner actor task. Prefer a supervisor grace
    /// period at least as long as the actor timeout when shutdown must pass
    /// through the actor layer's clean completion path.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = shutdown;
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

    /// Overrides the outer supervisor shutdown policy for the actor identified
    /// by this typed ref.
    ///
    /// The graph's
    /// [`actor_shutdown_timeout`](crate::GraphBuilder::actor_shutdown_timeout)
    /// still governs the inner actor task.
    #[must_use]
    pub fn actor_shutdown<M>(mut self, actor: &ActorRef<M>, shutdown: ShutdownPolicy) -> Self {
        self.actor_overrides
            .entry(actor.id().to_owned())
            .or_default()
            .shutdown = Some(shutdown);
        self
    }

    /// Validates the configuration and returns a ready-to-run [`Runtime`].
    ///
    /// Returns an error if the supervisor configuration is invalid.
    pub fn build(self) -> Result<Runtime, tokio_supervisor::SupervisorBuildError> {
        let mut supervisor = SupervisorBuilder::new()
            .strategy(self.strategy)
            .start_mode(self.start_mode);
        if let Some(intensity) = self.restart_intensity {
            supervisor = supervisor.restart_intensity(intensity);
        }

        let actor_factory = self
            .graph
            .as_ref()
            .map_or_else(RunnableActorFactory::new, Graph::dynamic_factory);
        let actors = Arc::new(ActorRuntimeState::new(actor_factory));

        for (id, subtree) in self.subtrees {
            let (nested_supervisor, nested_actors) = subtree.build()?.into_parts();
            supervisor = supervisor.supervisor(
                id,
                SupervisorSpec::new(nested_supervisor)
                    .attachment(RuntimeAttachment::subtree(&actors, nested_actors)),
            );
        }

        supervisor = self
            .children
            .into_iter()
            .fold(supervisor, |builder, child| builder.child(child));

        if let Some(graph) = self.graph {
            supervisor = actor_children(
                &graph,
                &actors,
                self.restart,
                self.shutdown,
                &self.actor_overrides,
            )
            .into_iter()
            .fold(supervisor, |builder, child| builder.child(child));
        }
        Ok(Runtime::with_actor_tree(supervisor.build()?, actors))
    }
}

impl std::fmt::Debug for RuntimeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeBuilder")
            .field("strategy", &self.strategy)
            .field("start_mode", &self.start_mode)
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
