use crate::{Graph, RunnableActorFactory};
use tokio_supervisor::{
    RestartIntensity, RestartPolicy, ShutdownPolicy, StartMode, Strategy, SupervisorBuilder,
};

use crate::{runtime::Runtime, supervised_actors::SupervisedActors};

/// One-stop builder for the common supervised-actor setup.
///
/// Wires an actor [`Graph`] into a [`Runtime`] where every actor runs as its
/// own supervised child. Nested builders added with [`subtree`](Self::subtree)
/// preserve the same actor-aware runtime behavior recursively. It can also
/// build a graph-less runtime that starts empty and grows through
/// [`RuntimeHandle::add_actor`](crate::RuntimeHandle::add_actor). Created via
/// [`Runtime::builder`].
///
/// For per-actor policy overrides or arbitrary non-actor children, drop down
/// to [`SupervisedActors`] with a [`SupervisorBuilder`].
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
///     async fn handle(&mut self, message: String, _ctx: &ActorContext<String>) -> ActorResult {
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
    strategy: Strategy,
    start_mode: StartMode,
    restart: RestartPolicy,
    shutdown: ShutdownPolicy,
    restart_intensity: Option<RestartIntensity>,
}

impl RuntimeBuilder {
    /// Creates a builder with default settings: [`OneForOne`](Strategy::OneForOne)
    /// strategy, [`OnFailure`](RestartPolicy::OnFailure) restart, default shutdown
    /// policy, no graph, and no subtrees.
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

        let mut subtrees = Vec::with_capacity(self.subtrees.len());
        for (id, subtree) in self.subtrees {
            let (nested_supervisor, nested_actors) = subtree.build()?.into_parts();
            supervisor = supervisor.supervisor(id.clone(), nested_supervisor);
            subtrees.push((id, nested_actors));
        }

        let runtime = match self.graph {
            Some(graph) => {
                let supervised = SupervisedActors::new(graph)
                    .restart(self.restart)
                    .shutdown(self.shutdown);
                supervised.build_runtime(supervisor)
            }
            None => {
                let supervisor = supervisor.build()?;
                Ok(Runtime::with_actor_tree(
                    supervisor,
                    RunnableActorFactory::new(),
                    Vec::new(),
                ))
            }
        }?;
        for (id, subtree) in subtrees {
            runtime.actor_state().record_subtree(id, subtree, None);
        }
        Ok(runtime)
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
            .field(
                "subtrees",
                &self.subtrees.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}
