//! Supervision trees expressed as inspectable recursive data.

use std::sync::Arc;

use tokio_supervisor::{
    ChildSpec, DynamicSupervisorBuilder, RestartIntensity, RestartPolicy, ScopeKind,
    ShutdownPolicy, Strategy, Supervisor, SupervisorBuildError, SupervisorBuilder, SupervisorSpec,
};

use crate::{
    Graph, RunnableActor, RunnableActorBuilder, Runtime,
    runtime::{ActorChildOptions, ActorRuntimeState, RuntimeAttachment, actor_child_spec},
};

/// Configuration carried by an ordered or dynamic scope node.
///
/// `children` is semantic for ordered nodes. Dynamic nodes must leave it empty;
/// retaining it in the declaration lets [`build`](SupervisionTree::build)
/// return a typed error when fluent construction attempts to declare a child.
pub struct SupervisionScope {
    /// Optional id when this scope is nested in another scope.
    pub id: Option<String>,
    /// Restart strategy. Dynamic scopes require [`Strategy::OneForOne`].
    pub strategy: Strategy,
    /// Optional scope-level restart-intensity default.
    pub restart_intensity: Option<RestartIntensity>,
    /// Default restart policy inherited by actor nodes.
    pub default_restart: RestartPolicy,
    /// Default shutdown policy inherited by actor nodes.
    pub default_shutdown: ShutdownPolicy,
    /// Declared child nodes, in semantic order.
    pub children: Vec<SupervisionTree>,
    invalid_config: Option<&'static str>,
    dynamic_builder: Option<RunnableActorBuilder>,
    reserved_builder: Option<ReservedScopeBuilder>,
    reserved_actors: Option<Arc<ActorRuntimeState>>,
}

enum ReservedScopeBuilder {
    Ordered(SupervisorBuilder),
    Dynamic(DynamicSupervisorBuilder),
}

impl Clone for SupervisionScope {
    /// Copies the declaration but **not** the reserved scope identity.
    ///
    /// A scope produced by [`RuntimeBuilder::into_tree`](crate::RuntimeBuilder::into_tree)
    /// carries the builder whose [`handle`](crate::RuntimeBuilder::handle) was
    /// already handed out. Only one tree can own that identity, so the clone
    /// reserves a fresh one when it is built, and the previously taken handle
    /// keeps addressing the original — which goes terminal if that original is
    /// dropped rather than built. Build the value the handle came from, and
    /// clone only declarations whose handles you have not taken.
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            strategy: self.strategy,
            restart_intensity: self.restart_intensity,
            default_restart: self.default_restart,
            default_shutdown: self.default_shutdown,
            children: self.children.clone(),
            invalid_config: self.invalid_config,
            dynamic_builder: self.dynamic_builder.clone(),
            reserved_builder: None,
            reserved_actors: None,
        }
    }
}

impl SupervisionScope {
    fn new() -> Self {
        Self {
            id: None,
            strategy: Strategy::default(),
            restart_intensity: None,
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            children: Vec::new(),
            invalid_config: None,
            dynamic_builder: None,
            reserved_builder: None,
            reserved_actors: None,
        }
    }
}

/// A recursive, executable supervision declaration.
///
/// [`RuntimeBuilder`](crate::RuntimeBuilder) is the convenient front door for
/// the common case, but it describes a tree through method calls. Calling
/// [`RuntimeBuilder::into_tree`](crate::RuntimeBuilder::into_tree) exposes the
/// equivalent declaration as data before anything runs; `RuntimeBuilder::build`
/// itself lowers through that same tree. Constructing a `SupervisionTree`
/// directly also lets one graph's actors occupy different scope levels while
/// retaining the typed wiring established by the graph.
///
/// [`outline`](Self::outline) removes factories and other executable payloads,
/// producing a [`SupervisionOutline`] that can be compared, debug-printed, and,
/// with the `serde` feature, serialized. The outline is the declared companion
/// to a running [`SupervisorSnapshot`](tokio_supervisor::SupervisorSnapshot).
///
/// # Scope kinds and child order
///
/// A runnable tree has an [`Ordered`](Self::Ordered) or
/// [`Dynamic`](Self::Dynamic) root. Ordered scopes contain a declared child
/// sequence; its order controls readiness-gated startup, reverse-order
/// shutdown, and [`Strategy::RestForOne`] restart scope. Dynamic scopes are
/// empty leaves whose membership is written through a
/// [`RuntimeHandle`](crate::RuntimeHandle) after spawn.
///
/// [`Actor`](Self::Actor), [`Child`](Self::Child), and
/// [`ActorWithScope`](Self::ActorWithScope) are child nodes. Add them beneath a
/// scope with [`actor`](Self::actor), [`task`](Self::task),
/// [`subtree`](Self::subtree), or [`actor_with_scope`](Self::actor_with_scope).
///
/// # Example
///
/// ```
/// use tokio_otp::{ActorSpec, SupervisionTree, prelude::*};
///
/// struct Worker;
///
/// impl Actor for Worker {
///     type Msg = ();
///
///     async fn handle(&mut self, (): (), _ctx: &mut ActorContext<()>) -> ActorResult {
///         Ok(Continue)
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut graph = GraphBuilder::new();
/// let _ingest = graph.actor("ingest", || Worker);
/// let _parse = graph.actor("parse", || Worker);
/// let graph = graph.build()?;
///
/// let tree = SupervisionTree::new()
///     .strategy(Strategy::RestForOne)
///     .actor(ActorSpec::new(graph.actors()[0].clone()).restart(RestartPolicy::Never))
///     .subtree(
///         "workers",
///         SupervisionTree::new().actor(graph.actors()[1].clone()),
///     );
///
/// let outline = tree.outline()?;
/// assert_eq!(outline.child_ids(), ["ingest", "workers"]);
/// let runtime = tree.build()?;
/// # drop(runtime);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub enum SupervisionTree {
    /// A declared, readiness-gated child sequence.
    Ordered {
        /// Scope configuration and ordered children.
        scope: SupervisionScope,
    },
    /// An empty scope whose membership is written at runtime.
    Dynamic {
        /// Dynamic-scope policy. Declared children are rejected at build time.
        scope: SupervisionScope,
    },
    /// A graph actor child.
    Actor(ActorSpec),
    /// An arbitrary non-actor task child.
    Child(ChildSpec),
    /// An actor leader followed by a scope it owns.
    ActorWithScope {
        /// Id of the generated ordered scope in its parent.
        id: String,
        /// Leader actor, installed first.
        actor: ActorSpec,
        /// Scope owned by the leader, installed second as `children`.
        children: Box<SupervisionTree>,
        /// Restart relationship between the leader and owned scope.
        strategy: Strategy,
    },
}

/// A graph actor placed in a supervision tree with optional policy overrides.
#[derive(Clone)]
pub struct ActorSpec {
    actor: RunnableActor,
    restart: Option<RestartPolicy>,
    shutdown: Option<ShutdownPolicy>,
    restart_intensity: Option<RestartIntensity>,
}

impl ActorSpec {
    /// Places a runnable actor using its enclosing ordered scope's defaults.
    pub fn new(actor: RunnableActor) -> Self {
        Self {
            actor,
            restart: None,
            shutdown: None,
            restart_intensity: None,
        }
    }

    /// Overrides the enclosing scope's default restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Overrides the enclosing scope's default shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Gives this actor its own restart-intensity window.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = Some(intensity);
        self
    }

    /// Returns the actor label, which is also its child id.
    pub fn label(&self) -> &str {
        self.actor.label()
    }
}

impl From<RunnableActor> for ActorSpec {
    fn from(actor: RunnableActor) -> Self {
        Self::new(actor)
    }
}

impl std::fmt::Debug for ActorSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorSpec")
            .field("label", &self.label())
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .field("restart_intensity", &self.restart_intensity)
            .finish()
    }
}

impl Default for SupervisionTree {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisionTree {
    /// Creates an empty ordered scope with standard runtime defaults.
    pub fn new() -> Self {
        Self::Ordered {
            scope: SupervisionScope::new(),
        }
    }

    /// Creates an empty dynamic scope.
    pub fn dynamic() -> Self {
        Self::Dynamic {
            scope: SupervisionScope::new(),
        }
    }

    pub(crate) fn with_ordered_builder(
        mut self,
        builder: SupervisorBuilder,
        actors: Arc<ActorRuntimeState>,
    ) -> Self {
        let scope = self
            .scope_mut()
            .expect("reserved builder applies only to scope nodes");
        scope.reserved_builder = Some(ReservedScopeBuilder::Ordered(builder));
        scope.reserved_actors = Some(actors);
        self
    }

    pub(crate) fn with_dynamic_builder(
        mut self,
        builder: DynamicSupervisorBuilder,
        actors: Arc<ActorRuntimeState>,
    ) -> Self {
        let scope = self
            .scope_mut()
            .expect("reserved builder applies only to scope nodes");
        scope.reserved_builder = Some(ReservedScopeBuilder::Dynamic(builder));
        scope.reserved_actors = Some(actors);
        self
    }

    /// Creates an ordered scope containing every actor in a graph.
    pub fn graph(graph: &Graph) -> Self {
        let mut tree = Self::new().dynamic_defaults(graph);
        for actor in graph.actors() {
            tree = tree.actor(actor.clone());
        }
        tree
    }

    fn scope(&self) -> Option<&SupervisionScope> {
        match self {
            Self::Ordered { scope } | Self::Dynamic { scope } => Some(scope),
            _ => None,
        }
    }

    fn scope_mut(&mut self) -> Option<&mut SupervisionScope> {
        match self {
            Self::Ordered { scope } | Self::Dynamic { scope } => Some(scope),
            _ => None,
        }
    }

    /// Adopts a graph's actor execution settings for runtime-added actors.
    #[must_use]
    pub fn dynamic_defaults(mut self, graph: &Graph) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.dynamic_builder = Some(graph.dynamic_builder());
        }
        self
    }

    /// Returns this scope's immutable kind, or `None` for a child node.
    pub fn kind(&self) -> Option<ScopeKind> {
        match self {
            Self::Ordered { .. } => Some(ScopeKind::Ordered),
            Self::Dynamic { .. } => Some(ScopeKind::Dynamic),
            _ => None,
        }
    }

    /// Sets the restart strategy of a scope node.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.strategy = strategy;
        }
        self
    }

    /// Sets this scope's default restart intensity.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.restart_intensity = Some(intensity);
        }
        self
    }

    /// Sets the restart policy inherited by actor nodes.
    #[must_use]
    pub fn default_restart(mut self, restart: RestartPolicy) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.default_restart = restart;
        }
        self
    }

    /// Sets the shutdown policy inherited by actor nodes.
    #[must_use]
    pub fn default_shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.default_shutdown = shutdown;
        }
        self
    }

    /// Appends an actor node to this scope.
    #[must_use]
    pub fn actor(self, actor: impl Into<ActorSpec>) -> Self {
        self.child(Self::Actor(actor.into()))
    }

    /// Appends an arbitrary task node to this scope.
    #[must_use]
    pub fn task(self, child: ChildSpec) -> Self {
        self.child(Self::Child(child))
    }

    /// Appends a named nested ordered or dynamic scope.
    #[must_use]
    pub fn subtree(mut self, id: impl Into<String>, tree: impl Into<SupervisionTree>) -> Self {
        let mut tree = tree.into();
        let Some(nested_scope) = tree.scope_mut() else {
            if let Some(scope) = self.scope_mut() {
                scope
                    .invalid_config
                    .get_or_insert("a nested subtree must be an ordered or dynamic scope");
            }
            return self;
        };
        nested_scope.id = Some(id.into());
        if let Some(scope) = self.scope_mut() {
            scope.children.push(tree);
        }
        self
    }

    /// Appends an actor leader with an owned scope using
    /// [`Strategy::RestForOne`].
    #[must_use]
    pub fn actor_with_scope(
        self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: impl Into<SupervisionTree>,
    ) -> Self {
        self.actor_with_scope_strategy(id, actor, children, Strategy::RestForOne)
    }

    /// Appends an actor leader with an owned scope and an explicit restart
    /// relationship.
    ///
    /// The node lowers to the ordered pair `[leader, children]`, so `strategy`
    /// states how the two relate when one of them fails:
    ///
    /// - [`RestForOne`](Strategy::RestForOne) — the default from
    ///   [`actor_with_scope`](Self::actor_with_scope). A failing leader
    ///   recycles the child scope with it; a failure inside the child scope
    ///   leaves the leader running.
    /// - [`OneForAll`](Strategy::OneForAll) — either side failing recycles
    ///   both. Use it when the leader cannot outlive the workers it created.
    /// - [`OneForOne`](Strategy::OneForOne) — the two restart independently.
    ///   Accepted, but rarely what a leader wants: it survives with a child
    ///   scope it no longer has state for.
    #[must_use]
    pub fn actor_with_scope_strategy(
        self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: impl Into<SupervisionTree>,
        strategy: Strategy,
    ) -> Self {
        self.child(Self::ActorWithScope {
            id: id.into(),
            actor: actor.into(),
            children: Box::new(children.into()),
            strategy,
        })
    }

    /// Appends an already constructed recursive child node.
    #[must_use]
    pub fn child(mut self, child: SupervisionTree) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.children.push(child);
        }
        self
    }

    /// Returns declared children in semantic order, or an empty slice for a
    /// child node.
    pub fn children(&self) -> &[SupervisionTree] {
        self.scope().map_or(&[], |scope| scope.children.as_slice())
    }

    /// Projects the executable scope to comparable, payload-free data.
    pub fn outline(&self) -> Result<SupervisionOutline, SupervisorBuildError> {
        let (kind, scope) = match self {
            Self::Ordered { scope } => (ScopeKind::Ordered, scope),
            Self::Dynamic { scope } => (ScopeKind::Dynamic, scope),
            _ => {
                return Err(SupervisorBuildError::InvalidConfig(
                    "a supervision root must be an ordered or dynamic scope",
                ));
            }
        };
        if let Some(message) = scope.invalid_config {
            return Err(SupervisorBuildError::InvalidConfig(message));
        }
        Ok(SupervisionOutline {
            kind,
            strategy: scope.strategy,
            default_restart: scope.default_restart,
            default_shutdown: scope.default_shutdown,
            restart_intensity: scope.restart_intensity.unwrap_or_default(),
            children: scope
                .children
                .iter()
                .map(|child| child.outline_child(scope.default_restart, scope.default_shutdown))
                .collect::<Result<_, _>>()?,
        })
    }

    fn outline_child(
        &self,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) -> Result<ChildOutline, SupervisorBuildError> {
        Ok(match self {
            Self::Actor(actor) => ChildOutline::Actor {
                label: actor.label().to_owned(),
                restart: actor.restart.unwrap_or(default_restart),
                shutdown: actor.shutdown.unwrap_or(default_shutdown),
                restart_intensity: actor.restart_intensity,
            },
            Self::Child(spec) => ChildOutline::Child {
                id: spec.id().to_owned(),
                restart: spec.restart_policy(),
                shutdown: spec.shutdown_policy(),
            },
            Self::Ordered { scope } | Self::Dynamic { scope } => ChildOutline::Scope {
                id: scope.id.clone().unwrap_or_else(|| "<unnamed>".to_owned()),
                outline: self.outline()?,
            },
            Self::ActorWithScope {
                id,
                actor,
                children,
                strategy,
            } => ChildOutline::ActorWithScope {
                id: id.clone(),
                leader: Box::new(ChildOutline::Actor {
                    label: actor.label().to_owned(),
                    restart: actor.restart.unwrap_or(default_restart),
                    shutdown: actor.shutdown.unwrap_or(default_shutdown),
                    restart_intensity: actor.restart_intensity,
                }),
                children: Box::new(children.outline()?),
                strategy: *strategy,
            },
        })
    }

    /// Validates and lowers this declaration to a runnable actor runtime.
    pub fn build(self) -> Result<Runtime, SupervisorBuildError> {
        let (supervisor, actors) = self.lower_scope()?;
        Ok(Runtime::with_actor_tree(supervisor, actors))
    }

    fn lower_scope(self) -> Result<(Supervisor, Arc<ActorRuntimeState>), SupervisorBuildError> {
        let (kind, mut scope) = match self {
            Self::Ordered { scope } => (ScopeKind::Ordered, scope),
            Self::Dynamic { scope } => (ScopeKind::Dynamic, scope),
            _ => {
                return Err(SupervisorBuildError::InvalidConfig(
                    "a supervision root must be an ordered or dynamic scope",
                ));
            }
        };
        if let Some(message) = scope.invalid_config {
            return Err(SupervisorBuildError::InvalidConfig(message));
        }
        let actor_builder = scope.dynamic_builder.clone().unwrap_or_default();
        let actors = scope.reserved_actors.take().unwrap_or_else(|| {
            Arc::new(ActorRuntimeState::new(
                actor_builder.clone(),
                scope.default_restart,
                scope.default_shutdown,
            ))
        });
        actors.configure(actor_builder, scope.default_restart, scope.default_shutdown);
        let reserved_builder = scope.reserved_builder.take();

        if kind == ScopeKind::Dynamic {
            if scope.strategy != Strategy::OneForOne {
                return Err(SupervisorBuildError::InvalidConfig(
                    "dynamic scopes require Strategy::OneForOne",
                ));
            }
            if !scope.children.is_empty() {
                return Err(SupervisorBuildError::InvalidConfig(
                    "dynamic scopes cannot have declared children",
                ));
            }
            let mut builder = match reserved_builder {
                Some(ReservedScopeBuilder::Dynamic(builder)) => builder,
                Some(ReservedScopeBuilder::Ordered(_)) => {
                    unreachable!("scope builder kind matches")
                }
                None => DynamicSupervisorBuilder::new(),
            };
            builder = builder
                .restart(scope.default_restart)
                .shutdown(scope.default_shutdown);
            if let Some(intensity) = scope.restart_intensity {
                builder = builder.restart_intensity(intensity);
            }
            return Ok((builder.build()?, actors));
        }

        let builder = match reserved_builder {
            Some(ReservedScopeBuilder::Ordered(builder)) => builder,
            Some(ReservedScopeBuilder::Dynamic(_)) => unreachable!("scope builder kind matches"),
            None => SupervisorBuilder::new(),
        };
        let mut builder = builder.strategy(scope.strategy);
        if let Some(intensity) = scope.restart_intensity {
            builder = builder.restart_intensity(intensity);
        }
        for child in scope.children {
            builder = match child {
                Self::Actor(actor) => builder.child(actor_child_spec(
                    actor.actor,
                    &actors,
                    ActorChildOptions::new(
                        actor.restart.unwrap_or(scope.default_restart),
                        actor.shutdown.unwrap_or(scope.default_shutdown),
                    )
                    .restart_intensity(actor.restart_intensity),
                )),
                Self::Child(spec) => builder.child(spec),
                tree @ (Self::Ordered { .. } | Self::Dynamic { .. }) => {
                    let id = tree.scope().and_then(|nested| nested.id.clone()).ok_or(
                        SupervisorBuildError::InvalidConfig("nested scopes require an id"),
                    )?;
                    let (nested, nested_actors) = tree.lower_scope()?;
                    builder.supervisor(
                        SupervisorSpec::new(id, nested)
                            .attachment(RuntimeAttachment::subtree(&actors, nested_actors)),
                    )
                }
                Self::ActorWithScope {
                    id,
                    actor,
                    children,
                    strategy,
                } => {
                    let owned_actors = Arc::new(ActorRuntimeState::new(
                        RunnableActorBuilder::new(),
                        scope.default_restart,
                        scope.default_shutdown,
                    ));
                    let (children_supervisor, children_actors) = children.lower_scope()?;
                    let children_handle = crate::RuntimeHandle::new(
                        children_supervisor.handle(),
                        Arc::clone(&children_actors),
                    );
                    let leader = actor_child_spec(
                        actor.actor,
                        &owned_actors,
                        ActorChildOptions::new(
                            actor.restart.unwrap_or(scope.default_restart),
                            actor.shutdown.unwrap_or(scope.default_shutdown),
                        )
                        .restart_intensity(actor.restart_intensity)
                        .children(children_handle),
                    );
                    let owned = SupervisorBuilder::new()
                        .strategy(strategy)
                        .child(leader)
                        .supervisor(
                            SupervisorSpec::new("children", children_supervisor).attachment(
                                RuntimeAttachment::subtree(&owned_actors, children_actors),
                            ),
                        )
                        .build()?;
                    builder.supervisor(
                        SupervisorSpec::new(id, owned)
                            .attachment(RuntimeAttachment::subtree(&actors, owned_actors)),
                    )
                }
            };
        }

        Ok((builder.build()?, actors))
    }
}

impl std::fmt::Debug for SupervisionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordered { .. } | Self::Dynamic { .. } => match self.outline() {
                Ok(outline) => outline.fmt(f),
                Err(error) => f
                    .debug_tuple("InvalidSupervisionTree")
                    .field(&error)
                    .finish(),
            },
            Self::Actor(actor) => actor.fmt(f),
            Self::Child(child) => f.debug_tuple("Child").field(&child.id()).finish(),
            Self::ActorWithScope { id, .. } => f
                .debug_struct("ActorWithScope")
                .field("id", id)
                .finish_non_exhaustive(),
        }
    }
}

/// Payload-free declaration tree suitable for comparison and serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct SupervisionOutline {
    /// Immutable scope kind.
    pub kind: ScopeKind,
    /// Restart strategy.
    pub strategy: Strategy,
    /// Restart policy inherited by children without an explicit override.
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_restart: RestartPolicy,
    /// Shutdown policy inherited by children without an explicit override.
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_shutdown: ShutdownPolicy,
    /// Default restart-intensity policy.
    pub restart_intensity: RestartIntensity,
    /// Declared children in semantic order; empty for a valid dynamic scope.
    pub children: Vec<ChildOutline>,
}

/// One payload-free child declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildOutline {
    /// An actor with resolved policies.
    Actor {
        /// Actor label and child id.
        label: String,
        /// Resolved restart policy.
        restart: RestartPolicy,
        /// Resolved shutdown policy.
        shutdown: ShutdownPolicy,
        /// Optional child-specific intensity policy.
        restart_intensity: Option<RestartIntensity>,
    },
    /// An arbitrary task child.
    Child {
        /// Child id.
        id: String,
        /// Restart policy.
        restart: RestartPolicy,
        /// Shutdown policy.
        shutdown: ShutdownPolicy,
    },
    /// A nested ordered or dynamic scope.
    Scope {
        /// Scope child id.
        id: String,
        /// Nested declaration.
        outline: SupervisionOutline,
    },
    /// Generated actor-owned ordered scope.
    ActorWithScope {
        /// Node id in the parent.
        id: String,
        /// Leader actor declaration.
        leader: Box<ChildOutline>,
        /// Owned scope declaration.
        children: Box<SupervisionOutline>,
        /// Restart relationship of leader and owned scope.
        strategy: Strategy,
    },
}

impl SupervisionOutline {
    /// Returns direct child ids in declaration order.
    pub fn child_ids(&self) -> Vec<&str> {
        self.children.iter().map(ChildOutline::id).collect()
    }

    /// Finds a direct child by id.
    pub fn child(&self, id: &str) -> Option<&ChildOutline> {
        self.children.iter().find(|child| child.id() == id)
    }
}

impl ChildOutline {
    /// Returns this node's id within its parent.
    pub fn id(&self) -> &str {
        match self {
            Self::Actor { label, .. } => label,
            Self::Child { id, .. } | Self::Scope { id, .. } | Self::ActorWithScope { id, .. } => id,
        }
    }
}
