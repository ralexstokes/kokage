//! Supervision trees expressed as inspectable recursive data.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio_supervisor::{
    ChildSpec, DynamicSupervisorBuilder, RestartIntensity, RestartPolicy, ScopeKind,
    ShutdownPolicy, Strategy, Supervisor, SupervisorBuildError, SupervisorBuilder, SupervisorSpec,
};

use crate::{
    Graph, RunnableActor, Runtime,
    actor::RunnableActorBuilder,
    runtime::{ActorChildOptions, ActorRuntimeState, RuntimeAttachment, actor_child_spec},
};

/// Configuration carried by an ordered or dynamic scope node.
///
/// `children` is semantic for ordered nodes. Dynamic nodes must leave it empty;
/// retaining it in the declaration lets [`build`](SupervisionTree::build)
/// return a typed error when fluent construction attempts to declare a child.
#[derive(Clone)]
pub struct SupervisionScope {
    id: Option<String>,
    strategy: Strategy,
    restart_intensity: Option<RestartIntensity>,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
    children: Vec<SupervisionTree>,
    invalid_config: Option<&'static str>,
    dynamic_builder: Option<RunnableActorBuilder>,
    reservation: Option<u64>,
}

enum ReservedScopeBuilder {
    Ordered(Option<SupervisorBuilder>),
    Dynamic(Option<DynamicSupervisorBuilder>),
}

struct ScopeReservation {
    id: u64,
    builder: ReservedScopeBuilder,
    actors: Arc<ActorRuntimeState>,
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
            reservation: None,
        }
    }
}

/// A recursive, executable supervision declaration.
///
/// This is the primary composition API. It is plain, inspectable, and
/// cloneable declaration data until [`reserve`](Self::reserve) is called or it
/// is lowered directly with [`build`](Self::build). It supports nested scopes,
/// arbitrary task children, per-actor policy overrides, and actor-owned
/// scopes. [`RuntimeBuilder`](crate::RuntimeBuilder) is thin convenience for
/// placing every actor from one graph in one ordered scope.
///
/// A tree can place one graph's actors at different scope levels while
/// retaining the typed wiring established by the graph;
/// [`#[derive(Supervision)]`](crate::Supervision) is the static shorthand for
/// exactly that, declaring the graph and this tree from one struct.
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
///     async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
///         Ok(Continue)
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut graph = GraphBuilder::new();
/// let (_ingest_slot, _ingest) = graph.slot("ingest");
/// graph.define(_ingest_slot, || Worker);
/// let (_parse_slot, _parse) = graph.slot("parse");
/// graph.define(_parse_slot, || Worker);
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

/// A supervision declaration with one or more pre-spawn scope identities.
///
/// Created by [`SupervisionTree::reserve`]. Unlike a plain
/// [`SupervisionTree`], this type deliberately does not implement [`Clone`]:
/// each reserved [`RuntimeHandle`](crate::RuntimeHandle) must bind to exactly
/// one eventual runtime or become terminal when the declaration is dropped.
/// It retains the tree's fluent configuration and composition surface, so a
/// handle can be taken before the final declaration is assembled. Its
/// introspection is deliberately limited to payload-free [`outline`](Self::outline):
/// exposing nested `SupervisionTree` nodes would make reservation markers
/// cloneable again.
pub struct ReservedSupervisionTree {
    tree: SupervisionTree,
    reservations: Vec<ScopeReservation>,
}

/// A graph actor placed in a supervision tree with optional policy overrides.
#[derive(Clone)]
pub struct ActorSpec {
    actor: RunnableActor,
    child_id: Option<String>,
    restart: Option<RestartPolicy>,
    shutdown: Option<ShutdownPolicy>,
    restart_intensity: Option<RestartIntensity>,
}

impl ActorSpec {
    /// Places a runnable actor using its enclosing ordered scope's defaults.
    pub fn new(actor: RunnableActor) -> Self {
        Self {
            actor,
            child_id: None,
            restart: None,
            shutdown: None,
            restart_intensity: None,
        }
    }

    /// Names this actor within its enclosing scope.
    ///
    /// Child ids are local to one supervisor, while an actor label is unique
    /// across the whole graph. They coincide by default. A nested derived
    /// scope sets a local id here so the supervisor path spells the
    /// qualified label once — `root.workers.parse` rather than
    /// `root.workers.workers.parse` — instead of repeating the scope name.
    #[must_use]
    pub fn child_id(mut self, id: impl Into<String>) -> Self {
        self.child_id = Some(id.into());
        self
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

    /// Returns the actor label, which is unique across the graph.
    pub fn label(&self) -> &str {
        self.actor.label()
    }

    /// Returns this actor's id within its enclosing scope.
    ///
    /// Defaults to [`label`](Self::label) unless
    /// [`child_id`](Self::child_id) overrode it.
    pub fn id(&self) -> &str {
        self.child_id.as_deref().unwrap_or_else(|| self.label())
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
            .field("id", &self.id())
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

    /// Creates an ordered scope containing every actor in a graph.
    pub fn graph(graph: &Graph) -> Self {
        let mut tree = Self::derived_scope(graph);
        for actor in graph.actors() {
            tree = tree.actor(actor.clone());
        }
        tree
    }

    /// Creates an empty graph-backed scope for generated supervision code.
    ///
    /// This is an internal derive contract, not an additional composition
    /// front door. Applications should use [`new`](Self::new),
    /// [`dynamic`](Self::dynamic), or [`graph`](Self::graph).
    #[doc(hidden)]
    pub fn derived_scope(graph: &Graph) -> Self {
        let mut tree = Self::new();
        tree.set_dynamic_builder(graph.dynamic_builder());
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

    fn set_dynamic_builder(&mut self, builder: RunnableActorBuilder) {
        if let Some(scope) = self.scope_mut() {
            scope.dynamic_builder = Some(builder);
        }
    }

    /// Moves this declaration into a non-cloneable form and reserves its root
    /// scope identity.
    ///
    /// The returned handle is available through
    /// [`ReservedSupervisionTree::handle`] before the runtime is built or
    /// spawned. Dropping the reserved declaration or failing to build it makes
    /// that identity terminal and closes its subscriptions.
    pub fn reserve(self) -> Result<ReservedSupervisionTree, SupervisorBuildError> {
        ReservedSupervisionTree::new(self)
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

    /// Names this scope node for use as a child of another scope.
    ///
    /// [`subtree`](Self::subtree) names a scope while appending it. Use this
    /// when a node is built separately from the scope that will adopt it and
    /// appended with [`child`](Self::child).
    ///
    /// Ignored on a child node — actor, child spec, and actor-with-scope nodes
    /// carry their id in the declaration itself — like the other scope setters.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.id = Some(id.into());
        }
        self
    }

    /// Records that a declared actor was not found in the graph.
    ///
    /// Not a stable surface: `#[derive(Supervision)]` calls this from the code
    /// it generates for [`Supervision::node`](crate::Supervision::node) when
    /// the graph it was handed does not contain an actor the scope declared,
    /// which happens only when `node` receives a different graph from the one
    /// its [`open`](crate::Supervision::open) populated. The mismatch then
    /// surfaces from [`build`](Self::build) and [`outline`](Self::outline) as
    /// [`SupervisorBuildError::InvalidConfig`] rather than panicking inside
    /// generated code.
    ///
    /// The message is `&'static str`, so it cannot name the missing label; the
    /// qualified label is the scope path joined to the field name, or to its
    /// `label` override.
    #[doc(hidden)]
    #[must_use]
    pub fn missing_actor(mut self) -> Self {
        if let Some(scope) = self.scope_mut() {
            scope.invalid_config.get_or_insert(
                "a derived scope references an actor that is not in this graph; \
                 `Supervision::node` must receive the graph its `open` populated",
            );
        }
        self
    }

    /// Appends a named nested ordered or dynamic scope.
    #[must_use]
    pub fn subtree(mut self, id: impl Into<String>, mut tree: SupervisionTree) -> Self {
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

    /// Appends an actor leader with an owned scope and an explicit restart
    /// relationship.
    ///
    /// The node lowers to the ordered pair `[leader, children]`, so `strategy`
    /// states how the two relate when one of them fails:
    ///
    /// - [`RestForOne`](Strategy::RestForOne) — a failing leader recycles the
    ///   child scope with it; a failure inside the child scope leaves the
    ///   leader running.
    /// - [`OneForAll`](Strategy::OneForAll) — either side failing recycles
    ///   both. Use it when the leader cannot outlive the workers it created.
    /// - [`OneForOne`](Strategy::OneForOne) — the two restart independently.
    ///   Accepted, but rarely what a leader wants: it survives with a child
    ///   scope it no longer has state for.
    #[must_use]
    pub fn actor_with_scope(
        self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: SupervisionTree,
        strategy: Strategy,
    ) -> Self {
        self.child(Self::ActorWithScope {
            id: id.into(),
            actor: actor.into(),
            children: Box::new(children),
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

    fn declared_id(&self) -> Option<&str> {
        match self {
            Self::Ordered { scope } | Self::Dynamic { scope } => scope.id.as_deref(),
            Self::Actor(actor) => Some(actor.id()),
            Self::Child(child) => Some(child.id()),
            Self::ActorWithScope { id, .. } => Some(id),
        }
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
                id: actor.id().to_owned(),
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
                    id: actor.id().to_owned(),
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
        let (supervisor, actors) = self.lower_scope(&mut Vec::new())?;
        Ok(Runtime::with_actor_tree(supervisor, actors))
    }

    fn lower_scope(
        self,
        reservations: &mut Vec<ScopeReservation>,
    ) -> Result<(Supervisor, Arc<ActorRuntimeState>), SupervisorBuildError> {
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
        let actor_builder = scope.dynamic_builder.clone().unwrap_or_default();
        let reservation = scope.reservation.and_then(|id| {
            reservations
                .iter()
                .position(|reservation| reservation.id == id)
                .map(|index| reservations.swap_remove(index))
        });
        let actors = reservation.as_ref().map_or_else(
            || {
                Arc::new(ActorRuntimeState::new(
                    actor_builder.clone(),
                    scope.default_restart,
                    scope.default_shutdown,
                ))
            },
            |reservation| Arc::clone(&reservation.actors),
        );
        actors.configure(actor_builder, scope.default_restart, scope.default_shutdown);
        let reserved_builder = reservation.map(|reservation| reservation.builder);

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
                Some(ReservedScopeBuilder::Dynamic(mut builder)) => builder
                    .take()
                    .expect("live reservation owns its dynamic builder"),
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
            Some(ReservedScopeBuilder::Ordered(mut builder)) => builder
                .take()
                .expect("live reservation owns its ordered builder"),
            Some(ReservedScopeBuilder::Dynamic(_)) => unreachable!("scope builder kind matches"),
            None => SupervisorBuilder::new(),
        };
        let mut builder = builder.strategy(scope.strategy);
        if let Some(intensity) = scope.restart_intensity {
            builder = builder.restart_intensity(intensity);
        }
        for child in scope.children {
            builder = match child {
                Self::Actor(ActorSpec {
                    actor,
                    child_id,
                    restart,
                    shutdown,
                    restart_intensity,
                }) => builder.child(actor_child_spec(
                    actor,
                    &actors,
                    ActorChildOptions::new(
                        restart.unwrap_or(scope.default_restart),
                        shutdown.unwrap_or(scope.default_shutdown),
                    )
                    .restart_intensity(restart_intensity)
                    .child_id(child_id),
                )),
                Self::Child(spec) => builder.child(spec),
                tree @ (Self::Ordered { .. } | Self::Dynamic { .. }) => {
                    let id = tree.scope().and_then(|nested| nested.id.clone()).ok_or(
                        SupervisorBuildError::InvalidConfig("nested scopes require an id"),
                    )?;
                    let (nested, nested_actors) = tree.lower_scope(reservations)?;
                    builder.supervisor(
                        SupervisorSpec::new(id, nested)
                            .attachment(RuntimeAttachment::subtree(&actors, nested_actors)),
                    )
                }
                Self::ActorWithScope {
                    id,
                    actor:
                        ActorSpec {
                            actor,
                            child_id,
                            restart,
                            shutdown,
                            restart_intensity,
                        },
                    children,
                    strategy,
                } => {
                    let owned_actors = Arc::new(ActorRuntimeState::new(
                        RunnableActorBuilder::new(),
                        scope.default_restart,
                        scope.default_shutdown,
                    ));
                    let (children_supervisor, children_actors) =
                        children.lower_scope(reservations)?;
                    let children_handle = crate::RuntimeHandle::new(
                        children_supervisor.handle(),
                        Arc::clone(&children_actors),
                    );
                    let leader = actor_child_spec(
                        actor,
                        &owned_actors,
                        ActorChildOptions::new(
                            restart.unwrap_or(scope.default_restart),
                            shutdown.unwrap_or(scope.default_shutdown),
                        )
                        .restart_intensity(restart_intensity)
                        .child_id(child_id)
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

static NEXT_RESERVATION_ID: AtomicU64 = AtomicU64::new(1);

impl ReservedSupervisionTree {
    fn new(mut tree: SupervisionTree) -> Result<Self, SupervisorBuildError> {
        let scope = tree.scope_mut().ok_or(SupervisorBuildError::InvalidConfig(
            "a supervision root must be an ordered or dynamic scope",
        ))?;
        let id = NEXT_RESERVATION_ID.fetch_add(1, Ordering::Relaxed);
        scope.reservation = Some(id);
        let actors = Arc::new(ActorRuntimeState::new(
            scope.dynamic_builder.clone().unwrap_or_default(),
            scope.default_restart,
            scope.default_shutdown,
        ));
        let builder = match tree.kind().expect("validated scope root") {
            ScopeKind::Ordered => ReservedScopeBuilder::Ordered(Some(SupervisorBuilder::new())),
            ScopeKind::Dynamic => {
                ReservedScopeBuilder::Dynamic(Some(DynamicSupervisorBuilder::new()))
            }
            _ => unreachable!("tokio-otp constructs only ordered and dynamic scopes"),
        };
        let mut reserved = Self {
            tree,
            reservations: vec![ScopeReservation {
                id,
                builder,
                actors,
            }],
        };
        reserved.refresh_root();
        Ok(reserved)
    }

    fn root_reservation(&self) -> &ScopeReservation {
        let id = self
            .tree
            .scope()
            .and_then(|scope| scope.reservation)
            .expect("reserved tree root has a reservation");
        self.reservations
            .iter()
            .find(|reservation| reservation.id == id)
            .expect("reserved tree owns its root reservation")
    }

    fn refresh_root(&mut self) {
        let scope = self
            .tree
            .scope()
            .expect("reserved tree root remains a scope");
        let id = scope
            .reservation
            .expect("reserved tree root has a reservation");
        let strategy = scope.strategy;
        let default_restart = scope.default_restart;
        let default_shutdown = scope.default_shutdown;
        let actor_builder = scope.dynamic_builder.clone().unwrap_or_default();
        let child_ids = scope
            .children
            .iter()
            .filter_map(SupervisionTree::declared_id)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let reservation = self
            .reservations
            .iter_mut()
            .find(|reservation| reservation.id == id)
            .expect("reserved tree owns its root reservation");
        reservation
            .actors
            .configure(actor_builder, default_restart, default_shutdown);
        if let ReservedScopeBuilder::Ordered(builder) = &mut reservation.builder {
            let configured = builder
                .take()
                .expect("live reservation owns its ordered builder")
                .strategy(strategy);
            configured.project_declared_children(child_ids);
            *builder = Some(configured);
        }
    }

    fn map_tree(mut self, update: impl FnOnce(SupervisionTree) -> SupervisionTree) -> Self {
        self.tree = update(self.tree);
        self.refresh_root();
        self
    }

    /// Returns the stable actor-aware handle reserved for the root scope.
    pub fn handle(&self) -> crate::RuntimeHandle {
        let reservation = self.root_reservation();
        let supervisor = match &reservation.builder {
            ReservedScopeBuilder::Ordered(Some(builder)) => builder.handle(),
            ReservedScopeBuilder::Dynamic(Some(builder)) => builder.handle(),
            ReservedScopeBuilder::Ordered(None) | ReservedScopeBuilder::Dynamic(None) => {
                unreachable!("live reservation owns its scope builder")
            }
        };
        crate::RuntimeHandle::new(supervisor, Arc::clone(&reservation.actors))
    }

    /// Returns this scope's immutable kind.
    pub fn kind(&self) -> ScopeKind {
        self.tree.kind().expect("reserved tree root is a scope")
    }

    /// Sets the restart strategy of this scope.
    #[must_use]
    pub fn strategy(self, strategy: Strategy) -> Self {
        self.map_tree(|tree| tree.strategy(strategy))
    }

    /// Sets this scope's default restart intensity.
    #[must_use]
    pub fn restart_intensity(self, intensity: RestartIntensity) -> Self {
        self.map_tree(|tree| tree.restart_intensity(intensity))
    }

    /// Sets the restart policy inherited by actor nodes.
    #[must_use]
    pub fn default_restart(self, restart: RestartPolicy) -> Self {
        self.map_tree(|tree| tree.default_restart(restart))
    }

    /// Sets the shutdown policy inherited by actor nodes.
    #[must_use]
    pub fn default_shutdown(self, shutdown: ShutdownPolicy) -> Self {
        self.map_tree(|tree| tree.default_shutdown(shutdown))
    }

    /// Names this scope node for use as a child of another scope.
    #[must_use]
    pub fn id(self, id: impl Into<String>) -> Self {
        let id = id.into();
        self.map_tree(|tree| tree.id(id))
    }

    /// Appends an actor node to this scope.
    #[must_use]
    pub fn actor(self, actor: impl Into<ActorSpec>) -> Self {
        let actor = actor.into();
        self.map_tree(|tree| tree.actor(actor))
    }

    /// Records that generated supervision could not resolve an actor.
    #[doc(hidden)]
    #[must_use]
    pub fn missing_actor(self) -> Self {
        self.map_tree(SupervisionTree::missing_actor)
    }

    /// Appends an arbitrary task node to this scope.
    #[must_use]
    pub fn task(self, child: ChildSpec) -> Self {
        self.map_tree(|tree| tree.task(child))
    }

    /// Appends an unreserved recursive child node.
    #[must_use]
    pub fn child(self, child: SupervisionTree) -> Self {
        self.map_tree(|tree| tree.child(child))
    }

    /// Appends a child that already owns one or more reserved identities.
    #[doc(hidden)]
    #[must_use]
    pub fn reserved_child(mut self, child: ReservedSupervisionTree) -> Self {
        self.reservations.extend(child.reservations);
        self.tree = self.tree.child(child.tree);
        self.refresh_root();
        self
    }

    /// Appends a named unreserved nested scope.
    #[must_use]
    pub fn subtree(self, id: impl Into<String>, tree: SupervisionTree) -> Self {
        let id = id.into();
        self.map_tree(|root| root.subtree(id, tree))
    }

    /// Appends a named nested scope that already owns reserved identities.
    #[must_use]
    pub fn reserved_subtree(
        mut self,
        id: impl Into<String>,
        mut tree: ReservedSupervisionTree,
    ) -> Self {
        tree.tree = tree.tree.id(id);
        self.reservations.extend(tree.reservations);
        self.tree = self.tree.child(tree.tree);
        self.refresh_root();
        self
    }

    /// Appends an actor leader with an unreserved owned scope.
    #[must_use]
    pub fn actor_with_scope(
        self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: SupervisionTree,
        strategy: Strategy,
    ) -> Self {
        let id = id.into();
        let actor = actor.into();
        self.map_tree(|tree| tree.actor_with_scope(id, actor, children, strategy))
    }

    /// Appends an actor leader with an owned scope that has reserved identities.
    #[must_use]
    pub fn actor_with_reserved_scope(
        mut self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: ReservedSupervisionTree,
        strategy: Strategy,
    ) -> Self {
        self.reservations.extend(children.reservations);
        self.tree = self
            .tree
            .actor_with_scope(id, actor, children.tree, strategy);
        self.refresh_root();
        self
    }

    /// Projects the executable scope to comparable, payload-free data.
    pub fn outline(&self) -> Result<SupervisionOutline, SupervisorBuildError> {
        self.tree.outline()
    }

    /// Validates and lowers this reserved declaration to a runnable runtime.
    pub fn build(self) -> Result<Runtime, SupervisorBuildError> {
        let Self {
            tree,
            mut reservations,
        } = self;
        let (supervisor, actors) = tree.lower_scope(&mut reservations)?;
        if !reservations.is_empty() {
            return Err(SupervisorBuildError::InvalidConfig(
                "a reserved scope identity is not attached to its declaration",
            ));
        }
        Ok(Runtime::with_actor_tree(supervisor, actors))
    }

    /// Applies a graph's actor execution settings to this scope.
    #[doc(hidden)]
    #[must_use]
    pub fn derived_defaults(mut self, graph: &Graph) -> Self {
        self.tree.set_dynamic_builder(graph.dynamic_builder());
        self.refresh_root();
        self
    }

    pub(crate) fn replace_graph(mut self, graph: Graph) -> Self {
        let scope = self
            .tree
            .scope_mut()
            .expect("runtime builder owns a scope root");
        scope.children.clear();
        scope.dynamic_builder = Some(graph.dynamic_builder());
        scope.children.extend(
            graph
                .actors()
                .iter()
                .cloned()
                .map(ActorSpec::from)
                .map(SupervisionTree::Actor),
        );
        self.refresh_root();
        self
    }
}

impl std::fmt::Debug for ReservedSupervisionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.tree.fmt(f)
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
        /// Child id within the enclosing scope; equals the actor label unless
        /// [`ActorSpec::child_id`] overrode it.
        id: String,
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
            Self::Actor { id, .. }
            | Self::Child { id, .. }
            | Self::Scope { id, .. }
            | Self::ActorWithScope { id, .. } => id,
        }
    }
}
