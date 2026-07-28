//! Supervision trees expressed as opaque, inspectable recursive data.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio_supervisor::{
    ChildSpec, DynamicSupervisorBuilder, OrderedSupervisorBuilder, RestartConfig, RestartPolicy,
    ScopeKind, ShutdownPolicy, Strategy, Supervisor, SupervisorBuildError,
};

use crate::{
    Graph, RunnableActor, Runtime,
    actor::RunnableActorBuilder,
    runtime::{ActorChildOptions, ActorRuntimeState, RuntimeAttachment, actor_child_spec},
};

#[derive(Clone)]
struct ScopeConfig {
    strategy: Strategy,
    restart_intensity: Option<RestartConfig>,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
    dynamic_builder: Option<RunnableActorBuilder>,
    reservation: Option<u64>,
}

impl ScopeConfig {
    fn new() -> Self {
        Self {
            strategy: Strategy::default(),
            restart_intensity: None,
            default_restart: RestartPolicy::default(),
            default_shutdown: ShutdownPolicy::default(),
            dynamic_builder: None,
            reservation: None,
        }
    }
}

#[derive(Clone)]
enum ScopeNode {
    Ordered {
        config: ScopeConfig,
        children: Vec<SupervisionChild>,
    },
    Dynamic {
        config: ScopeConfig,
    },
}

#[derive(Clone)]
enum SupervisionChild {
    Actor(ActorSpec),
    Task {
        child: ChildSpec,
        restart: RestartPolicy,
        shutdown: ShutdownPolicy,
    },
    Scope {
        id: String,
        node: ScopeNode,
    },
    ActorWithScope {
        id: String,
        actor: ActorSpec,
        children: ScopeNode,
        strategy: Strategy,
    },
}

enum ReservedScopeBuilder {
    Ordered(Option<OrderedSupervisorBuilder>),
    Dynamic(Option<DynamicSupervisorBuilder>),
}

struct ScopeReservation {
    id: u64,
    builder: ReservedScopeBuilder,
    actors: Arc<ActorRuntimeState>,
}

/// An opaque recursive supervision declaration.
///
/// This is the primary composition API. It remains cloneable declaration data
/// until [`reserve`](Self::reserve) is called or it is lowered directly with
/// [`build`](Self::build). It supports nested scopes, arbitrary task children,
/// per-actor policy overrides, and actor-owned scopes.
///
/// `SupervisionTree` is an ordered scope. `SupervisionTree<true>` is a dynamic
/// scope, created with [`SupervisionTree::dynamic`]. The const parameter makes
/// invalid operations structurally unavailable: dynamic trees have no
/// `strategy`, `actor`, `task`, or `subtree` methods.
///
/// A tree can place one graph's actors at different scope levels while
/// retaining the graph's typed wiring.
/// [`#[derive(Supervision)]`](crate::Supervision) is the static shorthand for
/// declaring both the graph and tree from one struct.
///
/// [`outline`](Self::outline) removes executable payloads, producing a
/// [`SupervisionOutline`] that can be compared, debug-printed, and, with the
/// `serde` feature, serialized. It is the declared companion to a running
/// [`SupervisorSnapshot`](tokio_supervisor::SupervisorSnapshot).
///
/// # Scope kinds and child order
///
/// Ordered scopes contain a declared child sequence. Its order controls
/// readiness-gated startup, reverse-order shutdown, and
/// [`Strategy::RestForOne`] restart scope. Dynamic scopes are empty leaves
/// whose membership is written through a [`RuntimeHandle`](crate::RuntimeHandle)
/// after spawn.
///
/// Call [`reserve`](Self::reserve) when a scope handle is needed before build.
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
///         Ok(())
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut graph = GraphBuilder::new();
/// let (ingest_slot, _ingest) = graph.slot("ingest");
/// graph.define(ingest_slot, || Worker);
/// let (parse_slot, _parse) = graph.slot("parse");
/// graph.define(parse_slot, || Worker);
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
/// assert_eq!(tree.outline().child_ids(), ["ingest", "workers"]);
/// let runtime = tree.build()?;
/// # drop(runtime);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SupervisionTree<const DYNAMIC: bool = false> {
    node: ScopeNode,
}

/// A supervision declaration that owns a pre-spawn scope identity.
///
/// This deliberately does not implement [`Clone`]. Dropping it makes its
/// reserved handle terminal. Like [`SupervisionTree`], the const parameter
/// distinguishes ordered and dynamic roots at compile time.
pub struct ReservedSupervisionTree<const DYNAMIC: bool = false> {
    tree: SupervisionTree<DYNAMIC>,
    reservations: Vec<ScopeReservation>,
}

/// A graph actor placed in a supervision tree with optional policy overrides.
#[derive(Clone)]
pub struct ActorSpec {
    actor: RunnableActor,
    child_id: Option<String>,
    restart: Option<RestartPolicy>,
    shutdown: Option<ShutdownPolicy>,
    restart_intensity: Option<RestartConfig>,
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
    /// scope uses a local id here so a graph label such as `workers.parse`
    /// appears under the `workers` scope as child `parse`, rather than
    /// repeating the scope name in the supervisor path.
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
    pub fn restart_intensity(mut self, intensity: RestartConfig) -> Self {
        self.restart_intensity = Some(intensity);
        self
    }

    fn actor_label(&self) -> &str {
        self.actor.label()
    }

    fn resolved_id(&self) -> &str {
        self.child_id
            .as_deref()
            .unwrap_or_else(|| self.actor_label())
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
            .field("id", &self.resolved_id())
            .field("label", &self.actor_label())
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .field("restart_intensity", &self.restart_intensity)
            .finish()
    }
}

impl Default for SupervisionTree<false> {
    fn default() -> Self {
        Self::new()
    }
}

impl SupervisionTree<false> {
    /// Creates an empty ordered scope with standard runtime defaults.
    pub fn new() -> Self {
        Self {
            node: ScopeNode::Ordered {
                config: ScopeConfig::new(),
                children: Vec::new(),
            },
        }
    }

    /// Creates an ordered scope containing every actor in a graph.
    pub fn graph(graph: &Graph) -> Self {
        let mut tree = Self::new().derived_defaults(graph);
        for actor in graph.actors() {
            tree = tree.actor(actor.clone());
        }
        tree
    }

    /// Sets the restart strategy of this ordered scope.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.config_mut().strategy = strategy;
        self
    }

    /// Appends an actor node.
    #[must_use]
    pub fn actor(mut self, actor: impl Into<ActorSpec>) -> Self {
        self.children_mut()
            .push(SupervisionChild::Actor(actor.into()));
        self
    }

    /// Appends an arbitrary task node with its resolved policies.
    ///
    /// `restart` and `shutdown` are the single source of truth for both the
    /// tree outline and the lowered runtime child. They deliberately replace
    /// values previously set through `ChildSpec::restart` or
    /// `ChildSpec::shutdown`; configure those two policies here instead.
    /// Other `ChildSpec` settings, including readiness and restart intensity,
    /// are preserved.
    #[must_use]
    pub fn task(
        mut self,
        child: ChildSpec,
        restart: RestartPolicy,
        shutdown: ShutdownPolicy,
    ) -> Self {
        self.children_mut().push(SupervisionChild::Task {
            child,
            restart,
            shutdown,
        });
        self
    }

    /// Appends a named ordered or dynamic nested scope.
    #[must_use]
    pub fn subtree<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        tree: SupervisionTree<CHILD_DYNAMIC>,
    ) -> Self {
        self.children_mut().push(SupervisionChild::Scope {
            id: id.into(),
            node: tree.node,
        });
        self
    }

    /// Appends an actor leader with a scope it owns.
    ///
    /// The generated ordered node installs the leader first and its owned
    /// scope second. `strategy` controls their failure relationship:
    /// [`Strategy::OneForOne`] restarts either independently,
    /// [`Strategy::OneForAll`] recycles both when either fails, and
    /// [`Strategy::RestForOne`] recycles the owned scope when the leader fails
    /// while an owned-scope failure leaves the leader running.
    /// The generated leader and `children` edges inherit this enclosing
    /// scope's restart and shutdown defaults unless the leader overrides them.
    #[must_use]
    pub fn actor_with_scope<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: SupervisionTree<CHILD_DYNAMIC>,
        strategy: Strategy,
    ) -> Self {
        self.children_mut().push(SupervisionChild::ActorWithScope {
            id: id.into(),
            actor: actor.into(),
            children: children.node,
            strategy,
        });
        self
    }

    fn children_mut(&mut self) -> &mut Vec<SupervisionChild> {
        match &mut self.node {
            ScopeNode::Ordered { children, .. } => children,
            ScopeNode::Dynamic { .. } => unreachable!("ordered tree has an ordered node"),
        }
    }
}

impl SupervisionTree<true> {
    /// Creates an empty dynamic scope.
    pub fn dynamic() -> Self {
        Self {
            node: ScopeNode::Dynamic {
                config: ScopeConfig::new(),
            },
        }
    }
}

impl<const DYNAMIC: bool> SupervisionTree<DYNAMIC> {
    fn config(&self) -> &ScopeConfig {
        match &self.node {
            ScopeNode::Ordered { config, .. } | ScopeNode::Dynamic { config } => config,
        }
    }

    fn config_mut(&mut self) -> &mut ScopeConfig {
        match &mut self.node {
            ScopeNode::Ordered { config, .. } | ScopeNode::Dynamic { config } => config,
        }
    }

    /// Sets this scope's default restart intensity.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartConfig) -> Self {
        self.config_mut().restart_intensity = Some(intensity);
        self
    }

    /// Sets the restart policy inherited by actor nodes.
    #[must_use]
    pub fn default_restart(mut self, restart: RestartPolicy) -> Self {
        self.config_mut().default_restart = restart;
        self
    }

    /// Sets the shutdown policy inherited by actor nodes.
    #[must_use]
    pub fn default_shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.config_mut().default_shutdown = shutdown;
        self
    }

    /// Reserves this root's stable identity. This operation is infallible.
    #[must_use = "dropping the reserved tree immediately terminalizes its scope identity"]
    pub fn reserve(self) -> ReservedSupervisionTree<DYNAMIC> {
        ReservedSupervisionTree::new(self)
    }

    /// Projects the executable scope to comparable, payload-free data.
    pub fn outline(&self) -> SupervisionOutline {
        self.node.outline()
    }

    /// Lowers this declaration to a runnable actor runtime.
    pub fn build(self) -> Result<Runtime, SupervisorBuildError> {
        let (supervisor, actors) = self.node.lower(&mut Vec::new())?;
        Ok(Runtime::with_actor_tree(supervisor, actors))
    }

    /// Applies a graph's dynamic actor construction settings to this scope.
    #[doc(hidden)]
    #[must_use]
    pub fn derived_defaults(mut self, graph: &Graph) -> Self {
        self.config_mut().dynamic_builder = Some(graph.dynamic_builder());
        self
    }
}

impl ScopeNode {
    fn config(&self) -> &ScopeConfig {
        match self {
            Self::Ordered { config, .. } | Self::Dynamic { config } => config,
        }
    }

    fn kind(&self) -> ScopeKind {
        match self {
            Self::Ordered { .. } => ScopeKind::Ordered,
            Self::Dynamic { .. } => ScopeKind::Dynamic,
        }
    }

    fn outline(&self) -> SupervisionOutline {
        let config = self.config();
        let children = match self {
            Self::Ordered { children, .. } => children
                .iter()
                .map(|child| child.outline(config.default_restart, config.default_shutdown))
                .collect(),
            Self::Dynamic { .. } => Vec::new(),
        };
        SupervisionOutline {
            kind: self.kind(),
            strategy: config.strategy,
            default_restart: config.default_restart,
            default_shutdown: config.default_shutdown,
            restart_intensity: config.restart_intensity.unwrap_or_default(),
            children,
        }
    }

    fn lower(
        self,
        reservations: &mut Vec<ScopeReservation>,
    ) -> Result<(Supervisor, Arc<ActorRuntimeState>), SupervisorBuildError> {
        let config = self.config().clone();
        let actor_builder = config.dynamic_builder.clone().unwrap_or_default();
        let reservation = config.reservation.and_then(|id| {
            reservations
                .iter()
                .position(|reservation| reservation.id == id)
                .map(|index| reservations.swap_remove(index))
        });
        let actors = reservation.as_ref().map_or_else(
            || {
                Arc::new(ActorRuntimeState::new(
                    actor_builder.clone(),
                    config.default_restart,
                    config.default_shutdown,
                ))
            },
            |reservation| Arc::clone(&reservation.actors),
        );
        actors.configure(
            actor_builder,
            config.default_restart,
            config.default_shutdown,
        );

        match self {
            Self::Dynamic { .. } => {
                let mut builder = match reservation.map(|reservation| reservation.builder) {
                    Some(ReservedScopeBuilder::Dynamic(mut builder)) => builder
                        .take()
                        .expect("live reservation owns its dynamic builder"),
                    Some(ReservedScopeBuilder::Ordered(_)) => unreachable!("scope kind matches"),
                    None => Supervisor::dynamic(),
                };
                builder = builder
                    .restart(config.default_restart)
                    .shutdown(config.default_shutdown);
                if let Some(intensity) = config.restart_intensity {
                    builder = builder.restart_intensity(intensity);
                }
                Ok((builder.build()?, actors))
            }
            Self::Ordered { children, .. } => {
                let builder = match reservation.map(|reservation| reservation.builder) {
                    Some(ReservedScopeBuilder::Ordered(mut builder)) => builder
                        .take()
                        .expect("live reservation owns its ordered builder"),
                    Some(ReservedScopeBuilder::Dynamic(_)) => unreachable!("scope kind matches"),
                    None => Supervisor::ordered(),
                };
                let mut builder = builder
                    .strategy(config.strategy)
                    .restart(config.default_restart)
                    .shutdown(config.default_shutdown);
                if let Some(intensity) = config.restart_intensity {
                    builder = builder.restart_intensity(intensity);
                }
                for child in children {
                    builder = child.lower(
                        builder,
                        &actors,
                        config.default_restart,
                        config.default_shutdown,
                        reservations,
                    )?;
                }
                Ok((builder.build()?, actors))
            }
        }
    }
}

impl SupervisionChild {
    fn declared_id(&self) -> &str {
        match self {
            Self::Actor(actor) => actor.resolved_id(),
            Self::Task { child, .. } => child.id(),
            Self::Scope { id, .. } | Self::ActorWithScope { id, .. } => id,
        }
    }

    fn outline(
        &self,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) -> ChildOutline {
        match self {
            Self::Actor(actor) => ChildOutline::Actor {
                id: actor.resolved_id().to_owned(),
                restart: actor.restart.unwrap_or(default_restart),
                shutdown: actor.shutdown.unwrap_or(default_shutdown),
                restart_intensity: actor.restart_intensity,
            },
            Self::Task {
                child,
                restart,
                shutdown,
            } => ChildOutline::Task {
                id: child.id().to_owned(),
                restart: *restart,
                shutdown: *shutdown,
            },
            Self::Scope { id, node } => ChildOutline::Scope {
                id: id.clone(),
                outline: node.outline(),
            },
            Self::ActorWithScope {
                id,
                actor,
                children,
                strategy,
            } => ChildOutline::ActorWithScope {
                id: id.clone(),
                leader: Box::new(ChildOutline::Actor {
                    id: actor.resolved_id().to_owned(),
                    restart: actor.restart.unwrap_or(default_restart),
                    shutdown: actor.shutdown.unwrap_or(default_shutdown),
                    restart_intensity: actor.restart_intensity,
                }),
                children: Box::new(children.outline()),
                strategy: *strategy,
            },
        }
    }

    fn lower(
        self,
        builder: OrderedSupervisorBuilder,
        actors: &Arc<ActorRuntimeState>,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
        reservations: &mut Vec<ScopeReservation>,
    ) -> Result<OrderedSupervisorBuilder, SupervisorBuildError> {
        Ok(match self {
            Self::Actor(ActorSpec {
                actor,
                child_id,
                restart,
                shutdown,
                restart_intensity,
            }) => builder.child(actor_child_spec(
                actor,
                actors,
                ActorChildOptions::new(
                    restart.unwrap_or(default_restart),
                    shutdown.unwrap_or(default_shutdown),
                )
                .restart_intensity(restart_intensity)
                .child_id(child_id),
            )),
            Self::Task {
                child,
                restart,
                shutdown,
            } => builder.child(child.restart(restart).shutdown(shutdown)),
            Self::Scope { id, node } => {
                let (nested, nested_actors) = node.lower(reservations)?;
                builder.child(
                    ChildSpec::supervisor(id, nested)
                        .attachment(RuntimeAttachment::subtree(actors, nested_actors)),
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
                    default_restart,
                    default_shutdown,
                ));
                let (children_supervisor, children_actors) = children.lower(reservations)?;
                let children_handle = crate::RuntimeHandle::new(
                    children_supervisor.handle(),
                    Arc::clone(&children_actors),
                );
                let leader = actor_child_spec(
                    actor,
                    &owned_actors,
                    ActorChildOptions::new(
                        restart.unwrap_or(default_restart),
                        shutdown.unwrap_or(default_shutdown),
                    )
                    .restart_intensity(restart_intensity)
                    .child_id(child_id)
                    .children(children_handle),
                );
                let owned = Supervisor::ordered()
                    .strategy(strategy)
                    .restart(default_restart)
                    .shutdown(default_shutdown)
                    .child(leader)
                    .child(
                        ChildSpec::supervisor("children", children_supervisor)
                            .attachment(RuntimeAttachment::subtree(&owned_actors, children_actors)),
                    )
                    .build()?;
                builder.child(
                    ChildSpec::supervisor(id, owned)
                        .attachment(RuntimeAttachment::subtree(actors, owned_actors)),
                )
            }
        })
    }
}

static NEXT_RESERVATION_ID: AtomicU64 = AtomicU64::new(1);

impl<const DYNAMIC: bool> ReservedSupervisionTree<DYNAMIC> {
    fn new(mut tree: SupervisionTree<DYNAMIC>) -> Self {
        let id = NEXT_RESERVATION_ID.fetch_add(1, Ordering::Relaxed);
        tree.config_mut().reservation = Some(id);
        let actors = Arc::new(ActorRuntimeState::new(
            tree.config().dynamic_builder.clone().unwrap_or_default(),
            tree.config().default_restart,
            tree.config().default_shutdown,
        ));
        let builder = match tree.node {
            ScopeNode::Ordered { .. } => ReservedScopeBuilder::Ordered(Some(Supervisor::ordered())),
            ScopeNode::Dynamic { .. } => ReservedScopeBuilder::Dynamic(Some(Supervisor::dynamic())),
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
        reserved
    }

    fn root_reservation(&self) -> &ScopeReservation {
        let id = self
            .tree
            .config()
            .reservation
            .expect("reserved tree root has a reservation");
        self.reservations
            .iter()
            .find(|reservation| reservation.id == id)
            .expect("reserved tree owns its root reservation")
    }

    fn refresh_root(&mut self) {
        let config = self.tree.config().clone();
        let id = config
            .reservation
            .expect("reserved tree root has a reservation");
        let child_ids = match &self.tree.node {
            ScopeNode::Ordered { children, .. } => children
                .iter()
                .map(|child| child.declared_id().to_owned())
                .collect(),
            ScopeNode::Dynamic { .. } => Vec::new(),
        };
        let reservation = self
            .reservations
            .iter_mut()
            .find(|reservation| reservation.id == id)
            .expect("reserved tree owns its root reservation");
        reservation.actors.configure(
            config.dynamic_builder.unwrap_or_default(),
            config.default_restart,
            config.default_shutdown,
        );
        if let ReservedScopeBuilder::Ordered(builder) = &mut reservation.builder {
            let configured = builder
                .take()
                .expect("live reservation owns its ordered builder")
                .strategy(config.strategy);
            configured.project_declared_children(child_ids);
            *builder = Some(configured);
        }
    }

    fn map_tree(
        mut self,
        update: impl FnOnce(SupervisionTree<DYNAMIC>) -> SupervisionTree<DYNAMIC>,
    ) -> Self {
        self.tree = update(self.tree);
        self.refresh_root();
        self
    }

    /// Returns the stable actor-aware handle reserved for this root scope.
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

    /// Sets this scope's default restart intensity.
    #[must_use]
    pub fn restart_intensity(self, intensity: RestartConfig) -> Self {
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

    /// Projects the executable scope to comparable, payload-free data.
    pub fn outline(&self) -> SupervisionOutline {
        self.tree.outline()
    }

    /// Lowers this reserved declaration to a runnable actor runtime.
    pub fn build(self) -> Result<Runtime, SupervisorBuildError> {
        let Self {
            tree,
            mut reservations,
        } = self;
        let (supervisor, actors) = tree.node.lower(&mut reservations)?;
        debug_assert!(
            reservations.is_empty(),
            "every reservation is structurally attached to its declaration"
        );
        Ok(Runtime::with_actor_tree(supervisor, actors))
    }

    /// Applies a graph's dynamic actor construction settings to this scope.
    #[doc(hidden)]
    #[must_use]
    pub fn derived_defaults(self, graph: &Graph) -> Self {
        self.map_tree(|tree| tree.derived_defaults(graph))
    }
}

impl ReservedSupervisionTree<false> {
    /// Sets the restart strategy of this ordered scope.
    #[must_use]
    pub fn strategy(self, strategy: Strategy) -> Self {
        self.map_tree(|tree| tree.strategy(strategy))
    }

    /// Appends an actor node.
    #[must_use]
    pub fn actor(self, actor: impl Into<ActorSpec>) -> Self {
        let actor = actor.into();
        self.map_tree(|tree| tree.actor(actor))
    }

    /// Appends an arbitrary task node with its resolved policies.
    ///
    /// The supplied policies are applied to `child` during lowering. See
    /// [`SupervisionTree::task`].
    #[must_use]
    pub fn task(self, child: ChildSpec, restart: RestartPolicy, shutdown: ShutdownPolicy) -> Self {
        self.map_tree(|tree| tree.task(child, restart, shutdown))
    }

    /// Appends a named unreserved nested scope.
    #[must_use]
    pub fn subtree<const CHILD_DYNAMIC: bool>(
        self,
        id: impl Into<String>,
        tree: SupervisionTree<CHILD_DYNAMIC>,
    ) -> Self {
        let id = id.into();
        self.map_tree(|root| root.subtree(id, tree))
    }

    /// Appends a named nested scope that already owns a reservation.
    #[must_use]
    pub fn reserved_subtree<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        tree: ReservedSupervisionTree<CHILD_DYNAMIC>,
    ) -> Self {
        self.reservations.extend(tree.reservations);
        self.tree = self.tree.subtree(id, tree.tree);
        self.refresh_root();
        self
    }

    /// Appends an actor leader with an unreserved owned scope.
    ///
    /// See [`SupervisionTree::actor_with_scope`] for the owned node's ordering
    /// and restart-strategy semantics.
    #[must_use]
    pub fn actor_with_scope<const CHILD_DYNAMIC: bool>(
        self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: SupervisionTree<CHILD_DYNAMIC>,
        strategy: Strategy,
    ) -> Self {
        let id = id.into();
        let actor = actor.into();
        self.map_tree(|tree| tree.actor_with_scope(id, actor, children, strategy))
    }

    /// Appends an actor leader with an owned scope that is already reserved.
    ///
    /// See [`SupervisionTree::actor_with_scope`] for the owned node's ordering
    /// and restart-strategy semantics.
    #[must_use]
    pub fn actor_with_reserved_scope<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        actor: impl Into<ActorSpec>,
        children: ReservedSupervisionTree<CHILD_DYNAMIC>,
        strategy: Strategy,
    ) -> Self {
        self.reservations.extend(children.reservations);
        self.tree = self
            .tree
            .actor_with_scope(id, actor, children.tree, strategy);
        self.refresh_root();
        self
    }
}

impl<const DYNAMIC: bool> std::fmt::Debug for ReservedSupervisionTree<DYNAMIC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.tree.fmt(f)
    }
}

impl<const DYNAMIC: bool> std::fmt::Debug for SupervisionTree<DYNAMIC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.outline().fmt(f)
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
    ///
    /// The serialized field deliberately keeps the behavior name
    /// `restart_intensity`; [`RestartConfig`] names its combined budget and
    /// backoff value.
    pub restart_intensity: RestartConfig,
    /// Declared children in semantic order; empty for a dynamic scope.
    pub children: Vec<ChildOutline>,
}

/// One payload-free child declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ChildOutline {
    /// An actor with resolved policies.
    Actor {
        /// Child id within the enclosing scope.
        id: String,
        /// Resolved restart policy.
        restart: RestartPolicy,
        /// Resolved shutdown policy.
        shutdown: ShutdownPolicy,
        /// Optional child-specific intensity policy.
        restart_intensity: Option<RestartConfig>,
    },
    /// An arbitrary task child.
    Task {
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
            | Self::Task { id, .. }
            | Self::Scope { id, .. }
            | Self::ActorWithScope { id, .. } => id,
        }
    }
}
