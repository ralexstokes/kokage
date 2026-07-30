//! Supervision trees expressed as opaque, inspectable recursive data.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::supervisor::{
    __private, BuildError, ChildSpec, DynamicSupervisorBuilder, OrderedSupervisorBuilder, Restart,
    ScopeKind, Shutdown, Strategy, Supervisor,
};

use crate::{
    DynamicRuntimeHandle, Runtime, RuntimeHandle,
    actor::{ActorNode, RunnableActorBuilder, SealedActorSpec},
    runtime::{ActorChildOptions, ActorRuntimeState, RuntimeAttachment, actor_child_spec},
};

#[derive(Clone)]
struct ScopeConfig {
    strategy: Strategy,
    default_restart: Restart,
    default_shutdown: Shutdown,
    mailbox_capacity: usize,
    reservation: Option<u64>,
}

impl ScopeConfig {
    fn new() -> Self {
        Self {
            strategy: Strategy::default(),
            default_restart: Restart::default(),
            default_shutdown: Shutdown::default(),
            mailbox_capacity: crate::actor::DEFAULT_MAILBOX_CAPACITY,
            reservation: None,
        }
    }
}

enum ScopeNode {
    Ordered {
        config: ScopeConfig,
        children: Vec<SupervisionChild>,
    },
    Dynamic {
        config: ScopeConfig,
    },
}

enum SupervisionChild {
    Actor(ActorNode),
    Task(ChildSpec),
    Scope {
        id: String,
        node: ScopeNode,
        restart: Option<Restart>,
        shutdown: Option<Shutdown>,
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

// Internal declaration data used while the public, identity-owning tree types
// are assembled. Public signatures intentionally do not expose this const
// generic implementation detail.
struct TreeData<const DYNAMIC: bool = false> {
    node: ScopeNode,
}

// Internal tree plus the identities reserved for its scopes.
struct IdentityTree<const DYNAMIC: bool = false> {
    tree: TreeData<DYNAMIC>,
    reservations: Vec<ScopeReservation>,
}

/// A single-use, identity-owning ordered supervision tree.
///
/// The tree reserves its stable runtime identity when it is created, so
/// [`handle`](Self::handle) is available before spawn. Moving a tree into a
/// parent transfers that same identity. Dropping an unspawned tree makes all
/// handles issued from it terminal.
///
/// Ordered scopes contain a declared child sequence. Declaration order controls
/// readiness-gated startup, reverse-order shutdown, and the suffix restarted
/// by [`Strategy::RestForOne`]. Use [`DynamicTree`] for an empty leaf whose
/// membership is written through a [`DynamicRuntimeHandle`] obtained before or
/// after spawn.
///
/// Actor declarations can be placed directly at different scope levels while
/// retaining their typed wiring.
///
/// [`outline`](Self::outline) removes executable payloads, producing a
/// [`SupervisionOutline`] that can be compared, debug-printed, and, with the
/// `serde` feature, serialized. It is the declaration-time companion to a
/// running [`SupervisorSnapshot`](crate::observe::SupervisorSnapshot).
///
/// # Example
///
/// ```
/// use kokage::{ActorSpec, OrderedTree, Restart, Strategy, prelude::*};
///
/// struct Worker;
///
/// impl Actor for Worker {
///     type Msg = ();
///
///     async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
///         Ok(())
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let ingest = ActorSpec::new("ingest", || Worker).restart(Restart::never());
/// let parse = ActorSpec::new("parse", || Worker);
///
/// let tree = OrderedTree::new()
///     .strategy(Strategy::RestForOne)
///     .actor(ingest)
///     .subtree(
///         "workers",
///         OrderedTree::new().actor(parse),
///     );
///
/// assert_eq!(tree.outline().child_ids(), ["ingest", "workers"]);
/// let runtime = tree.spawn()?;
/// runtime.shutdown_and_wait().await?;
/// # Ok(())
/// # }
/// ```
pub struct OrderedTree {
    inner: IdentityTree<false>,
}

/// A single-use, identity-owning dynamic supervision tree.
///
/// Dynamic trees begin empty and accept runtime membership through the
/// [`DynamicRuntimeHandle`] returned by [`handle`](Self::handle) before spawn
/// or by calling [`RuntimeHandle::dynamic`] on the spawned runtime's handle.
pub struct DynamicTree {
    inner: IdentityTree<true>,
}

/// Opaque ownership of either kind of supervision tree.
///
/// Public APIs use `impl Into<TreeNode>` so callers can pass an
/// [`OrderedTree`] or [`DynamicTree`] directly. `TreeNode` has no public
/// constructor or variants; this keeps the set of supported tree kinds
/// extensible without exposing the runtime's internal representation.
pub struct TreeNode {
    kind: TreeNodeKind,
    restart: Option<Restart>,
    shutdown: Option<Shutdown>,
}

enum TreeNodeKind {
    Ordered(OrderedTree),
    Dynamic(DynamicTree),
}

pub(crate) struct LoweredTreeNode {
    pub(crate) supervisor: Supervisor,
    pub(crate) actors: Arc<ActorRuntimeState>,
    pub(crate) restart: Option<Restart>,
    pub(crate) shutdown: Option<Shutdown>,
}

impl From<OrderedTree> for TreeNode {
    fn from(tree: OrderedTree) -> Self {
        Self {
            kind: TreeNodeKind::Ordered(tree),
            restart: None,
            shutdown: None,
        }
    }
}

impl From<DynamicTree> for TreeNode {
    fn from(tree: DynamicTree) -> Self {
        Self {
            kind: TreeNodeKind::Dynamic(tree),
            restart: None,
            shutdown: None,
        }
    }
}

impl TreeNode {
    /// Sets the policy used by the enclosing scope to restart this subtree.
    ///
    /// This configures the nested scope's edge in its parent. It is distinct
    /// from [`OrderedTree::default_restart`] and
    /// [`DynamicTree::default_restart`], which configure children inside the
    /// nested scope.
    #[must_use]
    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Sets the policy used by the enclosing scope to stop this subtree.
    ///
    /// This configures the nested scope's edge in its parent. It is distinct
    /// from [`OrderedTree::default_shutdown`] and
    /// [`DynamicTree::default_shutdown`], which configure children inside the
    /// nested scope.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    pub(crate) fn into_parts(self) -> Result<LoweredTreeNode, BuildError> {
        let Self {
            kind,
            restart,
            shutdown,
        } = self;
        let (supervisor, actors) = match kind {
            TreeNodeKind::Ordered(tree) => tree.into_parts(),
            TreeNodeKind::Dynamic(tree) => tree.into_parts(),
        }?;
        Ok(LoweredTreeNode {
            supervisor,
            actors,
            restart,
            shutdown,
        })
    }
}

macro_rules! tree_common_methods {
    () => {
        /// Sets the restart policy inherited by actors in this scope.
        #[must_use]
        pub fn default_restart(mut self, restart: Restart) -> Self {
            self.inner = self.inner.default_restart(restart);
            self
        }

        /// Sets the shutdown policy inherited by actors in this scope.
        #[must_use]
        pub fn default_shutdown(mut self, shutdown: Shutdown) -> Self {
            self.inner = self.inner.default_shutdown(shutdown);
            self
        }

        /// Sets the bounded mailbox capacity inherited by actors directly in this scope.
        ///
        /// The standard default is 64 messages per actor. Nested scopes do not
        /// inherit this value; configure each subtree explicitly when it needs
        /// a different default.
        ///
        /// This is the FIFO queue capacity and the maximum number of distinct
        /// unread keys for keyed conflation. Unkeyed conflation always has
        /// capacity 1 and ignores this setting. Individual actors can override
        /// it with
        /// [`ActorSpec::mailbox_capacity`](crate::ActorSpec::mailbox_capacity).
        /// The value is validated when the tree is spawned or dynamically
        /// inserted.
        #[must_use]
        pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
            self.inner = self.inner.mailbox_capacity(capacity);
            self
        }

        /// Projects the executable scope to comparable, payload-free data.
        pub fn outline(&self) -> SupervisionOutline {
            self.inner.outline()
        }

        pub(crate) fn into_parts(self) -> Result<(Supervisor, Arc<ActorRuntimeState>), BuildError> {
            self.inner.into_parts()
        }
    };
}

impl Default for OrderedTree {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderedTree {
    tree_common_methods!();

    /// Returns the stable actor-aware handle reserved for this root scope.
    pub fn handle(&self) -> RuntimeHandle {
        self.inner.handle()
    }

    /// Creates an empty ordered scope with standard runtime defaults.
    ///
    /// Empty ordered trees are valid and remain idle until shutdown. They are
    /// useful as a uniform root while configuration conditionally adds
    /// subtrees.
    pub fn new() -> Self {
        Self {
            inner: TreeData::new().with_identity(),
        }
    }

    /// Sets this scope's restart strategy.
    #[must_use]
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.inner = self.inner.strategy(strategy);
        self
    }

    /// Appends an actor declaration, sealed or still configurable.
    #[must_use]
    pub fn actor<M: Send + 'static>(mut self, actor: impl Into<SealedActorSpec<M>>) -> Self {
        self.inner = self.inner.actor(actor.into());
        self
    }

    /// Appends an arbitrary task node with its resolved policies.
    #[must_use]
    pub fn task(mut self, child: ChildSpec) -> Self {
        self.inner = self.inner.task(child);
        self
    }

    /// Appends a named ordered or dynamic nested scope.
    ///
    /// Pass `TreeNode::from(tree)` to override the restart or shutdown policy
    /// of the subtree's edge in this parent.
    #[must_use]
    pub fn subtree(mut self, id: impl Into<String>, tree: impl Into<TreeNode>) -> Self {
        let id = id.into();
        let TreeNode {
            kind,
            restart,
            shutdown,
        } = tree.into();
        self.inner = match kind {
            TreeNodeKind::Ordered(tree) => {
                self.inner.attach_subtree(id, tree.inner, restart, shutdown)
            }
            TreeNodeKind::Dynamic(tree) => {
                self.inner.attach_subtree(id, tree.inner, restart, shutdown)
            }
        };
        self
    }

    /// Builds and spawns this tree in the background.
    ///
    /// Retain the returned [`Runtime`] for as long as the runtime should remain
    /// alive. Dropping it requests graceful shutdown; its handles are non-owning.
    ///
    /// # Errors
    ///
    /// Invalid child ids, restart settings, or duplicate sibling ids return
    /// their corresponding build error. A failed spawn consumes the tree and
    /// makes every handle issued from it terminal. Having no children is
    /// valid and does not return an error.
    pub fn spawn(self) -> Result<Runtime, BuildError> {
        let (supervisor, actors) = self.inner.into_parts()?;
        Ok(Runtime::new(supervisor.spawn(), actors))
    }
}

impl Default for DynamicTree {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicTree {
    tree_common_methods!();

    /// Returns the stable, dynamic-membership-capable handle reserved for this
    /// root scope.
    pub fn handle(&self) -> DynamicRuntimeHandle {
        DynamicRuntimeHandle::new(self.inner.handle())
    }

    /// Creates an empty dynamic scope with standard runtime defaults.
    ///
    /// The spawned scope remains idle until actors, tasks, or subtrees are
    /// inserted through its dynamic handle.
    pub fn new() -> Self {
        Self {
            inner: TreeData::dynamic().with_identity(),
        }
    }

    /// Builds and spawns this tree in the background.
    ///
    /// Retain the returned [`Runtime`] for as long as the runtime should remain
    /// alive. Dropping it requests graceful shutdown; its handles are
    /// non-owning. Recover the root's dynamic capability after spawn with
    /// [`RuntimeHandle::dynamic`].
    ///
    /// # Errors
    ///
    /// Returns the applicable [`BuildError`] when the dynamic
    /// scope's restart configuration is invalid. A failed spawn consumes the
    /// tree and makes every handle issued from it terminal. An empty dynamic
    /// scope is valid and stays available for later insertion.
    pub fn spawn(self) -> Result<Runtime, BuildError> {
        let (supervisor, actors) = self.inner.into_parts()?;
        Ok(Runtime::new(supervisor.spawn(), actors))
    }
}

impl std::fmt::Debug for OrderedTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::fmt::Debug for DynamicTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl Default for TreeData<false> {
    fn default() -> Self {
        Self::new()
    }
}

impl TreeData<false> {
    /// Creates an empty ordered scope with standard runtime defaults.
    fn new() -> Self {
        Self {
            node: ScopeNode::Ordered {
                config: ScopeConfig::new(),
                children: Vec::new(),
            },
        }
    }

    /// Sets the restart strategy of this ordered scope.
    #[must_use]
    fn strategy(mut self, strategy: Strategy) -> Self {
        self.config_mut().strategy = strategy;
        self
    }

    /// Appends an actor node.
    #[must_use]
    fn actor<M: Send + 'static>(mut self, actor: SealedActorSpec<M>) -> Self {
        self.children_mut()
            .push(SupervisionChild::Actor(actor.into_deferred_node()));
        self
    }

    /// Appends an arbitrary task node.
    ///
    /// Explicit policies already set on `child` are preserved. Unset restart
    /// and shutdown policies inherit this scope's defaults during lowering.
    #[must_use]
    fn task(mut self, child: ChildSpec) -> Self {
        self.children_mut().push(SupervisionChild::Task(child));
        self
    }

    /// Appends a named ordered or dynamic nested scope.
    #[must_use]
    fn subtree<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        tree: TreeData<CHILD_DYNAMIC>,
        restart: Option<Restart>,
        shutdown: Option<Shutdown>,
    ) -> Self {
        self.children_mut().push(SupervisionChild::Scope {
            id: id.into(),
            node: tree.node,
            restart,
            shutdown,
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

impl TreeData<true> {
    /// Creates an empty dynamic scope.
    fn dynamic() -> Self {
        Self {
            node: ScopeNode::Dynamic {
                config: ScopeConfig::new(),
            },
        }
    }
}

impl<const DYNAMIC: bool> TreeData<DYNAMIC> {
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

    /// Sets the restart policy inherited by actor nodes.
    #[must_use]
    fn default_restart(mut self, restart: Restart) -> Self {
        self.config_mut().default_restart = restart;
        self
    }

    /// Sets the shutdown policy inherited by actor nodes.
    #[must_use]
    fn default_shutdown(mut self, shutdown: Shutdown) -> Self {
        self.config_mut().default_shutdown = shutdown;
        self
    }

    #[must_use]
    fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.config_mut().mailbox_capacity = capacity;
        self
    }

    fn with_identity(self) -> IdentityTree<DYNAMIC> {
        IdentityTree::new(self)
    }

    /// Projects the executable scope to comparable, payload-free data.
    fn outline(&self) -> SupervisionOutline {
        self.node.outline()
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
            children,
        }
    }

    fn lower(
        self,
        reservations: &mut Vec<ScopeReservation>,
    ) -> Result<(Supervisor, Arc<ActorRuntimeState>), BuildError> {
        let config = self.config().clone();
        if config.mailbox_capacity == 0 {
            return Err(BuildError::InvalidConfig(
                "actor mailbox capacity must be non-zero",
            ));
        }
        let actor_builder = RunnableActorBuilder::with_mailbox_capacity(config.mailbox_capacity);
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
                    .default_restart(config.default_restart)
                    .default_shutdown(config.default_shutdown);
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
                    .default_restart(config.default_restart)
                    .default_shutdown(config.default_shutdown);
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
            Self::Actor(actor) => actor.label(),
            Self::Task(child) => child.id(),
            Self::Scope { id, .. } => id,
        }
    }

    fn outline(&self, default_restart: Restart, default_shutdown: Shutdown) -> ChildOutline {
        match self {
            Self::Actor(actor) => ChildOutline::Actor {
                id: actor.label().to_owned(),
                restart: actor.restart.unwrap_or(default_restart),
                shutdown: actor.shutdown.unwrap_or(default_shutdown),
            },
            Self::Task(child) => {
                let (restart, shutdown) =
                    __private::child_policies(child, default_restart, default_shutdown);
                ChildOutline::Task {
                    id: child.id().to_owned(),
                    restart,
                    shutdown,
                }
            }
            Self::Scope {
                id,
                node,
                restart,
                shutdown,
            } => ChildOutline::Scope {
                id: id.clone(),
                restart: restart.unwrap_or(default_restart),
                shutdown: shutdown.unwrap_or(default_shutdown),
                outline: node.outline(),
            },
        }
    }

    fn lower(
        self,
        builder: OrderedSupervisorBuilder,
        actors: &Arc<ActorRuntimeState>,
        default_restart: Restart,
        default_shutdown: Shutdown,
        reservations: &mut Vec<ScopeReservation>,
    ) -> Result<OrderedSupervisorBuilder, BuildError> {
        Ok(match self {
            Self::Actor(actor) => {
                actor
                    .validate()
                    .map_err(|error| BuildError::InvalidConfig(error.message()))?;
                let ActorNode {
                    actor,
                    deferred: _,
                    restart,
                    shutdown,
                } = actors.materialize_actor_node(actor);
                builder.child(actor_child_spec(
                    actor.expect("tree lowering materialized the actor"),
                    actors,
                    ActorChildOptions::new(
                        restart.unwrap_or(default_restart),
                        shutdown.unwrap_or(default_shutdown),
                    ),
                ))
            }
            Self::Task(child) => builder.child(child),
            Self::Scope {
                id,
                node,
                restart,
                shutdown,
            } => {
                let (nested, nested_actors) = node.lower(reservations)?;
                let mut child = ChildSpec::supervisor(id, nested);
                if let Some(restart) = restart {
                    child = child.restart(restart);
                }
                if let Some(shutdown) = shutdown {
                    child = child.shutdown(shutdown);
                }
                builder.child(__private::attach(
                    child,
                    RuntimeAttachment::subtree(actors, nested_actors),
                ))
            }
        })
    }
}

static NEXT_RESERVATION_ID: AtomicU64 = AtomicU64::new(1);

impl<const DYNAMIC: bool> IdentityTree<DYNAMIC> {
    fn new(mut tree: TreeData<DYNAMIC>) -> Self {
        let id = NEXT_RESERVATION_ID.fetch_add(1, Ordering::Relaxed);
        tree.config_mut().reservation = Some(id);
        let actors = Arc::new(ActorRuntimeState::new(
            RunnableActorBuilder::with_mailbox_capacity(tree.config().mailbox_capacity),
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
        let declared_children = match &self.tree.node {
            ScopeNode::Ordered { children, .. } => children
                .iter()
                .map(|child| {
                    let restart = match child {
                        SupervisionChild::Actor(actor) => {
                            actor.restart.unwrap_or(config.default_restart)
                        }
                        SupervisionChild::Task(child) => {
                            __private::child_policies(
                                child,
                                config.default_restart,
                                config.default_shutdown,
                            )
                            .0
                        }
                        SupervisionChild::Scope { restart, .. } => {
                            restart.unwrap_or(config.default_restart)
                        }
                    };
                    (child.declared_id().to_owned(), restart)
                })
                .collect(),
            ScopeNode::Dynamic { .. } => Vec::new(),
        };
        let reservation = self
            .reservations
            .iter_mut()
            .find(|reservation| reservation.id == id)
            .expect("reserved tree owns its root reservation");
        reservation.actors.configure(
            RunnableActorBuilder::with_mailbox_capacity(config.mailbox_capacity),
            config.default_restart,
            config.default_shutdown,
        );
        if let ReservedScopeBuilder::Ordered(builder) = &mut reservation.builder {
            let configured = builder
                .take()
                .expect("live reservation owns its ordered builder")
                .strategy(config.strategy);
            configured.project_declared_children(declared_children);
            *builder = Some(configured);
        }
    }

    fn map_tree(mut self, update: impl FnOnce(TreeData<DYNAMIC>) -> TreeData<DYNAMIC>) -> Self {
        self.tree = update(self.tree);
        self.refresh_root();
        self
    }

    /// Returns the stable actor-aware handle reserved for this root scope.
    fn handle(&self) -> crate::RuntimeHandle {
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

    /// Sets the restart policy inherited by actor nodes.
    #[must_use]
    fn default_restart(self, restart: Restart) -> Self {
        self.map_tree(|tree| tree.default_restart(restart))
    }

    /// Sets the shutdown policy inherited by actor nodes.
    #[must_use]
    fn default_shutdown(self, shutdown: Shutdown) -> Self {
        self.map_tree(|tree| tree.default_shutdown(shutdown))
    }

    #[must_use]
    fn mailbox_capacity(self, capacity: usize) -> Self {
        self.map_tree(|tree| tree.mailbox_capacity(capacity))
    }

    /// Projects the executable scope to comparable, payload-free data.
    fn outline(&self) -> SupervisionOutline {
        self.tree.outline()
    }

    fn into_parts(self) -> Result<(Supervisor, Arc<ActorRuntimeState>), BuildError> {
        let Self {
            tree,
            mut reservations,
        } = self;
        let (supervisor, actors) = tree.node.lower(&mut reservations)?;
        debug_assert!(
            reservations.is_empty(),
            "every reservation is structurally attached to its declaration"
        );
        Ok((supervisor, actors))
    }
}

impl IdentityTree<false> {
    /// Sets the restart strategy of this ordered scope.
    #[must_use]
    fn strategy(self, strategy: Strategy) -> Self {
        self.map_tree(|tree| tree.strategy(strategy))
    }

    /// Appends an actor node.
    #[must_use]
    fn actor<M: Send + 'static>(self, actor: SealedActorSpec<M>) -> Self {
        self.map_tree(|tree| tree.actor(actor))
    }

    /// Appends an arbitrary task node with its resolved policies.
    ///
    /// Explicit policies on `child` survive lowering; unset policies inherit
    /// the enclosing scope defaults. See [`OrderedTree::task`].
    #[must_use]
    fn task(self, child: ChildSpec) -> Self {
        self.map_tree(|tree| tree.task(child))
    }

    /// Appends a named nested scope that already owns a reservation.
    #[must_use]
    fn attach_subtree<const CHILD_DYNAMIC: bool>(
        mut self,
        id: impl Into<String>,
        tree: IdentityTree<CHILD_DYNAMIC>,
        restart: Option<Restart>,
        shutdown: Option<Shutdown>,
    ) -> Self {
        self.reservations.extend(tree.reservations);
        self.tree = self.tree.subtree(id, tree.tree, restart, shutdown);
        self.refresh_root();
        self
    }
}

impl<const DYNAMIC: bool> std::fmt::Debug for IdentityTree<DYNAMIC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.tree.fmt(f)
    }
}

impl<const DYNAMIC: bool> std::fmt::Debug for TreeData<DYNAMIC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.outline().fmt(f)
    }
}

/// Payload-free declaration tree suitable for comparison and serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(from = "outline_wire::WireOutline")
)]
#[non_exhaustive]
pub struct SupervisionOutline {
    /// Immutable scope kind.
    pub kind: ScopeKind,
    /// Restart strategy.
    pub strategy: Strategy,
    /// Restart policy inherited by children without an explicit override.
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_restart: Restart,
    /// Shutdown policy inherited by children without an explicit override.
    #[cfg_attr(feature = "serde", serde(default))]
    pub default_shutdown: Shutdown,
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
        restart: Restart,
        /// Resolved shutdown policy.
        shutdown: Shutdown,
    },
    /// An arbitrary task child.
    Task {
        /// Child id.
        id: String,
        /// Restart policy.
        restart: Restart,
        /// Shutdown policy.
        shutdown: Shutdown,
    },
    /// A nested ordered or dynamic scope.
    Scope {
        /// Scope child id.
        id: String,
        /// Resolved policy used by the parent to restart this scope.
        #[cfg_attr(feature = "serde", serde(default))]
        restart: Restart,
        /// Resolved policy used by the parent to stop this scope.
        #[cfg_attr(feature = "serde", serde(default))]
        shutdown: Shutdown,
        /// Nested declaration.
        outline: SupervisionOutline,
    },
}

/// Deserialization mirror for [`SupervisionOutline`].
///
/// Scope-edge policies were added to the outline format after it first
/// shipped. An outline persisted without them carries no explicit edge
/// policy, which at declaration time meant "inherit the enclosing scope's
/// defaults" — so missing fields must resolve against the parent outline's
/// `default_restart` and `default_shutdown`, not the global defaults.
#[cfg(feature = "serde")]
mod outline_wire {
    use super::{ChildOutline, Restart, ScopeKind, Shutdown, Strategy, SupervisionOutline};

    #[derive(serde::Deserialize)]
    pub struct WireOutline {
        kind: ScopeKind,
        strategy: Strategy,
        #[serde(default)]
        default_restart: Restart,
        #[serde(default)]
        default_shutdown: Shutdown,
        children: Vec<WireChild>,
    }

    #[derive(serde::Deserialize)]
    enum WireChild {
        Actor {
            id: String,
            restart: Restart,
            shutdown: Shutdown,
        },
        Task {
            id: String,
            restart: Restart,
            shutdown: Shutdown,
        },
        Scope {
            id: String,
            #[serde(default)]
            restart: Option<Restart>,
            #[serde(default)]
            shutdown: Option<Shutdown>,
            outline: WireOutline,
        },
    }

    impl From<WireOutline> for SupervisionOutline {
        fn from(wire: WireOutline) -> Self {
            let WireOutline {
                kind,
                strategy,
                default_restart,
                default_shutdown,
                children,
            } = wire;
            let children = children
                .into_iter()
                .map(|child| match child {
                    WireChild::Actor {
                        id,
                        restart,
                        shutdown,
                    } => ChildOutline::Actor {
                        id,
                        restart,
                        shutdown,
                    },
                    WireChild::Task {
                        id,
                        restart,
                        shutdown,
                    } => ChildOutline::Task {
                        id,
                        restart,
                        shutdown,
                    },
                    WireChild::Scope {
                        id,
                        restart,
                        shutdown,
                        outline,
                    } => ChildOutline::Scope {
                        id,
                        restart: restart.unwrap_or(default_restart),
                        shutdown: shutdown.unwrap_or(default_shutdown),
                        outline: outline.into(),
                    },
                })
                .collect();
            Self {
                kind,
                strategy,
                default_restart,
                default_shutdown,
                children,
            }
        }
    }
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
            Self::Actor { id, .. } | Self::Task { id, .. } | Self::Scope { id, .. } => id,
        }
    }
}
