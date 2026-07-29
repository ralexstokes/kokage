use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, OnceLock},
};

use kokage_supervisor::{RestartConfig, RestartPolicy, ShutdownPolicy, TerminalMembership};

use crate::actor::{
    binding::{BindingCore, BindingLifecycle, MailboxMode},
    context::ActorRef,
    error::GraphBuildError,
    factory::ActorFactory,
    graph::{
        ErasedActorFactory, ErasedRunner, Graph, RunnableActor, RunnableActorBuilder,
        RunnableActorParts,
    },
    observability::{GraphObservability, anonymous_graph_name},
    raw::RawActor,
};

/// Internal mailbox portion of the public [`ActorSpec`] vocabulary.
pub(crate) struct ActorOptions<M> {
    pub(crate) mailbox_mode: MailboxMode<M>,
    pub(crate) size_hint: Option<fn(&M) -> usize>,
    pub(crate) mailbox_capacity: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorOptionsValidationError {
    ZeroMailboxCapacity,
}

impl ActorOptionsValidationError {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::ZeroMailboxCapacity => "actor mailbox capacity must be non-zero",
        }
    }
}

impl<M> Clone for ActorOptions<M> {
    fn clone(&self) -> Self {
        Self {
            mailbox_mode: self.mailbox_mode.clone(),
            size_hint: self.size_hint,
            mailbox_capacity: self.mailbox_capacity,
        }
    }
}

impl<M> fmt::Debug for ActorOptions<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorOptions")
            .field("mailbox_mode", &self.mailbox_mode)
            .field("size_hint", &self.size_hint)
            .field("mailbox_capacity", &self.mailbox_capacity)
            .finish()
    }
}

impl<M> ActorOptions<M> {
    /// Creates options using a FIFO queue without message-size observation.
    pub fn new() -> Self {
        Self {
            mailbox_mode: MailboxMode::queue(),
            size_hint: None,
            mailbox_capacity: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ActorOptionsValidationError> {
        if self.mailbox_capacity == Some(0) {
            return Err(ActorOptionsValidationError::ZeroMailboxCapacity);
        }
        Ok(())
    }

    /// Overrides the hosting scope's mailbox capacity for this actor alone.
    ///
    /// Graph actors otherwise inherit [`GraphBuilder::mailbox_capacity`], while
    /// runtime-added actors inherit the hosting runtime scope's default. The
    /// value must be non-zero. It is the FIFO queue capacity and the maximum
    /// number of distinct unread keys for keyed conflation; unkeyed conflation
    /// always has capacity 1 and ignores it.
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }

    /// Selects the actor's mailbox storage policy.
    pub fn mailbox(mut self, mailbox_mode: MailboxMode<M>) -> Self {
        self.mailbox_mode = mailbox_mode;
        self
    }

    /// Enables accepted-message byte observation using `size_hint`.
    ///
    /// The function normally reports the size of payload buffers owned by the
    /// message. A bare function pointer keeps the option cheap to clone and
    /// permits foreign message types without an orphan-rule workaround.
    /// Non-capturing closures also coerce to the function pointer when their
    /// parameter type is explicit, for example
    /// `.message_size(|message: &Snapshot| message.0.len())`; closures that
    /// capture state are not accepted.
    pub fn message_size(mut self, size_hint: fn(&M) -> usize) -> Self {
        self.size_hint = Some(size_hint);
        self
    }
}

impl<M> Default for ActorOptions<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// One actor declaration, shared by graphs, supervision trees, and dynamic
/// insertion.
///
/// The declaration owns its incarnation factory, stable mailbox binding, and
/// all per-actor configuration. It is intentionally not [`Clone`]. Obtain a
/// restart-stable typed ref with [`actor_ref`](Self::actor_ref), then consume
/// the declaration through [`GraphBuilder::actor`],
/// [`crate::OrderedTree::actor`], or [`crate::DynamicRuntime::add_actor`].
/// Terminal memberships are retained by default in every destination. Select
/// [`TerminalMembership::Remove`] explicitly for an ephemeral dynamic actor.
pub struct ActorSpec<M: Send + 'static> {
    pub(crate) actor_id: Arc<str>,
    pub(crate) binding: OnceLock<Arc<BindingCore<M>>>,
    pub(crate) factory: Box<dyn ErasedActorFactory<M>>,
    pub(crate) actor_options: ActorOptions<M>,
    pub(crate) child_id: Option<String>,
    pub(crate) restart: Option<RestartPolicy>,
    pub(crate) shutdown: Option<ShutdownPolicy>,
    pub(crate) restart_config: Option<RestartConfig>,
    pub(crate) terminal_membership: TerminalMembership,
}

impl<M: Send + 'static> ActorSpec<M> {
    /// Creates a declaration for `factory` under `actor_id`.
    ///
    /// The declaration uses [`TerminalMembership::Retain`] unless changed
    /// with [`terminal_membership`](Self::terminal_membership).
    pub fn new<F>(actor_id: impl Into<String>, factory: F) -> Self
    where
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        Self {
            actor_id: Arc::from(actor_id.into()),
            binding: OnceLock::new(),
            factory: Box::new(factory),
            actor_options: ActorOptions::new(),
            child_id: None,
            restart: None,
            shutdown: None,
            restart_config: None,
            terminal_membership: TerminalMembership::Retain,
        }
    }

    /// Returns this declaration's restart-stable typed actor ref.
    pub fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::from_core(self.binding(), None)
    }

    pub(crate) fn binding(&self) -> &Arc<BindingCore<M>> {
        self.binding
            .get_or_init(|| match self.actor_options.size_hint {
                Some(size_hint) => Arc::new(BindingCore::with_message_size(
                    Arc::clone(&self.actor_id),
                    size_hint,
                )),
                None => Arc::new(BindingCore::new(Arc::clone(&self.actor_id))),
            })
    }

    /// Overrides the hosting scope's mailbox capacity for this actor.
    #[must_use]
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.actor_options = self.actor_options.mailbox_capacity(capacity);
        self
    }

    /// Selects the actor's mailbox storage policy.
    #[must_use]
    pub fn mailbox(mut self, mailbox: MailboxMode<M>) -> Self {
        self.actor_options = self.actor_options.mailbox(mailbox);
        self
    }

    /// Enables accepted-message byte observation.
    ///
    /// Configure this before calling [`actor_ref`](Self::actor_ref).
    #[must_use]
    pub fn message_size(mut self, size_hint: fn(&M) -> usize) -> Self {
        assert!(
            self.binding.get().is_none(),
            "message_size must be configured before ActorSpec::actor_ref"
        );
        self.actor_options = self.actor_options.message_size(size_hint);
        self
    }

    /// Overrides the enclosing scope's restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Overrides the enclosing scope's shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Gives this actor its own restart-intensity window.
    #[must_use]
    pub fn restart_config(mut self, config: RestartConfig) -> Self {
        self.restart_config = Some(config);
        self
    }

    /// Selects what happens to this child after a terminal exit.
    #[must_use]
    pub fn terminal_membership(mut self, membership: TerminalMembership) -> Self {
        self.terminal_membership = membership;
        self
    }

    // Generated supervision uses a scope-local id while retaining its
    // path-qualified actor label.
    #[doc(hidden)]
    #[must_use]
    pub fn child_id(mut self, id: impl Into<String>) -> Self {
        self.child_id = Some(id.into());
        self
    }

    pub(crate) fn into_node(self, builder: &RunnableActorBuilder) -> ActorNode {
        let Self {
            actor_id,
            binding,
            factory,
            actor_options,
            child_id,
            restart,
            shutdown,
            restart_config,
            terminal_membership,
        } = self;
        let binding = binding
            .into_inner()
            .unwrap_or_else(|| match actor_options.size_hint {
                Some(size_hint) => Arc::new(BindingCore::with_message_size(
                    Arc::clone(&actor_id),
                    size_hint,
                )),
                None => Arc::new(BindingCore::new(Arc::clone(&actor_id))),
            });
        let actor = builder.actor_from_parts(
            actor_id,
            binding,
            factory,
            actor_options.mailbox_mode,
            actor_options.mailbox_capacity,
        );
        ActorNode {
            actor,
            child_id,
            restart,
            shutdown,
            restart_config,
            terminal_membership,
        }
    }
}

impl<M: Send + 'static> fmt::Debug for ActorSpec<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSpec")
            .field("actor_id", &self.actor_id)
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .field("restart_config", &self.restart_config)
            .field("terminal_membership", &self.terminal_membership)
            .finish_non_exhaustive()
    }
}

/// A linear placement token for one materialized actor declaration.
///
/// `ActorNode` is intentionally not cloneable. Moving it into a supervision
/// tree makes duplicate placement unrepresentable. Custom actor hosts can
/// explicitly leave the supervision vocabulary with
/// [`into_runnable`](Self::into_runnable).
pub struct ActorNode {
    pub(crate) actor: RunnableActor,
    pub(crate) child_id: Option<String>,
    pub(crate) restart: Option<RestartPolicy>,
    pub(crate) shutdown: Option<ShutdownPolicy>,
    pub(crate) restart_config: Option<RestartConfig>,
    pub(crate) terminal_membership: TerminalMembership,
}

impl ActorNode {
    /// Returns the actor label carried by this placement token.
    pub fn label(&self) -> &str {
        self.actor.label()
    }

    /// Overrides the supervisor-local child id while retaining the graph-wide
    /// actor label. This is useful when placing a qualified graph actor in a
    /// nested supervision scope.
    #[must_use]
    pub fn child_id(mut self, id: impl Into<String>) -> Self {
        self.child_id = Some(id.into());
        self
    }

    /// Converts this placement token into the advanced custom-host actor.
    pub fn into_runnable(self) -> RunnableActor {
        self.actor
    }

    pub(crate) fn actor_label(&self) -> &str {
        self.label()
    }

    pub(crate) fn resolved_id(&self) -> &str {
        self.child_id
            .as_deref()
            .unwrap_or_else(|| self.actor.label())
    }
}

impl fmt::Debug for ActorNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorNode")
            .field("id", &self.resolved_id())
            .field("label", &self.actor_label())
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .field("restart_config", &self.restart_config)
            .field("terminal_membership", &self.terminal_membership)
            .finish()
    }
}

/// Unfilled actor declaration for cyclic graph wiring.
///
/// Create the slot and its ref before factories that close a cycle, then pass
/// the slot and factory to [`GraphBuilder::define`]. It has the same fluent
/// configuration vocabulary as [`ActorSpec`].
pub struct ActorSlot<M: Send + 'static> {
    actor_id: Arc<str>,
    binding: OnceLock<Arc<BindingCore<M>>>,
    actor_options: ActorOptions<M>,
    child_id: Option<String>,
    restart: Option<RestartPolicy>,
    shutdown: Option<ShutdownPolicy>,
    restart_config: Option<RestartConfig>,
    terminal_membership: TerminalMembership,
}

impl<M: Send + 'static> ActorSlot<M> {
    /// Opens an unfilled actor declaration.
    pub fn new(actor_id: impl Into<String>) -> Self {
        Self {
            actor_id: Arc::from(actor_id.into()),
            binding: OnceLock::new(),
            actor_options: ActorOptions::new(),
            child_id: None,
            restart: None,
            shutdown: None,
            restart_config: None,
            terminal_membership: TerminalMembership::Retain,
        }
    }

    /// Returns this slot's restart-stable typed actor ref.
    pub fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::from_core(self.binding(), None)
    }

    fn binding(&self) -> &Arc<BindingCore<M>> {
        self.binding
            .get_or_init(|| match self.actor_options.size_hint {
                Some(size_hint) => Arc::new(BindingCore::with_message_size(
                    Arc::clone(&self.actor_id),
                    size_hint,
                )),
                None => Arc::new(BindingCore::new(Arc::clone(&self.actor_id))),
            })
    }

    /// Overrides the graph's default mailbox capacity for this slot.
    #[must_use]
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.actor_options = self.actor_options.mailbox_capacity(capacity);
        self
    }

    /// Selects the slot's mailbox storage policy.
    #[must_use]
    pub fn mailbox(mut self, mailbox: MailboxMode<M>) -> Self {
        self.actor_options = self.actor_options.mailbox(mailbox);
        self
    }

    /// Enables accepted-message byte observation for this slot.
    #[must_use]
    pub fn message_size(mut self, size_hint: fn(&M) -> usize) -> Self {
        assert!(
            self.binding.get().is_none(),
            "message_size must be configured before ActorSlot::actor_ref"
        );
        self.actor_options = self.actor_options.message_size(size_hint);
        self
    }

    /// Overrides the enclosing scope's restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = Some(restart);
        self
    }
    /// Overrides the enclosing scope's shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = Some(shutdown);
        self
    }
    /// Gives this actor its own restart-intensity window.
    #[must_use]
    pub fn restart_config(mut self, config: RestartConfig) -> Self {
        self.restart_config = Some(config);
        self
    }
    /// Selects what happens to this child after a terminal exit.
    #[must_use]
    pub fn terminal_membership(mut self, membership: TerminalMembership) -> Self {
        self.terminal_membership = membership;
        self
    }

    fn into_spec<F>(self, factory: F) -> ActorSpec<M>
    where
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        ActorSpec {
            actor_id: self.actor_id,
            binding: self.binding,
            factory: Box::new(factory),
            actor_options: self.actor_options,
            child_id: self.child_id,
            restart: self.restart,
            shutdown: self.shutdown,
            restart_config: self.restart_config,
            terminal_membership: self.terminal_membership,
        }
    }
}

/// Builder for constructing a validated actor graph.
///
/// Register actors in dependency order with [`actor`](Self::actor). Each call installs the incarnation factory
/// and returns a restart-stable, typed [`ActorRef`].
///
/// Cyclic graphs need refs before every factory can be constructed. For those,
/// create independent [`ActorSlot`] values and their refs, then fill the slots
/// with [`define`](Self::define).
///
/// # Mailboxes and cycles
///
/// Mailboxes use bounded FIFO queues by default, with capacity configured by
/// [`mailbox_capacity`](Self::mailbox_capacity). Backpressure through
/// [`send`](ActorRef::send) can deadlock a cyclic graph: two actors that send
/// to each other while both queues are full wait forever, and a
/// [`call`](ActorRef::call) cycle deadlocks at depth one because the callee
/// cannot answer while the caller awaits the reply. Idioms: use
/// [`try_send`](ActorRef::try_send) on feedback edges, select a
/// [`MailboxMode::conflate`] mailbox for lossy
/// state snapshots, and call only "downhill" along a DAG ordering.
pub struct GraphBuilder {
    name: Option<String>,
    slots: Vec<Slot>,
    index: HashMap<Arc<str>, usize>,
    errors: Vec<GraphBuildError>,
    mailbox_capacity: usize,
}

struct Slot {
    actor_id: Arc<str>,
    binding_lifecycle: Arc<dyn BindingLifecycle>,
    runner: Option<Arc<dyn ErasedRunner>>,
    mailbox_capacity: Option<usize>,
    child_id: Option<String>,
    restart: Option<RestartPolicy>,
    shutdown: Option<ShutdownPolicy>,
    restart_config: Option<RestartConfig>,
    terminal_membership: TerminalMembership,
}

pub(crate) const DEFAULT_MAILBOX_CAPACITY: usize = 64;

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphBuilder {
    /// Creates a new builder with no actors and a default mailbox capacity of
    /// 64 messages per actor.
    pub fn new() -> Self {
        Self {
            name: None,
            slots: Vec::new(),
            index: HashMap::new(),
            errors: Vec::new(),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
        }
    }

    /// Sets the graph name used in tracing fields.
    ///
    /// If omitted, a stable anonymous name is generated during
    /// [`build`](Self::build).
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the bounded mailbox capacity used for every actor in the graph.
    ///
    /// This is the FIFO queue capacity and the maximum number of distinct
    /// unread keys for keyed conflation. Unkeyed conflation always has
    /// capacity 1 and ignores this setting. Individual actors can depart from
    /// this default with [`ActorSpec::mailbox_capacity`].
    pub fn mailbox_capacity(&mut self, capacity: usize) -> &mut Self {
        self.mailbox_capacity = capacity;
        self
    }

    /// Registers one complete actor declaration and returns its stable ref.
    ///
    /// Register actors in dependency order so each factory can capture refs
    /// returned by earlier calls. Use [`ActorSlot::new`] and
    /// [`define`](Self::define) instead when cyclic wiring requires a ref before
    /// its factory can be constructed.
    pub fn actor<M: Send + 'static>(&mut self, spec: ActorSpec<M>) -> ActorRef<M> {
        let actor_ref = spec.actor_ref();
        let options_validation = spec.actor_options.validate();
        let ActorSpec {
            actor_id,
            binding,
            factory,
            actor_options,
            child_id,
            restart,
            shutdown,
            restart_config,
            terminal_membership,
        } = spec;
        let binding = binding
            .into_inner()
            .expect("actor_ref initialized the declaration binding");
        if let Err(ActorOptionsValidationError::ZeroMailboxCapacity) = options_validation {
            self.errors.push(GraphBuildError::ZeroMailboxCapacity);
            return actor_ref;
        }
        let Some((index, _)) = self.push_slot_with_core(
            Arc::clone(&actor_id),
            Arc::clone(&binding),
            actor_options.mailbox_capacity,
        ) else {
            return actor_ref;
        };
        let slot = &mut self.slots[index];
        slot.runner = Some(factory.into_runner(binding, actor_options.mailbox_mode));
        slot.child_id = child_id;
        slot.restart = restart;
        slot.shutdown = shutdown;
        slot.restart_config = restart_config;
        slot.terminal_membership = terminal_membership;
        actor_ref
    }

    /// Fills and registers a cyclic-wiring slot.
    ///
    /// The slot token's message type must match the actor's message type, so a
    /// mismatched actor is rejected by the compiler. Consuming the token makes
    /// double fills unrepresentable in ordinary Rust code. See [`ActorFactory`]
    /// for the incarnation lifecycle contract.
    pub fn define<M, F>(&mut self, slot: ActorSlot<M>, factory: F) -> ActorRef<M>
    where
        M: Send + 'static,
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        self.actor(slot.into_spec(factory))
    }

    /// Validates the graph and returns an immutable [`Graph`].
    pub fn build(mut self) -> Result<Graph, GraphBuildError> {
        let graph_name = match self.name {
            Some(name) if name.is_empty() => {
                return Err(GraphBuildError::EmptyGraphName);
            }
            Some(name) => Arc::from(name),
            None => anonymous_graph_name(),
        };

        if !self.errors.is_empty() {
            return Err(self.errors.remove(0));
        }
        if self.slots.is_empty() {
            return Err(GraphBuildError::EmptyGraph);
        }
        if self.mailbox_capacity == 0 {
            return Err(GraphBuildError::ZeroMailboxCapacity);
        }

        let observability = GraphObservability::new(Arc::clone(&graph_name));
        let mut actors = Vec::with_capacity(self.slots.len());

        for slot in self.slots {
            let runner = slot
                .runner
                .unwrap_or_else(|| unreachable!("only complete declarations are registered"));
            let actor = RunnableActor::new(RunnableActorParts {
                actor_id: slot.actor_id,
                binding_lifecycle: slot.binding_lifecycle,
                runner,
                mailbox_capacity: slot.mailbox_capacity.unwrap_or(self.mailbox_capacity),
                observability: observability.clone(),
            });
            actors.push(ActorNode {
                actor,
                child_id: slot.child_id,
                restart: slot.restart,
                shutdown: slot.shutdown,
                restart_config: slot.restart_config,
                terminal_membership: slot.terminal_membership,
            });
        }

        Ok(Graph::new(
            graph_name,
            actors,
            observability,
            self.mailbox_capacity,
        ))
    }

    /// Spawns all declared actors in a flat ordered one-for-one scope.
    ///
    /// Use [`build`](Self::build) plus [`crate::OrderedTree`] when actors need
    /// a custom supervision topology.
    pub fn spawn(self) -> Result<crate::Runtime, crate::GraphSpawnError> {
        Ok(crate::OrderedTree::graph(self.build()?).spawn()?)
    }

    fn push_slot_with_core<M: Send + 'static>(
        &mut self,
        actor_id: Arc<str>,
        core: Arc<BindingCore<M>>,
        mailbox_capacity: Option<usize>,
    ) -> Option<(usize, ActorRef<M>)> {
        if actor_id.is_empty() {
            self.errors.push(GraphBuildError::EmptyActorId);
            return None;
        }

        if self.index.contains_key(actor_id.as_ref()) {
            self.errors.push(GraphBuildError::DuplicateActorId {
                actor_id: actor_id.to_string(),
            });
            return None;
        }

        let actor_ref = ActorRef::from_core(&core, None);
        let index = self.slots.len();
        self.index.insert(actor_id.clone(), index);
        self.slots.push(Slot {
            actor_id,
            binding_lifecycle: core,
            runner: None,
            mailbox_capacity,
            child_id: None,
            restart: None,
            shutdown: None,
            restart_config: None,
            terminal_membership: TerminalMembership::Retain,
        });
        Some((index, actor_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::{ActorSpec, GraphBuilder, MailboxMode};
    use crate::{Actor, ActorResult, MessageContext};

    struct OpaqueMessage;

    struct Snapshot(Vec<u8>);

    struct OpaqueActor;

    impl Actor for OpaqueActor {
        type Msg = OpaqueMessage;

        async fn handle(
            &mut self,
            _: OpaqueMessage,
            _: &mut MessageContext<'_, Self>,
        ) -> ActorResult {
            Ok(())
        }
    }

    #[test]
    fn actor_spec_debug_does_not_bound_the_message_type() {
        let spec: ActorSpec<OpaqueMessage> =
            ActorSpec::new("worker", || OpaqueActor).mailbox(MailboxMode::conflate());
        assert!(format!("{spec:?}").contains("worker"));
    }

    #[test]
    fn message_size_accepts_a_foreign_message_type() {
        let spec = ActorSpec::new("worker", || StringActor).message_size(String::len);
        let size_hint = spec
            .actor_options
            .size_hint
            .expect("message sizing is enabled");

        assert_eq!(size_hint(&"payload".to_owned()), 7);
    }

    #[test]
    fn message_size_accepts_an_explicitly_typed_non_capturing_closure() {
        let spec = ActorSpec::new("worker", || SnapshotActor)
            .message_size(|message: &Snapshot| message.0.len());
        let size_hint = spec
            .actor_options
            .size_hint
            .expect("message sizing is enabled");

        assert_eq!(size_hint(&Snapshot(vec![0; 7])), 7);
    }

    #[test]
    fn actor_spec_applies_options_and_returns_linear_node() {
        let mut builder = GraphBuilder::new();
        let actor_ref = builder
            .actor(ActorSpec::new("worker", || OpaqueActor).mailbox(MailboxMode::conflate()));
        let graph = builder.build().expect("graph builds");
        let mut nodes = graph.into_nodes();

        assert_eq!(actor_ref.id(), "worker");
        assert_eq!(nodes.remove(0).into_runnable().label(), "worker");
    }

    struct StringActor;
    impl Actor for StringActor {
        type Msg = String;
        async fn handle(&mut self, _: String, _: &mut MessageContext<'_, Self>) -> ActorResult {
            Ok(())
        }
    }

    struct SnapshotActor;
    impl Actor for SnapshotActor {
        type Msg = Snapshot;
        async fn handle(&mut self, _: Snapshot, _: &mut MessageContext<'_, Self>) -> ActorResult {
            Ok(())
        }
    }
}
