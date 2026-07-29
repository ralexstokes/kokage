use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::actor::{
    binding::{BindingCore, BindingLifecycle, MailboxMode},
    context::ActorRef,
    error::GraphBuildError,
    factory::ActorFactory,
    graph::{ErasedRunner, Graph, RunnableActor, RunnableActorParts, TypedRunner},
    observability::{GraphObservability, anonymous_graph_name},
    raw::RawActor,
};

/// Per-actor registration options.
///
/// Options compose independently, so an actor can use a non-default mailbox
/// and message-size observation together:
///
/// ```
/// use kokage::{ActorOptions, MailboxMode};
///
/// struct Snapshot(Vec<u8>);
///
/// fn snapshot_size(message: &Snapshot) -> usize {
///     message.0.len()
/// }
///
/// let options: ActorOptions<Snapshot> = ActorOptions::new()
///     .mailbox(MailboxMode::conflate())
///     .message_size(snapshot_size);
/// ```
pub struct ActorOptions<M> {
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

static NEXT_BUILDER_ID: AtomicU64 = AtomicU64::new(1);

/// Unfilled position for one actor in a graph builder.
///
/// Slots are the cyclic-wiring escape hatch: they are created by
/// [`GraphBuilder::slot`] or [`GraphBuilder::slot_with`] and consumed by
/// [`GraphBuilder::define`]. The token is intentionally neither [`Clone`] nor
/// [`Copy`], so a slot can only be filled once in ordinary Rust code. Use
/// [`GraphBuilder::actor`] or [`GraphBuilder::actor_with`] when the factory can
/// be defined as soon as its ref is created.
pub struct ActorSlot<M> {
    builder_id: u64,
    index: Option<usize>,
    core: Arc<BindingCore<M>>,
    mailbox_mode: MailboxMode<M>,
}

impl<M> ActorSlot<M> {
    fn new(
        builder_id: u64,
        index: Option<usize>,
        core: Arc<BindingCore<M>>,
        mailbox_mode: MailboxMode<M>,
    ) -> Self {
        Self {
            builder_id,
            index,
            core,
            mailbox_mode,
        }
    }
}

/// Builder for constructing a validated actor graph.
///
/// Register actors in dependency order with [`actor`](Self::actor) or
/// [`actor_with`](Self::actor_with). Each call installs the incarnation factory
/// and returns a restart-stable, typed [`ActorRef`].
///
/// Cyclic graphs need refs before every factory can be constructed. For those,
/// open the cycle's refs with [`slot`](Self::slot) or
/// [`slot_with`](Self::slot_with), then fill the slots with
/// [`define`](Self::define).
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
    builder_id: u64,
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
            builder_id: NEXT_BUILDER_ID.fetch_add(1, Ordering::Relaxed),
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
    /// this default with [`ActorOptions::mailbox_capacity`].
    pub fn mailbox_capacity(&mut self, capacity: usize) -> &mut Self {
        self.mailbox_capacity = capacity;
        self
    }

    /// Registers an actor with default [`ActorOptions`] and returns its
    /// restart-stable ref.
    ///
    /// Register actors in dependency order so each factory can capture refs
    /// returned by earlier calls. Use [`slot`](Self::slot) and
    /// [`define`](Self::define) instead when cyclic wiring requires a ref before
    /// its factory can be constructed.
    pub fn actor<F>(&mut self, actor_id: &str, factory: F) -> ActorRef<<F::Actor as RawActor>::Msg>
    where
        F: ActorFactory,
    {
        self.actor_with(actor_id, ActorOptions::new(), factory)
    }

    /// Registers an actor with explicit options and returns its restart-stable
    /// ref.
    ///
    /// `options` configures this actor's mailbox and message-size observation.
    /// Use [`slot_with`](Self::slot_with) and [`define`](Self::define) instead
    /// when cyclic wiring requires a ref before its factory can be constructed.
    pub fn actor_with<F>(
        &mut self,
        actor_id: &str,
        options: ActorOptions<<F::Actor as RawActor>::Msg>,
        factory: F,
    ) -> ActorRef<<F::Actor as RawActor>::Msg>
    where
        F: ActorFactory,
    {
        let (slot, actor_ref) = self.slot_with(actor_id, options);
        self.define(slot, factory);
        actor_ref
    }

    /// Opens a named slot for cyclic wiring with default [`ActorOptions`] and
    /// returns its fill token plus a restart-stable ref.
    ///
    /// See [`slot_with`](Self::slot_with) for cyclic-wiring order and explicit
    /// mailbox or message-size options.
    pub fn slot<M: Send + 'static>(&mut self, actor_id: &str) -> (ActorSlot<M>, ActorRef<M>) {
        self.slot_with(actor_id, ActorOptions::new())
    }

    /// Opens a named slot for cyclic wiring with explicit options and returns
    /// its fill token plus a restart-stable ref.
    ///
    /// This enables cyclic wiring: create all refs first, hand them to actor
    /// constructors, then consume each [`ActorSlot`] with [`define`](Self::define).
    /// The name is fixed when the slot is opened because it is used as the
    /// actor label in observability. `options` configures this actor's mailbox
    /// and message-size observation.
    pub fn slot_with<M: Send + 'static>(
        &mut self,
        actor_id: &str,
        options: ActorOptions<M>,
    ) -> (ActorSlot<M>, ActorRef<M>) {
        let options_validation = options.validate();
        let ActorOptions {
            mailbox_mode,
            size_hint,
            mailbox_capacity,
        } = options;
        let actor_id: Arc<str> = actor_id.into();
        let core = Arc::new(match size_hint {
            Some(size_hint) => {
                BindingCore::<M>::with_message_size(Arc::clone(&actor_id), size_hint)
            }
            None => BindingCore::<M>::new(Arc::clone(&actor_id)),
        });
        let registration = match options_validation {
            Ok(()) => {
                self.push_slot_with_core(Arc::clone(&actor_id), Arc::clone(&core), mailbox_capacity)
            }
            Err(ActorOptionsValidationError::ZeroMailboxCapacity) => {
                self.errors.push(GraphBuildError::ZeroMailboxCapacity);
                None
            }
        };
        let (index, actor_ref) = match registration {
            Some((index, actor_ref)) => (Some(index), actor_ref),
            None => (None, Self::detached_ref(&actor_id, size_hint)),
        };
        (
            ActorSlot::new(self.builder_id, index, core, mailbox_mode),
            actor_ref,
        )
    }

    /// Fills a cyclic-wiring slot from a reusable incarnation factory.
    ///
    /// The slot token's message type must match the actor's message type, so a
    /// mismatched actor is rejected by the compiler. Consuming the token makes
    /// double fills unrepresentable in ordinary Rust code. See [`ActorFactory`]
    /// for the incarnation lifecycle contract.
    pub fn define<F>(&mut self, slot: ActorSlot<<F::Actor as RawActor>::Msg>, factory: F)
    where
        F: ActorFactory,
    {
        let ActorSlot {
            builder_id,
            index,
            core,
            mailbox_mode,
        } = slot;
        if builder_id != self.builder_id {
            self.errors.push(GraphBuildError::ForeignSlot);
            return;
        }

        let Some(index) = index else {
            return;
        };
        let slot = self
            .slots
            .get_mut(index)
            .expect("actor slot index was registered by this builder");

        slot.runner = Some(Arc::new(TypedRunner {
            factory: Arc::new(factory),
            binding: core,
            mailbox_mode,
        }));
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
            let Some(runner) = slot.runner else {
                return Err(GraphBuildError::MissingActor {
                    actor_id: slot.actor_id.to_string(),
                });
            };
            actors.push(RunnableActor::new(RunnableActorParts {
                actor_id: slot.actor_id,
                binding_lifecycle: slot.binding_lifecycle,
                runner,
                mailbox_capacity: slot.mailbox_capacity.unwrap_or(self.mailbox_capacity),
                observability: observability.clone(),
            }));
        }

        Ok(Graph::new(
            graph_name,
            actors,
            observability,
            self.mailbox_capacity,
        ))
    }

    fn detached_ref<M>(actor_id: &str, size_hint: Option<fn(&M) -> usize>) -> ActorRef<M> {
        match size_hint {
            Some(size_hint) => ActorRef::detached_with_size_hint(actor_id.into(), size_hint),
            None => ActorRef::detached(actor_id.into()),
        }
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
        });
        Some((index, actor_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::{ActorOptions, GraphBuilder, MailboxMode};
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
    fn actor_options_clone_and_debug_do_not_bound_the_message_type() {
        let options: ActorOptions<OpaqueMessage> =
            ActorOptions::new().mailbox(MailboxMode::conflate());

        let cloned = options.clone();
        assert_eq!(format!("{cloned:?}"), format!("{options:?}"));
    }

    #[test]
    fn message_size_accepts_a_foreign_message_type() {
        let options = ActorOptions::<String>::new().message_size(String::len);
        let size_hint = options.size_hint.expect("message sizing is enabled");

        assert_eq!(size_hint(&"payload".to_owned()), 7);
    }

    #[test]
    fn message_size_accepts_an_explicitly_typed_non_capturing_closure() {
        let options =
            ActorOptions::<Snapshot>::new().message_size(|message: &Snapshot| message.0.len());
        let size_hint = options.size_hint.expect("message sizing is enabled");

        assert_eq!(size_hint(&Snapshot(vec![0; 7])), 7);
    }

    #[test]
    fn graph_actor_for_uses_binding_identity() {
        let mut first = GraphBuilder::new();
        let actor_ref = first.actor("worker", || OpaqueActor);
        let graph = first.build().expect("first graph builds");

        assert_eq!(
            graph
                .actor_for(&actor_ref)
                .expect("ref resolves in its graph")
                .label(),
            "worker"
        );
        assert!(graph.actor_for(&actor_ref.clone()).is_ok());

        let mut second = GraphBuilder::new();
        let foreign_ref = second.actor("worker", || OpaqueActor);
        second.build().expect("second graph builds");
        assert!(matches!(
            graph.actor_for(&foreign_ref),
            Err(crate::GraphLookupError::ForeignActorRef { actor_id, .. }) if actor_id == "worker"
        ));

        let detached = crate::ActorRef::<OpaqueMessage>::detached("worker".into());
        assert!(matches!(
            graph.actor_for(&detached),
            Err(crate::GraphLookupError::ForeignActorRef { actor_id, .. }) if actor_id == "worker"
        ));
    }

    #[test]
    fn actor_with_applies_options_and_returns_the_registered_ref() {
        let mut builder = GraphBuilder::new();
        let actor_ref = builder.actor_with(
            "worker",
            ActorOptions::new().mailbox(MailboxMode::conflate()),
            || OpaqueActor,
        );
        let graph = builder.build().expect("graph builds");

        assert_eq!(actor_ref.id(), "worker");
        assert!(graph.actor_for(&actor_ref).is_ok());
    }
}
