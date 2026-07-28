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

/// Provides an application-defined size for a typed actor message.
///
/// The value is an observability hint, normally the size of payload buffers
/// owned by the message. It is sampled only when the target actor opts in via
/// [`ActorOptions::message_size`] when registering the actor.
pub trait MessageSize {
    /// Returns the message size to report, in bytes.
    fn size_hint(&self) -> usize;
}

/// Per-actor registration options.
///
/// Options compose independently, so an actor can use a non-default mailbox
/// and message-size observation together:
///
/// ```
/// use tokio_otp::{ActorOptions, MailboxMode, MessageSize};
///
/// struct Snapshot(Vec<u8>);
///
/// impl MessageSize for Snapshot {
///     fn size_hint(&self) -> usize {
///         self.0.len()
///     }
/// }
///
/// let options: ActorOptions<Snapshot> = ActorOptions::new()
///     .mailbox(MailboxMode::Conflate)
///     .message_size();
/// ```
pub struct ActorOptions<M> {
    pub(crate) mailbox_mode: MailboxMode<M>,
    pub(crate) size_hint: Option<fn(&M) -> usize>,
    pub(crate) mailbox_capacity: Option<usize>,
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
            mailbox_mode: MailboxMode::Queue,
            size_hint: None,
            mailbox_capacity: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.mailbox_capacity == Some(0) {
            return Err("actor mailbox capacity must be non-zero");
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

    /// Enables accepted-message byte observation using `M`'s size hint.
    pub fn message_size(mut self) -> Self
    where
        M: MessageSize,
    {
        self.size_hint = Some(MessageSize::size_hint);
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
/// Slots are created by [`GraphBuilder::slot`] and consumed by
/// [`GraphBuilder::define`]. The token is intentionally neither [`Clone`] nor
/// [`Copy`], so a slot can only be filled once in ordinary Rust code.
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
/// Registration mints restart-stable, typed [`ActorRef`]s with
/// [`slot`](Self::slot), then fills each slot with [`define`](Self::define).
/// Reserving refs before defining factories allows refs to be captured by
/// other actors' state, including when wiring cyclic graphs.
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
/// [`MailboxMode::Conflate`](crate::MailboxMode::Conflate) mailbox for lossy
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

    /// Opens a named slot and returns its fill token plus a restart-stable ref.
    ///
    /// This enables cyclic wiring: create all refs first, hand them to actor
    /// constructors, then consume each [`ActorSlot`] with [`define`](Self::define).
    /// The name is fixed when the slot is opened because it is used as the
    /// actor label in observability. `options` configures this actor's mailbox
    /// and message-size observation.
    pub fn slot<M: Send + 'static>(
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
            Err(message) => {
                self.errors.push(GraphBuildError::InvalidConfig(message));
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

    /// Fills a previously opened actor slot from a reusable incarnation factory.
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
            self.errors.push(GraphBuildError::InvalidConfig(
                "actor slot belongs to a different graph builder",
            ));
            return;
        }

        let Some(index) = index else {
            return;
        };
        let Some(slot) = self.slots.get_mut(index) else {
            self.errors
                .push(GraphBuildError::InvalidConfig("actor slot is detached"));
            return;
        };

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
                return Err(GraphBuildError::InvalidConfig(
                    "graph name must not be empty",
                ));
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
            return Err(GraphBuildError::InvalidConfig(
                "mailbox capacity must be non-zero",
            ));
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
            self.errors
                .push(GraphBuildError::InvalidConfig("actor id must not be empty"));
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
    use super::{ActorOptions, MailboxMode};

    struct OpaqueMessage;

    #[test]
    fn actor_options_clone_and_debug_do_not_bound_the_message_type() {
        let options: ActorOptions<OpaqueMessage> =
            ActorOptions::new().mailbox(MailboxMode::Conflate);

        let cloned = options.clone();
        assert_eq!(format!("{cloned:?}"), format!("{options:?}"));
    }
}
