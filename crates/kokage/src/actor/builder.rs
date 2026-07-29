use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use kokage_supervisor::{Restart, Shutdown};

use crate::actor::{
    binding::{BindingCore, MailboxMode},
    context::ActorRef,
    factory::ActorFactory,
    graph::{ErasedActorFactory, RunnableActor, RunnableActorBuilder},
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

trait DeferredActorFactory: Send {
    fn label(&self) -> &str;

    fn validate(&self) -> Result<(), ActorOptionsValidationError>;

    fn materialize(self: Box<Self>, builder: &RunnableActorBuilder) -> RunnableActor;
}

struct DeferredActorSpec<M: Send + 'static> {
    actor_id: Arc<str>,
    binding: OnceLock<Arc<BindingCore<M>>>,
    factory: Box<dyn ErasedActorFactory<M>>,
    actor_options: ActorOptions<M>,
}

impl<M: Send + 'static> DeferredActorFactory for DeferredActorSpec<M> {
    fn label(&self) -> &str {
        &self.actor_id
    }

    fn validate(&self) -> Result<(), ActorOptionsValidationError> {
        self.actor_options.validate()
    }

    fn materialize(self: Box<Self>, builder: &RunnableActorBuilder) -> RunnableActor {
        let Self {
            actor_id,
            binding,
            factory,
            actor_options,
        } = *self;
        let binding = binding
            .into_inner()
            .unwrap_or_else(|| match actor_options.size_hint {
                Some(size_hint) => Arc::new(BindingCore::with_message_size(
                    Arc::clone(&actor_id),
                    size_hint,
                )),
                None => Arc::new(BindingCore::new(Arc::clone(&actor_id))),
            });
        builder.actor_from_parts(
            actor_id,
            binding,
            factory,
            actor_options.mailbox_mode,
            actor_options.mailbox_capacity,
        )
    }
}

pub(crate) struct DeferredActor(Box<dyn DeferredActorFactory>);

impl DeferredActor {
    fn label(&self) -> &str {
        self.0.label()
    }

    fn validate(&self) -> Result<(), ActorOptionsValidationError> {
        self.0.validate()
    }

    fn materialize(self, builder: &RunnableActorBuilder) -> RunnableActor {
        self.0.materialize(builder)
    }
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
    /// Actors otherwise inherit the hosting runtime scope's default. The value
    /// must be non-zero. It is the FIFO queue capacity and the maximum
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

/// One actor declaration, shared by supervision trees and dynamic insertion.
///
/// The declaration owns its incarnation factory, stable mailbox binding, and
/// all per-actor configuration. It is intentionally not [`Clone`]. Obtain a
/// restart-stable typed ref with [`actor_ref`](Self::actor_ref), then consume
/// the declaration through [`crate::OrderedTree::actor`] or
/// [`crate::DynamicRuntimeHandle::add_actor`].
///
/// `CONFIGURABLE` is `true` until [`actor_ref`](Self::actor_ref) mints the
/// stable binding. That transition returns [`SealedActorSpec`], whose mailbox
/// configuration is frozen. Most code can omit the parameter and use the
/// default configurable state.
pub struct ActorSpec<M: Send + 'static, const CONFIGURABLE: bool = true> {
    pub(crate) actor_id: Arc<str>,
    pub(crate) binding: OnceLock<Arc<BindingCore<M>>>,
    pub(crate) factory: Box<dyn ErasedActorFactory<M>>,
    pub(crate) actor_options: ActorOptions<M>,
    pub(crate) restart: Option<Restart>,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<M: Send + 'static> ActorSpec<M, true> {
    /// Creates a declaration for `factory` under `actor_id`.
    ///
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
            restart: None,
            shutdown: None,
        }
    }

    /// Mints this declaration's restart-stable typed ref and seals mailbox
    /// configuration.
    ///
    /// The returned declaration can be placed in a tree, but no longer has
    /// `mailbox`, `mailbox_capacity`, or `message_size` methods. Configure the
    /// declaration first, then destructure this result.
    pub fn actor_ref(self) -> (SealedActorSpec<M>, ActorRef<M>) {
        let binding = Arc::clone(self.binding());
        let actor_ref = ActorRef::from_core(&binding, None);
        let Self {
            actor_id,
            binding,
            factory,
            actor_options,
            restart,
            shutdown,
        } = self;
        (
            ActorSpec {
                actor_id,
                binding,
                factory,
                actor_options,
                restart,
                shutdown,
            },
            actor_ref,
        )
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
    #[must_use]
    pub fn message_size(mut self, size_hint: fn(&M) -> usize) -> Self {
        self.actor_options = self.actor_options.message_size(size_hint);
        self
    }
}

/// An [`ActorSpec`] whose stable binding has been minted and whose mailbox
/// configuration is frozen.
pub type SealedActorSpec<M> = ActorSpec<M, false>;

impl<M: Send + 'static, const CONFIGURABLE: bool> ActorSpec<M, CONFIGURABLE> {
    /// Overrides the enclosing scope's complete restart declaration.
    ///
    /// This replaces the inherited mode, budget, backoff, and terminal-removal
    /// behavior. Restate any scope-level values this actor should retain.
    #[must_use]
    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Overrides the enclosing scope's shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = Some(shutdown);
        self
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

    /// Converts this declaration into the advanced custom-host actor.
    ///
    /// This conversion stores configuration without applying supervision-tree
    /// validation. Supervised placement rejects a zero mailbox capacity with
    /// [`SupervisorBuildError`](crate::SupervisorBuildError); a direct host
    /// sees the same rejection as
    /// [`ActorRunError::ZeroMailboxCapacity`](crate::host::ActorRunError::ZeroMailboxCapacity)
    /// when the run starts.
    pub fn into_runnable(self) -> RunnableActor {
        self.into_deferred_node()
            .materialize(&RunnableActorBuilder::new())
            .actor
            .expect("materialized actor declaration carries its runnable actor")
    }

    pub(crate) fn into_node(self, builder: &RunnableActorBuilder) -> ActorNode {
        self.into_deferred_node().materialize(builder)
    }

    pub(crate) fn into_deferred_node(self) -> ActorNode {
        let Self {
            actor_id,
            binding,
            factory,
            actor_options,
            restart,
            shutdown,
        } = self;
        let deferred = DeferredActor(Box::new(DeferredActorSpec {
            actor_id,
            binding,
            factory,
            actor_options,
        }));
        ActorNode {
            actor: None,
            deferred: Some(deferred),
            restart,
            shutdown,
        }
    }
}

impl<M: Send + 'static> ActorSpec<M, false> {
    /// Returns another handle to this sealed declaration's stable binding.
    ///
    /// Mailbox configuration has already been frozen by the consuming
    /// `actor_ref` call that produced this declaration, so every handle
    /// observes the same binding configuration.
    pub fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::from_core(self.binding(), None)
    }
}

impl<M: Send + 'static, const CONFIGURABLE: bool> fmt::Debug for ActorSpec<M, CONFIGURABLE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSpec")
            .field("actor_id", &self.actor_id)
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}

/// Internal type-erased actor declaration used by supervision-tree lowering.
pub(crate) struct ActorNode {
    pub(crate) actor: Option<RunnableActor>,
    pub(crate) deferred: Option<DeferredActor>,
    pub(crate) restart: Option<Restart>,
    pub(crate) shutdown: Option<Shutdown>,
}

impl ActorNode {
    pub(crate) fn label(&self) -> &str {
        match (&self.actor, &self.deferred) {
            (Some(actor), None) => actor.label(),
            (None, Some(deferred)) => deferred.label(),
            _ => unreachable!("an actor node has exactly one payload"),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ActorOptionsValidationError> {
        match (&self.actor, &self.deferred) {
            (Some(_), None) => Ok(()),
            (None, Some(deferred)) => deferred.validate(),
            _ => unreachable!("an actor node has exactly one payload"),
        }
    }

    pub(crate) fn materialize(mut self, builder: &RunnableActorBuilder) -> Self {
        if let Some(deferred) = self.deferred.take() {
            self.actor = Some(deferred.materialize(builder));
        }
        self
    }
}

impl fmt::Debug for ActorNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorNode")
            .field("id", &self.label())
            .field("restart", &self.restart)
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

/// Unfilled actor declaration for cyclic wiring.
///
/// Create the slot and its ref before factories that close a cycle, then pass
/// the factory to [`define`](Self::define) to obtain an ordinary [`ActorSpec`].
/// A slot has the same fluent configuration vocabulary as `ActorSpec`.
/// `CONFIGURABLE` follows the same transition: [`actor_ref`](Self::actor_ref)
/// returns a [`SealedActorSlot`] whose mailbox configuration is frozen.
pub struct ActorSlot<M: Send + 'static, const CONFIGURABLE: bool = true> {
    actor_id: Arc<str>,
    binding: OnceLock<Arc<BindingCore<M>>>,
    actor_options: ActorOptions<M>,
    restart: Option<Restart>,
    shutdown: Option<Shutdown>,
}

impl<M: Send + 'static> ActorSlot<M, true> {
    /// Opens an unfilled actor declaration.
    pub fn new(actor_id: impl Into<String>) -> Self {
        Self {
            actor_id: Arc::from(actor_id.into()),
            binding: OnceLock::new(),
            actor_options: ActorOptions::new(),
            restart: None,
            shutdown: None,
        }
    }

    /// Mints this slot's restart-stable typed ref and seals mailbox
    /// configuration.
    pub fn actor_ref(self) -> (SealedActorSlot<M>, ActorRef<M>) {
        let binding = Arc::clone(self.binding());
        let actor_ref = ActorRef::from_core(&binding, None);
        let Self {
            actor_id,
            binding,
            actor_options,
            restart,
            shutdown,
        } = self;
        (
            ActorSlot {
                actor_id,
                binding,
                actor_options,
                restart,
                shutdown,
            },
            actor_ref,
        )
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

    /// Overrides the hosting scope's default mailbox capacity for this slot.
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
        self.actor_options = self.actor_options.message_size(size_hint);
        self
    }
}

/// An [`ActorSlot`] whose stable binding has been minted and whose mailbox
/// configuration is frozen.
pub type SealedActorSlot<M> = ActorSlot<M, false>;

impl<M: Send + 'static> ActorSlot<M, false> {
    /// Returns another handle to this sealed slot's stable binding.
    ///
    /// Mailbox configuration has already been frozen by the consuming
    /// `actor_ref` call that produced this slot, so every handle observes the
    /// same binding configuration.
    pub fn actor_ref(&self) -> ActorRef<M> {
        let binding = self
            .binding
            .get()
            .expect("a sealed actor slot has already minted its binding");
        ActorRef::from_core(binding, None)
    }
}

impl<M: Send + 'static, const CONFIGURABLE: bool> ActorSlot<M, CONFIGURABLE> {
    /// Overrides the enclosing scope's complete restart declaration.
    ///
    /// This replaces the inherited mode, budget, backoff, and terminal-removal
    /// behavior. Restate any scope-level values this actor should retain.
    #[must_use]
    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Overrides the enclosing scope's shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Defines this cyclic-wiring slot into an ordinary actor declaration.
    ///
    /// The slot token's message type must match the actor's message type, so a
    /// mismatch is rejected by the compiler. Consuming the token also makes a
    /// second definition unrepresentable in ordinary Rust code.
    pub fn define<F>(self, factory: F) -> ActorSpec<M, CONFIGURABLE>
    where
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        ActorSpec {
            actor_id: self.actor_id,
            binding: self.binding,
            factory: Box::new(factory),
            actor_options: self.actor_options,
            restart: self.restart,
            shutdown: self.shutdown,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ActorSlot, ActorSpec, MailboxMode};
    use crate::{Actor, ActorResult, MessageContext, Restart, Shutdown};

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
    fn actor_spec_applies_options_and_returns_runnable() {
        let spec = ActorSpec::new("worker", || OpaqueActor).mailbox(MailboxMode::conflate());
        let (spec, actor_ref) = spec.actor_ref();
        let actor = spec.into_runnable();
        assert_eq!(actor_ref.id(), "worker");
        assert_eq!(actor.label(), "worker");
    }

    #[test]
    fn into_runnable_stores_zero_capacity_without_supervisor_validation() {
        let actor = ActorSpec::new("worker", || OpaqueActor)
            .mailbox_capacity(0)
            .into_runnable();

        assert_eq!(actor.label(), "worker");
    }

    #[tokio::test]
    async fn run_until_rejects_zero_mailbox_capacity() {
        let actor = ActorSpec::new("worker", || OpaqueActor)
            .mailbox_capacity(0)
            .into_runnable();

        let result = actor
            .run_until(
                std::future::ready(()),
                Default::default(),
                Shutdown::drain_for(Duration::from_secs(1)),
            )
            .await;

        assert!(matches!(
            result,
            Err(crate::host::ActorRunError::ZeroMailboxCapacity { actor_id }) if actor_id == "worker"
        ));
    }

    #[test]
    fn sealed_declarations_still_accept_non_binding_policies() {
        let (spec, _actor_ref) = ActorSpec::new("spec", || OpaqueActor).actor_ref();
        let spec = spec.restart(Restart::never()).shutdown(Shutdown::abort());
        assert_eq!(spec.restart, Some(Restart::never()));
        assert_eq!(spec.shutdown, Some(Shutdown::abort()));

        let (slot, _actor_ref) = ActorSlot::<OpaqueMessage>::new("slot").actor_ref();
        let spec = slot
            .restart(Restart::always())
            .shutdown(Shutdown::discard_after_current(Duration::from_secs(1)))
            .define(|| OpaqueActor);
        assert_eq!(spec.restart, Some(Restart::always()));
        assert_eq!(
            spec.shutdown,
            Some(Shutdown::discard_after_current(Duration::from_secs(1)))
        );
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
