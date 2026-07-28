use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorFactory, ActorOptions, ActorRef, ActorStats, RawActor, RunnableActor,
    SupervisorPathSegment, actor::RunnableActorBuilder,
};
use thiserror::Error;
use tokio::sync::watch;
use tokio_supervisor::{
    AttachedChildIdentity, ChildSpec, CompletionGuard, CompletionOutcome, ControlError,
    LifecycleEvent, LifecycleWatch, RestartIntensity, RestartPolicy, ShutdownPolicy, Supervisor,
    SupervisorError, SupervisorHandle, SupervisorSnapshot, SupervisorSpec,
};

use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct ActorRuntimeState {
    config: Mutex<ActorRuntimeConfig>,
}

#[derive(Debug)]
struct ActorRuntimeConfig {
    actor_builder: RunnableActorBuilder,
    default_restart: RestartPolicy,
    default_shutdown: ShutdownPolicy,
}

impl ActorRuntimeState {
    pub(crate) fn new(
        actor_builder: RunnableActorBuilder,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) -> Self {
        Self {
            config: Mutex::new(ActorRuntimeConfig {
                actor_builder,
                default_restart,
                default_shutdown,
            }),
        }
    }

    pub(crate) fn configure(
        &self,
        actor_builder: RunnableActorBuilder,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) {
        *self.config.lock().unwrap_or_else(PoisonError::into_inner) = ActorRuntimeConfig {
            actor_builder,
            default_restart,
            default_shutdown,
        };
    }

    fn actor_defaults(&self) -> (RestartPolicy, ShutdownPolicy) {
        let config = self.config.lock().unwrap_or_else(PoisonError::into_inner);
        (config.default_restart, config.default_shutdown)
    }

    fn make_actor<F>(
        &self,
        label: impl Into<String>,
        factory: F,
        options: ActorOptions<<F::Actor as RawActor>::Msg>,
    ) -> (RunnableActor, ActorRef<<F::Actor as RawActor>::Msg>)
    where
        F: ActorFactory,
    {
        // Construction runs the caller's factory, which may reach back into
        // this runtime. Release the config lock first so that re-entry cannot
        // deadlock on a non-reentrant mutex.
        let actor_builder = self
            .config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .actor_builder
            .clone();
        actor_builder.actor_with_options(label, factory, options)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeAttachment {
    owner: Weak<ActorRuntimeState>,
    kind: RuntimeAttachmentKind,
}

#[derive(Clone, Debug)]
enum RuntimeAttachmentKind {
    Actor(RunnableActor),
    Subtree(Arc<ActorRuntimeState>),
}

impl RuntimeAttachment {
    fn actor(owner: &Arc<ActorRuntimeState>, actor: RunnableActor) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            kind: RuntimeAttachmentKind::Actor(actor),
        }
    }

    pub(crate) fn subtree(owner: &Arc<ActorRuntimeState>, state: Arc<ActorRuntimeState>) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            kind: RuntimeAttachmentKind::Subtree(state),
        }
    }

    fn belongs_to(&self, owner: &Arc<ActorRuntimeState>) -> bool {
        self.owner.ptr_eq(&Arc::downgrade(owner))
    }
}

/// Options applied when adding a runtime actor to a supervised runtime.
///
/// These options configure both the actor's mailbox and its supervised-child
/// lifecycle. The message type is inferred from the factory passed to
/// [`RuntimeHandle::add_actor`]. Configure restart and shutdown behavior with
/// [`restart`](Self::restart) and [`shutdown`](Self::shutdown); options left
/// unset inherit the dynamic runtime's defaults.
#[derive(Debug)]
#[non_exhaustive]
pub struct DynamicActorOptions<M = ()> {
    // Restart policy for the supervised actor child.
    restart: RestartPolicy,
    restart_is_default: bool,
    // Shutdown policy for the supervised actor child.
    shutdown: ShutdownPolicy,
    shutdown_is_default: bool,
    /// Optional restart intensity override for this actor child.
    pub restart_intensity: Option<RestartIntensity>,
    actor_options: ActorOptions<M>,
    // `None` selects the dynamic-actor default. Keeping the override unresolved
    // makes `restart(...).remove_on_exit(...)` order-independent.
    remove_on_exit: Option<bool>,
}

impl<M> Clone for DynamicActorOptions<M> {
    fn clone(&self) -> Self {
        Self {
            restart: self.restart,
            restart_is_default: self.restart_is_default,
            shutdown: self.shutdown,
            shutdown_is_default: self.shutdown_is_default,
            restart_intensity: self.restart_intensity,
            actor_options: self.actor_options.clone(),
            remove_on_exit: self.remove_on_exit,
        }
    }
}

impl<M> Default for DynamicActorOptions<M> {
    fn default() -> Self {
        Self {
            restart: RestartPolicy::OnFailure,
            restart_is_default: true,
            shutdown: ShutdownPolicy::default(),
            shutdown_is_default: true,
            restart_intensity: None,
            actor_options: ActorOptions::new(),
            remove_on_exit: None,
        }
    }
}

impl<M> DynamicActorOptions<M> {
    /// Creates options with restart-on-failure and the default shutdown policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the restart policy for the supervised actor child.
    ///
    /// Without this override, the actor inherits the dynamic runtime's
    /// configured restart default.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self.restart_is_default = false;
        self
    }

    /// Sets the shutdown policy for the supervised actor child.
    ///
    /// Without this override, the actor inherits the dynamic runtime's
    /// configured shutdown default.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = shutdown;
        self.shutdown_is_default = false;
        self
    }

    /// Overrides the supervisor's restart intensity for this actor.
    #[must_use]
    pub fn restart_intensity(mut self, restart_intensity: RestartIntensity) -> Self {
        self.restart_intensity = Some(restart_intensity);
        self
    }

    /// Sets the actor's mailbox and message-observation options.
    ///
    /// [`ActorOptions::mailbox_capacity`] overrides the hosting scope's
    /// default for this actor. Unkeyed
    /// [`MailboxMode::Conflate`](crate::MailboxMode::Conflate) always has
    /// capacity one and ignores both the scope default and the override.
    #[must_use]
    pub fn options(mut self, options: ActorOptions<M>) -> Self {
        self.actor_options = options;
        self
    }

    /// Sets whether the actor child is removed after a terminal exit.
    ///
    /// Removal happens only when the restart policy declines a restart, never
    /// during a restart cycle. Dynamic actors are removed on terminal exit by
    /// default, independent of their restart policy. Pass `false` to retain a
    /// terminal actor in supervisor snapshots. Watchers observe
    /// [`Terminated`](crate::MonitorEvent::Terminated) before removal completes,
    /// but the child id becomes reusable only when removal completes; wait for
    /// the snapshot to drop the membership before re-adding the same id.
    #[must_use]
    pub fn remove_on_exit(mut self, remove_on_exit: bool) -> Self {
        self.remove_on_exit = Some(remove_on_exit);
        self
    }

    fn child_options(
        &self,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) -> DynamicChildOptions {
        let restart = if self.restart_is_default {
            default_restart
        } else {
            self.restart
        };
        let shutdown = if self.shutdown_is_default {
            default_shutdown
        } else {
            self.shutdown
        };
        let remove_on_exit = self.remove_on_exit.unwrap_or(true);
        DynamicChildOptions {
            restart,
            shutdown,
            restart_intensity: self.restart_intensity,
            remove_on_exit,
        }
    }

    fn into_parts(
        self,
        default_restart: RestartPolicy,
        default_shutdown: ShutdownPolicy,
    ) -> (ActorOptions<M>, DynamicChildOptions) {
        let child_options = self.child_options(default_restart, default_shutdown);
        (self.actor_options, child_options)
    }
}

struct DynamicChildOptions {
    restart: RestartPolicy,
    shutdown: ShutdownPolicy,
    restart_intensity: Option<RestartIntensity>,
    remove_on_exit: bool,
}

/// Cancellation guard for a lifecycle-event mailbox pump.
///
/// Created by [`RuntimeHandle::watch_lifecycle_to`]. Dropping the guard
/// cancels the pump. It also stops automatically when the watched supervisor
/// identity or target actor permanently terminates.
#[must_use = "dropping the guard immediately cancels the lifecycle watch"]
pub struct LifecycleWatchGuard {
    cancellation: CancellationToken,
}

impl LifecycleWatchGuard {
    /// Cancels the lifecycle pump.
    ///
    /// Cancellation is idempotent. A message already accepted by the target
    /// mailbox cannot be retracted.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether the lifecycle pump has been cancelled or stopped.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

impl std::fmt::Debug for LifecycleWatchGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleWatchGuard")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Drop for LifecycleWatchGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn spawn_lifecycle_watch_to<M, F>(
    mut lifecycle: LifecycleWatch,
    target: ActorRef<M>,
    mut map: F,
) -> LifecycleWatchGuard
where
    M: Send + 'static,
    F: FnMut(LifecycleEvent) -> M + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();

    tokio::spawn(async move {
        let _cancel_on_exit = task_cancellation.clone().drop_guard();
        loop {
            let Some(event) = (tokio::select! {
                biased;
                () = task_cancellation.cancelled() => None,
                () = target.wait_terminated() => None,
                event = lifecycle.next() => event,
            }) else {
                return;
            };

            tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return,
                () = target.wait_terminated() => return,
                sent = target.send_to_incarnation(map(event)) => {
                    if sent.is_err() {
                        return;
                    }
                }
            }
        }
    });

    LifecycleWatchGuard { cancellation }
}

/// Configured-but-not-yet-running runtime that owns a supervisor and its
/// actor factory.
///
/// Start the runtime with [`spawn`](Self::spawn), which returns the
/// [`RuntimeHandle`] control surface. To drive the runtime in the foreground
/// while keeping that control surface, call `spawn()` and then
/// [`RuntimeHandle::wait`]. Use [`into_supervisor`](Self::into_supervisor)
/// as the explicit escape hatch to the raw [`Supervisor`].
pub struct Runtime {
    supervisor: Supervisor,
    actors: Arc<ActorRuntimeState>,
}

impl Runtime {
    /// Starts building a supervised actor runtime.
    ///
    /// Provide a graph to run every graph actor as its own supervised child in
    /// one ordered scope. Use [`SupervisionTree`](crate::SupervisionTree) for
    /// recursive composition, arbitrary task children, actor-owned scopes, or
    /// per-actor policy overrides. Use [`Runtime::dynamic`] when actors or
    /// subtrees will be added at runtime.
    ///
    /// See [`RuntimeBuilder`](crate::RuntimeBuilder) for an example.
    pub fn builder() -> crate::RuntimeBuilder {
        crate::RuntimeBuilder::new()
    }

    /// Starts building a graph-less runtime with dynamic membership.
    pub fn dynamic() -> crate::DynamicRuntimeBuilder {
        crate::DynamicRuntimeBuilder::new()
    }

    /// Creates an actor-aware runtime around an arbitrary supervisor.
    ///
    /// The supplied supervisor keeps its immutable scope kind and starts with
    /// no actor metadata. Actors added through [`RuntimeHandle::add_actor`] are
    /// tracked normally when it is a dynamic supervisor; an ordered supervisor
    /// remains observation-only through this actor-aware wrapper.
    /// `Supervisor` is intentionally not re-exported by `tokio-otp`, so this
    /// raw adoption path requires a direct `tokio-supervisor` dependency.
    ///
    /// ```no_run
    /// use tokio_otp::{
    ///     Actor, MessageContext, ActorResult, DynamicActorOptions, Runtime,
    ///     prelude::Continue,
    /// };
    /// use tokio_supervisor::DynamicSupervisorBuilder;
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
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let supervisor = DynamicSupervisorBuilder::new().build()?;
    /// let handle = Runtime::new(supervisor).spawn();
    /// handle
    ///     .add_actor("worker", || Worker, DynamicActorOptions::new())
    ///     .await?;
    /// handle.shutdown_and_wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(supervisor: Supervisor) -> Self {
        let default_restart = supervisor.default_restart_policy();
        let default_shutdown = supervisor.default_shutdown_policy();
        Self {
            supervisor,
            actors: Arc::new(ActorRuntimeState::new(
                RunnableActorBuilder::new(),
                default_restart,
                default_shutdown,
            )),
        }
    }

    pub(crate) fn with_actor_tree(supervisor: Supervisor, actors: Arc<ActorRuntimeState>) -> Self {
        Self { supervisor, actors }
    }

    pub(crate) fn into_parts(self) -> (Supervisor, Arc<ActorRuntimeState>) {
        (self.supervisor, self.actors)
    }

    /// Returns this runtime's stable actor-aware handle before it is spawned.
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle::new(self.supervisor.handle(), Arc::clone(&self.actors))
    }

    /// Returns the underlying [`Supervisor`] for first-class nesting.
    ///
    /// This discards the actor factories and recursive actor metadata, so
    /// [`RuntimeHandle::add_actor`] and recursive actor stats are not available
    /// after converting to a raw supervisor. Keep the full runtime and use
    /// [`spawn`](Self::spawn) if you need actor-aware runtime behavior.
    /// `Supervisor` is intentionally not re-exported by `tokio-otp`, so using
    /// this raw escape hatch requires a direct `tokio-supervisor` dependency.
    ///
    /// ```no_run
    /// use tokio_otp::{Actor, MessageContext, ActorResult, GraphBuilder, Runtime};
    /// use tokio_otp::prelude::Continue;
    /// use tokio_supervisor::{SupervisorBuilder, SupervisorSpec};
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
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut graph = GraphBuilder::new();
    /// let (actor_slot, _) = graph.slot("worker", tokio_otp::ActorOptions::new());
    /// graph.define(actor_slot, || Worker);
    /// let actor_subtree = Runtime::builder()
    ///     .graph(graph.build()?)
    ///     .build()?
    ///     .into_supervisor();
    /// let root = SupervisorBuilder::new()
    ///     .supervisor(SupervisorSpec::new("actors", actor_subtree))
    ///     .build()?;
    /// # let _ = root;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_supervisor(self) -> Supervisor {
        self.supervisor
    }

    /// Spawns the supervisor in the background and returns a combined handle.
    pub fn spawn(self) -> RuntimeHandle {
        let supervisor = self.supervisor.spawn();
        RuntimeHandle::new(supervisor, self.actors)
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// Cheaply cloneable runtime control surface.
///
/// For a spawned root runtime, dropping the last public handle clone requests
/// graceful shutdown. A handle scoped to a nested runtime does not own that
/// runtime's lifecycle; dropping it leaves the parent-owned subtree running.
#[derive(Clone)]
pub struct RuntimeHandle {
    supervisor: SupervisorHandle,
    actors: Arc<ActorRuntimeState>,
}

/// Errors returned when adding an actor-aware runtime subtree dynamically.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum AddSubtreeError {
    /// The runtime builder contains an invalid supervisor configuration.
    #[error("failed to build runtime subtree: {0}")]
    Build(#[from] tokio_supervisor::SupervisorBuildError),
    /// The parent supervisor rejected the dynamic child operation.
    #[error("failed to add runtime subtree: {0}")]
    Control(#[from] ControlError),
}

impl RuntimeHandle {
    pub(crate) fn new(supervisor: SupervisorHandle, actors: Arc<ActorRuntimeState>) -> Self {
        Self { supervisor, actors }
    }

    pub(crate) fn unavailable() -> Self {
        static UNAVAILABLE: OnceLock<RuntimeHandle> = OnceLock::new();

        UNAVAILABLE
            .get_or_init(|| {
                let builder = tokio_supervisor::DynamicSupervisorBuilder::new();
                let supervisor = builder.handle();
                drop(builder);
                Self::new(
                    supervisor,
                    Arc::new(ActorRuntimeState::new(
                        RunnableActorBuilder::new(),
                        RestartPolicy::default(),
                        ShutdownPolicy::default(),
                    )),
                )
            })
            .clone()
    }

    /// Requests a graceful shutdown of the supervisor.
    pub fn shutdown(&self) {
        self.supervisor.shutdown();
    }

    /// Requests a graceful shutdown and waits for the supervisor to stop.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Builds and adds an actor-aware runtime subtree dynamically.
    ///
    /// The returned handle can add actors or further subtrees, and recursive
    /// [`actor_stats`](Self::actor_stats) include the new subtree. Removing the
    /// child detaches its actor metadata with the supervisor membership;
    /// retained subtree handles then fail control operations with
    /// [`ControlError::Unavailable`].
    ///
    /// The subtree must be in its non-cloneable reserved form. Call
    /// [`SupervisionTree::reserve`](crate::SupervisionTree::reserve) for a
    /// plain declaration; [`RuntimeBuilder`](crate::RuntimeBuilder) and
    /// [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder) convert into
    /// that form directly. Reservation is explicit because it can fail, and
    /// because accepting a fresh plain declaration here could not preserve any
    /// pre-spawn root or nested handles through insertion.
    ///
    /// If the subtree itself restarts, its statically declared graph actors
    /// is recreated, while children added later through the returned handle
    /// are lost and must be replayed by the application. If this handle's
    /// supervisor restarts, the dynamically added subtree is not recreated.
    ///
    /// Restart intensity remains tracked per child across this boundary.
    /// This operation is supported only when this handle targets a dynamic
    /// scope; ordered scopes return
    /// [`ControlError::UnsupportedByScopeKind`]. Dynamic additions start
    /// immediately and dynamic siblings stop concurrently under one shared
    /// maximum-grace deadline. Use [`wait_started`](Self::wait_started) when
    /// readiness is needed.
    pub async fn add_subtree(
        &self,
        id: impl Into<String>,
        tree: impl Into<crate::ReservedSupervisionTree>,
    ) -> Result<RuntimeHandle, AddSubtreeError> {
        let id = id.into();
        let (nested_supervisor, nested_actors) = tree.into().build()?.into_parts();
        let lineage = self
            .supervisor
            .add_supervisor(
                SupervisorSpec::new(id.clone(), nested_supervisor).attachment(
                    RuntimeAttachment::subtree(&self.actors, Arc::clone(&nested_actors)),
                ),
            )
            .await?;
        self.subtree_membership(&id, Some(lineage))
            .ok_or(ControlError::Unavailable.into())
    }

    /// Returns the actor-aware handle for a direct runtime subtree.
    ///
    /// `None` means that this runtime has no registered subtree with `id`.
    pub fn subtree(&self, id: &str) -> Option<RuntimeHandle> {
        self.subtree_membership(id, None)
    }

    fn subtree_membership(&self, id: &str, lineage: Option<u64>) -> Option<RuntimeHandle> {
        self.supervisor
            .attached_children::<RuntimeAttachment>()
            .into_iter()
            .find_map(|attached| {
                let [identity] = attached.path() else {
                    return None;
                };
                if identity.id != id
                    || lineage.is_some_and(|lineage| identity.lineage != lineage)
                    || !attached.attachment().belongs_to(&self.actors)
                {
                    return None;
                }
                let RuntimeAttachmentKind::Subtree(actors) = &attached.attachment().kind else {
                    return None;
                };
                Some(Self::new(
                    attached.supervisor()?.clone(),
                    Arc::clone(actors),
                ))
            })
    }

    /// Adds an arbitrary supervised task child to this runtime.
    ///
    /// This is the task-level counterpart to [`add_actor`](Self::add_actor).
    /// It is supported only for dynamic scopes; ordered scopes return
    /// [`ControlError::UnsupportedByScopeKind`]. Success means the membership
    /// was inserted and startup was scheduled, and returns the lineage
    /// assigned to that membership. Task children do not appear in
    /// [`actor_stats`](Self::actor_stats), but remain visible through snapshots
    /// and lifecycle watches.
    pub async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.supervisor.add_child(child).await
    }

    /// Adds a supervised runtime actor from an incarnation factory and returns
    /// its stable typed ref.
    ///
    /// The actor's label is also its direct supervisor child id, so it can be
    /// removed later with [`remove_child`](Self::remove_child). See
    /// [`ActorFactory`] for the incarnation lifecycle contract. This operation
    /// is supported only for dynamic scopes; ordered scopes return
    /// [`ControlError::UnsupportedByScopeKind`]. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`wait_started`](Self::wait_started)
    /// retains the stronger readiness contract. A zero
    /// [`ActorOptions::mailbox_capacity`] is rejected with
    /// [`ControlError::InvalidConfig`].
    pub async fn add_actor<F>(
        &self,
        label: impl Into<String>,
        factory: F,
        options: DynamicActorOptions<<F::Actor as RawActor>::Msg>,
    ) -> Result<ActorRef<<F::Actor as RawActor>::Msg>, ControlError>
    where
        F: ActorFactory,
    {
        let (default_restart, default_shutdown) = self.actors.actor_defaults();
        let (actor_options, dynamic_options) =
            options.into_parts(default_restart, default_shutdown);
        actor_options
            .validate()
            .map_err(ControlError::InvalidConfig)?;
        let actor = self.actors.make_actor(label, factory, actor_options);
        self.add_constructed_actor(actor, dynamic_options).await
    }

    async fn add_constructed_actor<M>(
        &self,
        (actor, actor_ref): (RunnableActor, ActorRef<M>),
        options: DynamicChildOptions,
    ) -> Result<ActorRef<M>, ControlError> {
        let child = actor_child_spec(
            actor.clone(),
            &self.actors,
            ActorChildOptions::new(options.restart, options.shutdown)
                .restart_intensity(options.restart_intensity)
                .remove_on_exit(options.remove_on_exit),
        );
        self.supervisor.add_child(child).await?;

        Ok(actor_ref)
    }

    /// Removes a child from the supervisor.
    ///
    /// Removal marks the membership as removing and starts its configured
    /// shutdown. When cooperative shutdown completes within its grace period,
    /// an [`Actor`](crate::Actor) stops its normal receive loop, closes external
    /// intake, applies its [`DrainPolicy`](crate::DrainPolicy), runs `on_stop`,
    /// makes the mailbox binding terminal, and is then detached. Immediate
    /// abort, or expiry of the cooperative grace period, can skip any remaining
    /// drain or hook work before detachment. The returned future completes
    /// after detachment (or after the configured shutdown backstop aborts it).
    ///
    /// A send racing with removal may still be accepted. With
    /// `DrainPolicy::Drain`, work accepted before drain closes intake belongs
    /// to the queued prefix handled before `on_stop`. With `Discard`, accepted
    /// work that remains queued is dropped. Once the actor closes intake,
    /// `try_send` may briefly return
    /// [`SendError::MailboxClosed`](crate::SendError::MailboxClosed), while an
    /// awaited `send` waits and then returns
    /// [`SendError::ActorTerminated`](crate::SendError::ActorTerminated).
    /// Removal does not return queued messages: end-to-end delivery ownership
    /// belongs in an application acknowledgement and replay protocol.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.supervisor.remove_child(id).await
    }

    /// Waits for the supervisor to stop.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }

    /// Waits until all current actor children have completed `on_start`.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait_started().await
    }

    /// Waits until every named child is simultaneously completed.
    ///
    /// Children are addressed by their supervisor child id. That id defaults
    /// to an actor's graph label, but derived and nested scopes use the local
    /// field or child name. Use a [`subtree`](Self::subtree) handle for nested
    /// scopes. Awaiting an id that is never a child of this scope does not
    /// complete. See
    /// [`CompletionOutcome`] for the distinction between completion and the
    /// supervisor stopping first.
    pub async fn wait_completed<I, S>(&self, ids: I) -> CompletionOutcome
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_completed(ids).await
    }

    /// Shuts this runtime down once every named child has completed.
    ///
    /// Child ids follow the same rules as [`wait_completed`](Self::wait_completed).
    ///
    /// Arm this from a pre-spawn handle when fast children could complete
    /// immediately. The returned guard must be retained; dropping it cancels
    /// the completion watch and leaves the runtime running.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_on_completion<I, S>(&self, ids: I) -> CompletionGuard
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.shutdown_on_completion(ids)
    }

    /// Returns the ordered lifecycle stream for this runtime's direct
    /// children, including restart scheduling and restart-intensity failure.
    ///
    /// Create the watch before reading [`snapshot`](Self::snapshot), then
    /// discard child transitions whose `seq` is at most the snapshot's
    /// `lifecycle_seq` to obtain a gap-free state-plus-stream view. Pre-spawn snapshots
    /// already project configured children, so reducers should apply their
    /// later `Added` events as idempotent membership upserts. Use a
    /// [`subtree`](Self::subtree) handle for nested scopes.
    pub fn watch_lifecycle(&self) -> LifecycleWatch {
        self.supervisor.watch_lifecycle()
    }

    /// Returns the ordered lifecycle stream for this runtime's entire
    /// supervisor tree.
    ///
    /// Events from nested scopes carry a stable supervisor path relative to
    /// this runtime. The stream also includes supervisor start/stop
    /// transitions and scheduled-restart delays.
    pub fn watch_lifecycle_recursive(&self) -> LifecycleWatch {
        self.supervisor.watch_lifecycle_recursive()
    }

    /// Pumps lifecycle events into `target` using its ordinary mailbox policy.
    ///
    /// The pump follows the target through ordinary actor restarts, but never
    /// replays an event to a fresh incarnation: lifecycle events are discrete
    /// history, so replay would fabricate a transition. A restarted consumer
    /// should rehydrate using watch → snapshot → `lifecycle_seq` filtering in
    /// `on_start`. FIFO mailboxes are recommended when every transition must
    /// be observed; a conflating mailbox may discard intermediate messages.
    ///
    /// The pump stops when the returned guard is dropped or cancelled, when
    /// this runtime's identity becomes terminal after draining its staged
    /// events, or when the target actor permanently terminates.
    pub fn watch_lifecycle_to<M, F>(&self, target: &ActorRef<M>, map: F) -> LifecycleWatchGuard
    where
        M: Send + 'static,
        F: FnMut(LifecycleEvent) -> M + Send + 'static,
    {
        spawn_lifecycle_watch_to(self.watch_lifecycle(), target.clone(), map)
    }

    /// Returns a clone of the latest supervisor snapshot.
    pub fn snapshot(&self) -> SupervisorSnapshot {
        self.supervisor.snapshot()
    }

    /// Returns point-in-time stats for this runtime and all nested runtime
    /// subtrees. This runtime's actors come first, followed recursively by each
    /// subtree in declaration order.
    ///
    /// Each sample is derived from the opaque actor attachment carried by the
    /// supervisor's current child membership. This excludes stale actors after
    /// raw child removal, same-id replacement, or a subtree restart that drops
    /// incarnation-local dynamic children by construction.
    pub fn actor_stats(&self) -> Vec<ActorStats> {
        let mut runtime_owners = HashMap::from([(Vec::new(), Arc::clone(&self.actors))]);
        let mut stats = Vec::new();

        for attached in self.supervisor.attached_children::<RuntimeAttachment>() {
            let Some((child, supervisor_path)) = attached.path().split_last() else {
                continue;
            };
            let Some(owner) = runtime_owners.get(supervisor_path) else {
                continue;
            };
            let attachment = attached.attachment();
            if !attachment.belongs_to(owner) {
                continue;
            }

            match &attachment.kind {
                RuntimeAttachmentKind::Actor(actor) => {
                    let mut actor_stats = actor.stats();
                    actor_stats.supervisor_path = Some(
                        supervisor_path
                            .iter()
                            .map(supervisor_path_segment)
                            .collect(),
                    );
                    actor_stats.lineage = Some(child.lineage);
                    stats.push(actor_stats);
                }
                RuntimeAttachmentKind::Subtree(subtree) => {
                    runtime_owners.insert(attached.path().to_vec(), Arc::clone(subtree));
                }
            }
        }

        stats
    }

    /// Returns a watch receiver that updates when the snapshot changes.
    pub fn subscribe_snapshots(&self) -> watch::Receiver<SupervisorSnapshot> {
        self.supervisor.subscribe_snapshots()
    }
}

impl std::fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHandle").finish_non_exhaustive()
    }
}

/// Terminates the actor's binding when the supervisor drops the child spec —
/// the point after which no further restart can happen (restart intensity
/// exhausted, child removed, or supervisor exit). Without this, an `Always`
/// or `OnFailure` actor whose last run failed leaves its binding `Unbound` and
/// senders wait forever for a rebind.
struct TerminateBindingOnDrop {
    actor: RunnableActor,
}

impl TerminateBindingOnDrop {
    fn new(actor: RunnableActor) -> Self {
        Self { actor }
    }
}

impl Drop for TerminateBindingOnDrop {
    fn drop(&mut self) {
        self.actor.terminate_binding();
    }
}

/// How one actor is supervised as a child of its enclosing scope.
pub(crate) struct ActorChildOptions {
    /// Id within the enclosing scope. Defaults to the actor label.
    pub(crate) child_id: Option<String>,
    pub(crate) restart: RestartPolicy,
    pub(crate) shutdown: ShutdownPolicy,
    pub(crate) restart_intensity: Option<RestartIntensity>,
    /// Whether the membership disappears when the actor exits, rather than
    /// resting as an inactive entry.
    pub(crate) remove_on_exit: bool,
    /// The scope this actor leads, for an `ActorWithScope` leader. `None` for
    /// every other actor shape.
    pub(crate) children: Option<RuntimeHandle>,
}

impl ActorChildOptions {
    pub(crate) fn new(restart: RestartPolicy, shutdown: ShutdownPolicy) -> Self {
        Self {
            child_id: None,
            restart,
            shutdown,
            restart_intensity: None,
            remove_on_exit: false,
            children: None,
        }
    }

    pub(crate) fn child_id(mut self, child_id: Option<String>) -> Self {
        self.child_id = child_id;
        self
    }

    pub(crate) fn restart_intensity(mut self, intensity: Option<RestartIntensity>) -> Self {
        self.restart_intensity = intensity;
        self
    }

    pub(crate) fn remove_on_exit(mut self, remove_on_exit: bool) -> Self {
        self.remove_on_exit = remove_on_exit;
        self
    }

    pub(crate) fn children(mut self, children: RuntimeHandle) -> Self {
        self.children = Some(children);
        self
    }
}

pub(crate) fn actor_child_spec(
    actor: RunnableActor,
    owner: &Arc<ActorRuntimeState>,
    options: ActorChildOptions,
) -> ChildSpec {
    let ActorChildOptions {
        child_id,
        restart,
        shutdown,
        restart_intensity,
        remove_on_exit,
        children,
    } = options;
    let actor_id = child_id.unwrap_or_else(|| actor.label().to_owned());
    let attachment = RuntimeAttachment::actor(owner, actor.clone());
    let guard = Arc::new(TerminateBindingOnDrop::new(actor));
    let child_guard = Arc::clone(&guard);
    let actor_owner = Arc::clone(owner);
    let mut child = ChildSpec::new(actor_id, move |ctx| {
        let actor = child_guard.actor.clone();
        let supervisor = RuntimeHandle::new(ctx.supervisor(), Arc::clone(&actor_owner));
        let children = children.clone();
        async move {
            actor
                .run_until_ready(
                    ctx.shutdown_token().cancelled(),
                    ctx.abort_token().cancelled(),
                    restart,
                    supervisor,
                    children,
                    || ctx.mark_ready(),
                )
                .await
                .map_err(Into::into)
        }
    })
    .attachment(attachment)
    .wait_for_ready()
    .restart(restart)
    .remove_on_exit(remove_on_exit)
    .shutdown(shutdown);

    if let Some(intensity) = restart_intensity {
        child = child.restart_intensity(intensity);
    }

    child
}

fn supervisor_path_segment(identity: &AttachedChildIdentity) -> SupervisorPathSegment {
    SupervisorPathSegment {
        id: identity.id.clone(),
        lineage: identity.lineage,
        generation: identity.generation,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn unavailable_runtime_handle_is_cached() {
        let first = super::RuntimeHandle::unavailable();
        let second = super::RuntimeHandle::unavailable();

        assert!(std::sync::Arc::ptr_eq(&first.actors, &second.actors));
    }

    #[tokio::test]
    async fn subtree_membership_lookup_rejects_a_same_id_replacement() {
        let root = crate::Runtime::dynamic()
            .build()
            .expect("runtime builds")
            .spawn();
        root.add_subtree("workers", crate::Runtime::builder())
            .await
            .expect("first subtree added");
        let first_lineage = root
            .snapshot()
            .child("workers")
            .expect("first membership is visible")
            .lineage;

        root.remove_child("workers")
            .await
            .expect("first subtree removed");
        root.add_subtree("workers", crate::Runtime::builder())
            .await
            .expect("replacement subtree added");
        let replacement_lineage = root
            .snapshot()
            .child("workers")
            .expect("replacement membership is visible")
            .lineage;

        assert_ne!(first_lineage, replacement_lineage);
        assert!(
            root.subtree_membership("workers", Some(first_lineage))
                .is_none(),
            "a lookup bound to the completed add must not return a same-id replacement"
        );
        assert!(
            root.subtree_membership("workers", Some(replacement_lineage))
                .is_some()
        );

        root.shutdown_and_wait().await.expect("clean shutdown");
    }
}
