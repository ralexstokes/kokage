use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorFactory, ActorOptions, ActorRef, MailboxMode,
    actor::{
        ActorOptionsValidationError, ActorStats, CancelOnDrop, RawActor, RunnableActor,
        RunnableActorBuilder, SupervisorPathSegment,
    },
};
use kokage_supervisor::{
    __private::{self, AttachedChildIdentity},
    CancellationToken, ChildLifecycleEvent, ChildLifecycleWatch, ChildSpec, CompletionGuard,
    CompletionOutcome, ControlError, DynamicSupervisorHandle, LifecycleWatch, RestartConfig,
    RestartPolicy, RunningSupervisor, ShutdownPolicy, SupervisorBuildError, SupervisorError,
    SupervisorHandle, SupervisorSnapshot, SupervisorSnapshotReceiver,
};

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
/// [`DynamicRuntime::add_actor_with`]. Configure restart and shutdown behavior with
/// [`restart`](Self::restart) and [`shutdown`](Self::shutdown); options left
/// unset inherit the dynamic runtime's defaults.
#[derive(Debug)]
#[non_exhaustive]
pub struct DynamicActorOptions<M = ()> {
    // Restart-policy override for the supervised actor child.
    restart: Option<RestartPolicy>,
    // Shutdown-policy override for the supervised actor child.
    shutdown: Option<ShutdownPolicy>,
    // Optional restart intensity override for this actor child.
    restart_config: Option<RestartConfig>,
    actor_options: ActorOptions<M>,
    // `None` selects the dynamic-actor default. Keeping the override unresolved
    // makes `restart(...).remove_on_exit(...)` order-independent.
    remove_on_exit: Option<bool>,
}

impl<M> Clone for DynamicActorOptions<M> {
    fn clone(&self) -> Self {
        Self {
            restart: self.restart,
            shutdown: self.shutdown,
            restart_config: self.restart_config,
            actor_options: self.actor_options.clone(),
            remove_on_exit: self.remove_on_exit,
        }
    }
}

impl<M> Default for DynamicActorOptions<M> {
    fn default() -> Self {
        Self {
            restart: None,
            shutdown: None,
            restart_config: None,
            actor_options: ActorOptions::new(),
            remove_on_exit: None,
        }
    }
}

impl<M> DynamicActorOptions<M> {
    /// Creates options that inherit the dynamic scope's restart and shutdown
    /// policies.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the restart policy for the supervised actor child.
    ///
    /// Without this override, the actor inherits the dynamic runtime's
    /// configured restart default.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = Some(restart);
        self
    }

    /// Sets the shutdown policy for the supervised actor child.
    ///
    /// Without this override, the actor inherits the dynamic runtime's
    /// configured shutdown default.
    #[must_use]
    pub fn shutdown(mut self, shutdown: ShutdownPolicy) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Overrides the supervisor's restart intensity for this actor.
    #[must_use]
    pub fn restart_config(mut self, restart_config: RestartConfig) -> Self {
        self.restart_config = Some(restart_config);
        self
    }

    /// Overrides the hosting scope's mailbox capacity for this actor alone.
    ///
    /// [`ActorOptions::mailbox_capacity`] overrides the hosting scope's
    /// default for this actor. Unkeyed
    /// [`MailboxMode::conflate()`](crate::MailboxMode::conflate()) always has
    /// capacity one and ignores both the scope default and the override.
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

    /// Enables accepted-message byte observation using `size_hint`.
    #[must_use]
    pub fn message_size(mut self, size_hint: fn(&M) -> usize) -> Self {
        self.actor_options = self.actor_options.message_size(size_hint);
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
        let restart = self.restart.unwrap_or(default_restart);
        let shutdown = self.shutdown.unwrap_or(default_shutdown);
        let remove_on_exit = self.remove_on_exit.unwrap_or(true);
        DynamicChildOptions {
            restart,
            shutdown,
            restart_config: self.restart_config,
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
    restart_config: Option<RestartConfig>,
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
}

impl std::fmt::Debug for LifecycleWatchGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleWatchGuard")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl Drop for LifecycleWatchGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn spawn_lifecycle_watch_to<M, F>(
    mut lifecycle: ChildLifecycleWatch,
    target: ActorRef<M>,
    mut map: F,
) -> LifecycleWatchGuard
where
    M: Send + 'static,
    F: FnMut(ChildLifecycleEvent) -> M + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();

    tokio::spawn(async move {
        let _cancel_on_exit = CancelOnDrop::new(task_cancellation.clone());
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

/// Owns a spawned actor runtime.
///
/// Dropping this value requests graceful shutdown. Handles cloned from it are
/// non-owning and may be dropped without affecting runtime lifetime.
/// Because `Runtime` dereferences to [`RuntimeHandle`], `runtime.clone()` also
/// compiles, but returns a non-owning `RuntimeHandle`; use [`handle`](Self::handle)
/// to make that ownership transition explicit.
#[must_use = "dropping the runtime requests graceful shutdown"]
pub struct Runtime {
    supervisor: RunningSupervisor,
    handle: RuntimeHandle,
}

impl Runtime {
    pub(crate) fn new(supervisor: RunningSupervisor, actors: Arc<ActorRuntimeState>) -> Self {
        let handle = RuntimeHandle::new(supervisor.handle(), actors);
        Self { supervisor, handle }
    }

    /// Returns a non-owning runtime control and observation handle.
    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    /// Requests graceful shutdown without waiting for completion.
    pub fn shutdown(&self) {
        self.supervisor.shutdown();
    }

    /// Requests graceful shutdown and waits for completion.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Waits for the runtime to stop.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }
}

impl std::ops::Deref for Runtime {
    type Target = RuntimeHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// Cheaply cloneable, non-owning runtime control and observation surface.
///
/// Dropping any root or nested handle leaves the runtime running. A spawned
/// root remains alive until its owning [`Runtime`] is shut down or dropped.
#[derive(Clone)]
pub struct RuntimeHandle {
    supervisor: SupervisorHandle,
    actors: Arc<ActorRuntimeState>,
}

/// Runtime-membership capability for a dynamic actor scope.
///
/// Obtain this value with [`RuntimeHandle::dynamic`]. The wrapper keeps the
/// scope's ordinary control and observation identity while exposing the five
/// operations that can change membership.
#[derive(Clone, Debug)]
pub struct DynamicRuntime {
    actors: Arc<ActorRuntimeState>,
    supervisor: DynamicSupervisorHandle,
}

impl RuntimeHandle {
    pub(crate) fn new(supervisor: SupervisorHandle, actors: Arc<ActorRuntimeState>) -> Self {
        Self { supervisor, actors }
    }

    pub(crate) fn unavailable() -> Self {
        static UNAVAILABLE: OnceLock<RuntimeHandle> = OnceLock::new();

        UNAVAILABLE
            .get_or_init(|| {
                let builder = kokage_supervisor::Supervisor::dynamic();
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

    /// Returns this scope's dynamic-membership capability.
    ///
    /// Ordered root and subtree handles return `None`; dynamic scopes return
    /// `Some` before and after spawn.
    pub fn dynamic(&self) -> Option<DynamicRuntime> {
        self.supervisor.dynamic().map(|supervisor| DynamicRuntime {
            actors: Arc::clone(&self.actors),
            supervisor,
        })
    }

    /// Returns the actor-aware handle for a direct runtime subtree.
    ///
    /// `None` means that this runtime has no registered subtree with `id`.
    pub fn subtree(&self, id: &str) -> Option<RuntimeHandle> {
        self.subtree_membership(id, None)
    }

    fn subtree_membership(&self, id: &str, lineage: Option<u64>) -> Option<RuntimeHandle> {
        runtime_subtree_membership(
            __private::attached_children::<RuntimeAttachment>(&self.supervisor),
            &self.actors,
            id,
            lineage,
        )
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
    /// children, including restart scheduling.
    ///
    /// Create the watch before reading [`snapshot`](Self::snapshot), then
    /// discard child transitions whose `seq` is at most the snapshot's
    /// `lifecycle_seq` to obtain a gap-free state-plus-stream view. Pre-spawn snapshots
    /// already project configured children, so reducers should apply their
    /// later `Added` events as idempotent membership upserts. Use a
    /// [`subtree`](Self::subtree) handle for nested scopes.
    pub fn watch_lifecycle(&self) -> ChildLifecycleWatch {
        self.supervisor.watch_lifecycle()
    }

    /// Arms a watch for the next restart of `child_id`.
    ///
    /// The lifecycle subscription and current generation are captured before
    /// this method returns. The restart may therefore be triggered before the
    /// returned future is first polled without losing its `Started` event.
    /// A restart already reflected in the captured generation becomes the
    /// baseline, even if its `Started` event is buffered, so only a later
    /// generation can complete the future.
    ///
    /// Returns `None` if the child is not currently supervised, is removed
    /// before restarting, the watch lags, or this runtime identity becomes
    /// terminal before the restart is observed.
    pub fn restart_of(
        &self,
        child_id: &str,
    ) -> impl std::future::Future<Output = Option<u64>> + Send + 'static {
        self.supervisor.restart_of(child_id)
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
        F: FnMut(ChildLifecycleEvent) -> M + Send + 'static,
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
    ///
    /// Unlike [`ActorRef::stats`], each returned sample populates
    /// [`ActorStats::supervisor_path`] and [`ActorStats::lineage`] from the
    /// current runtime membership. Message-size totals remain `None` unless
    /// observation was enabled in that actor's [`ActorOptions`].
    pub fn actor_stats(&self) -> Vec<ActorStats> {
        let mut runtime_owners = HashMap::from([(Vec::new(), Arc::clone(&self.actors))]);
        let mut stats = Vec::new();

        for attached in __private::attached_children::<RuntimeAttachment>(&self.supervisor) {
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
    pub fn subscribe_snapshots(&self) -> SupervisorSnapshotReceiver {
        self.supervisor.subscribe_snapshots()
    }
}

fn runtime_subtree_membership(
    attached_children: Vec<__private::AttachedChild<RuntimeAttachment>>,
    actors: &Arc<ActorRuntimeState>,
    id: &str,
    lineage: Option<u64>,
) -> Option<RuntimeHandle> {
    attached_children.into_iter().find_map(|attached| {
        let [identity] = attached.path() else {
            return None;
        };
        if identity.id != id
            || lineage.is_some_and(|lineage| identity.lineage != lineage)
            || !attached.attachment().belongs_to(actors)
        {
            return None;
        }
        let RuntimeAttachmentKind::Subtree(subtree_actors) = &attached.attachment().kind else {
            return None;
        };
        Some(RuntimeHandle::new(
            attached.supervisor()?.clone(),
            Arc::clone(subtree_actors),
        ))
    })
}

impl DynamicRuntime {
    /// Builds and adds an actor-aware runtime subtree dynamically.
    ///
    /// The returned handle can add actors or further subtrees, and recursive
    /// [`RuntimeHandle::actor_stats`] include the new subtree. Removing the
    /// child detaches its actor metadata with the supervisor membership;
    /// retained subtree handles then fail control operations with
    /// [`ControlError::Unavailable`].
    ///
    /// If the subtree itself restarts, its statically declared graph actors
    /// are recreated, while children added later through the returned handle
    /// are lost and must be replayed by the application. If this scope's
    /// supervisor restarts, the dynamically added subtree is not recreated.
    ///
    /// Restart intensity remains tracked per child across this boundary.
    /// Dynamic additions start immediately and dynamic siblings stop
    /// concurrently under one shared maximum-grace deadline. Use
    /// [`RuntimeHandle::wait_started`] when readiness is needed.
    ///
    /// Both failure phases use [`ControlError::Rejected`]: first the supplied
    /// tree is lowered and validated, then the parent validates insertion of
    /// the resulting child. For example, a duplicate actor binding fails the
    /// first phase, while an already-occupied child id fails the second. The
    /// nested [`SupervisorBuildError`] identifies the validation rule, but a
    /// caller should not infer the phase solely from an error variant because
    /// some rules, such as duplicate child ids, can arise in either phase.
    /// Any error consumes the supplied tree and makes handles previously issued
    /// from it terminal.
    pub async fn add_subtree(
        &self,
        id: impl Into<String>,
        tree: impl Into<crate::TreeNode>,
    ) -> Result<RuntimeHandle, ControlError> {
        let id = id.into();
        let parts = tree.into().into_parts();
        let (nested_supervisor, nested_actors) = parts.map_err(ControlError::Rejected)?;
        let lineage = self
            .supervisor
            .add_child(__private::attach(
                ChildSpec::supervisor(id.clone(), nested_supervisor),
                RuntimeAttachment::subtree(&self.actors, Arc::clone(&nested_actors)),
            ))
            .await?;
        runtime_subtree_membership(
            __private::dynamic_attached_children::<RuntimeAttachment>(&self.supervisor),
            &self.actors,
            &id,
            Some(lineage),
        )
        .ok_or(ControlError::Unavailable)
    }

    /// Adds an arbitrary supervised task child to this runtime.
    ///
    /// This is the task-level counterpart to adding an actor. Success means
    /// the membership was inserted and startup was scheduled, and returns the
    /// lineage assigned to that membership. Task children do not appear in
    /// [`RuntimeHandle::actor_stats`], but remain visible through snapshots and
    /// lifecycle watches.
    pub async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.supervisor.add_child(child).await
    }

    /// Adds a supervised runtime actor with default options and returns its
    /// stable typed ref.
    ///
    /// See [`Self::add_actor_with`] for child-id, readiness, and explicit
    /// mailbox-option details.
    pub async fn add_actor<F>(
        &self,
        label: impl Into<String>,
        factory: F,
    ) -> Result<ActorRef<<F::Actor as RawActor>::Msg>, ControlError>
    where
        F: ActorFactory,
    {
        self.add_actor_with(label, factory, DynamicActorOptions::new())
            .await
    }

    /// Adds a supervised runtime actor from an incarnation factory with
    /// explicit options and returns its stable typed ref.
    ///
    /// The actor's label is also its direct supervisor child id, so it can be
    /// removed later through the dynamic capability. See [`ActorFactory`] for
    /// the incarnation lifecycle contract. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`RuntimeHandle::wait_started`] retains
    /// the stronger readiness contract. A zero
    /// [`ActorOptions::mailbox_capacity`] is rejected with
    /// [`ControlError::Rejected`].
    pub async fn add_actor_with<F>(
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
            .map_err(|error: ActorOptionsValidationError| {
                ControlError::Rejected(SupervisorBuildError::InvalidConfig(error.message()))
            })?;
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
                .restart_config(options.restart_config)
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
    /// [`TrySendError::Closed`](crate::TrySendError::Closed), while an awaited
    /// `send` waits and then returns [`SendError`](crate::SendError).
    /// Removal does not return queued messages: end-to-end delivery ownership
    /// belongs in an application acknowledgement and replay protocol.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.supervisor.remove_child(id).await
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
    pub(crate) restart_config: Option<RestartConfig>,
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
            restart_config: None,
            remove_on_exit: false,
            children: None,
        }
    }

    pub(crate) fn child_id(mut self, child_id: Option<String>) -> Self {
        self.child_id = child_id;
        self
    }

    pub(crate) fn restart_config(mut self, config: Option<RestartConfig>) -> Self {
        self.restart_config = config;
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
        restart_config,
        remove_on_exit,
        children,
    } = options;
    let actor_id = child_id.unwrap_or_else(|| actor.label().to_owned());
    let attachment = RuntimeAttachment::actor(owner, actor.clone());
    let guard = Arc::new(TerminateBindingOnDrop::new(actor));
    let child_guard = Arc::clone(&guard);
    let actor_owner = Arc::clone(owner);
    let mut child = __private::attach(
        ChildSpec::task(actor_id, move |ctx| {
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
        }),
        attachment,
    )
    .wait_for_ready()
    .restart(restart)
    .remove_on_exit(remove_on_exit)
    .shutdown(shutdown);

    if let Some(config) = restart_config {
        child = child.restart_config(config);
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
    use crate::{DynamicTree, OrderedTree};

    #[test]
    fn unavailable_runtime_handle_is_cached() {
        let first = super::RuntimeHandle::unavailable();
        let second = super::RuntimeHandle::unavailable();

        assert!(std::sync::Arc::ptr_eq(&first.actors, &second.actors));
    }

    #[tokio::test]
    async fn subtree_membership_lookup_rejects_a_same_id_replacement() {
        let root = DynamicTree::new().spawn().expect("runtime builds");
        root.dynamic()
            .expect("dynamic scope")
            .add_subtree("workers", OrderedTree::new())
            .await
            .expect("first subtree added");
        let first_lineage = root
            .snapshot()
            .child("workers")
            .expect("first membership is visible")
            .lineage;

        root.dynamic()
            .expect("dynamic scope")
            .remove_child("workers")
            .await
            .expect("first subtree removed");
        root.dynamic()
            .expect("dynamic scope")
            .add_subtree("workers", OrderedTree::new())
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
