use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorRef, ActorSpec, TerminalMembership,
    actor::{
        ActorNode, ActorOptionsValidationError, ActorStats, CancelOnDrop, RunnableActor,
        RunnableActorBuilder, SupervisorPathSegment,
    },
};
use kokage_supervisor::{
    __private::{self, AttachedChildIdentity},
    CancellationToken, ChildSpec, CompletionError, CompletionGuard, CompletionOutcome,
    ControlError, DynamicSupervisorHandle, LifecycleEvent, LifecycleWatch, RestartConfig,
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

    fn make_actor<M: Send + 'static>(&self, spec: ActorSpec<M>) -> ActorNode {
        // Construction runs the caller's factory, which may reach back into
        // this runtime. Release the config lock first so that re-entry cannot
        // deadlock on a non-reentrant mutex.
        let actor_builder = self
            .config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .actor_builder
            .clone();
        spec.into_node(&actor_builder)
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
            if !event.is_child_transition()
                && !matches!(
                    event.kind,
                    kokage_supervisor::LifecycleEventKind::Lagged { .. }
                )
            {
                continue;
            }

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
/// Use [`handle`](Self::handle) to make the transition from the owning runtime
/// to a non-owning [`RuntimeHandle`] explicit.
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

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// Owns a spawned actor runtime whose root has dynamic membership.
///
/// Unlike a plain [`Runtime`], this type preserves the capability established
/// by [`DynamicTree::spawn`](crate::DynamicTree::spawn), so actors and subtrees
/// can be added or removed without rediscovering the root scope's kind. Use
/// [`handle`](Self::handle) to clone a non-owning, membership-capable handle,
/// or [`runtime`](Self::runtime) / [`into_runtime`](Self::into_runtime)
/// when only the plain runtime ownership surface is needed.
#[must_use = "dropping the runtime requests graceful shutdown"]
pub struct DynamicRuntime {
    runtime: Runtime,
    handle: DynamicRuntimeHandle,
}

impl DynamicRuntime {
    pub(crate) fn new(supervisor: RunningSupervisor, actors: Arc<ActorRuntimeState>) -> Self {
        let runtime = Runtime::new(supervisor, actors);
        let handle = DynamicRuntimeHandle::new(runtime.handle());
        Self { runtime, handle }
    }

    /// Returns a non-owning runtime handle that preserves dynamic membership.
    pub fn handle(&self) -> DynamicRuntimeHandle {
        self.handle.clone()
    }

    /// Borrows the plain owning runtime.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Erases the statically known dynamic capability from this owner.
    pub fn into_runtime(self) -> Runtime {
        self.runtime
    }

    /// Requests graceful shutdown without waiting for completion.
    pub fn shutdown(&self) {
        self.runtime.shutdown();
    }

    /// Requests graceful shutdown and waits for completion.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.runtime.shutdown_and_wait().await
    }

    /// Waits for the runtime to stop.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.runtime.wait().await
    }

    /// Builds and adds an actor-aware runtime subtree dynamically.
    pub async fn add_subtree(
        &self,
        id: impl Into<String>,
        tree: impl Into<crate::TreeNode>,
    ) -> Result<RuntimeHandle, ControlError> {
        self.handle.add_subtree(id, tree).await
    }

    /// Adds an arbitrary supervised task child to this runtime.
    pub async fn add_child(&self, child: ChildSpec) -> Result<u64, ControlError> {
        self.handle.add_child(child).await
    }

    /// Adds one actor declaration and returns its stable typed ref.
    pub async fn add_actor<M: Send + 'static>(
        &self,
        spec: ActorSpec<M>,
    ) -> Result<ActorRef<M>, ControlError> {
        self.handle.add_actor(spec).await
    }

    /// Removes a child from the supervisor.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.handle.remove_child(id).await
    }
}

impl AsRef<Runtime> for DynamicRuntime {
    fn as_ref(&self) -> &Runtime {
        self.runtime()
    }
}

impl From<DynamicRuntime> for Runtime {
    fn from(runtime: DynamicRuntime) -> Self {
        runtime.into_runtime()
    }
}

impl std::fmt::Debug for DynamicRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicRuntime").finish_non_exhaustive()
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

/// Non-owning runtime handle for a statically known dynamic actor scope.
///
/// [`DynamicTree::handle`](crate::DynamicTree::handle) and
/// [`DynamicRuntime::handle`] return this type directly. For scopes reached by
/// runtime navigation, use [`RuntimeHandle::dynamic`], because the nested
/// scope's kind is not statically known.
#[derive(Clone, Debug)]
pub struct DynamicRuntimeHandle {
    handle: RuntimeHandle,
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
    pub fn dynamic(&self) -> Option<DynamicRuntimeHandle> {
        self.supervisor
            .dynamic()
            .map(|supervisor| DynamicRuntimeHandle {
                handle: self.clone(),
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
    /// scopes. An unknown child returns [`CompletionError::UnknownChild`]. See
    /// [`CompletionOutcome`] for the distinction between completion and the
    /// supervisor stopping first.
    pub async fn wait_completed<I, S>(&self, ids: I) -> Result<CompletionOutcome, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_completed(ids).await
    }

    /// Waits for named children to be added dynamically and then complete.
    pub async fn wait_completed_dynamic<I, S>(&self, ids: I) -> CompletionOutcome
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_completed_dynamic(ids).await
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

    /// Returns the ordered lifecycle stream for this runtime's entire tree.
    ///
    /// Create the watch before reading [`snapshot`](Self::snapshot), then
    /// discard child transitions whose `seq()` is at most the snapshot's
    /// `lifecycle_seq` to obtain a gap-free state-plus-stream view. Pre-spawn snapshots
    /// already project configured children, so reducers should apply their
    /// later `ChildAdded` events as idempotent membership upserts. Call
    /// [`LifecycleWatch::direct_children`] for only this scope.
    pub fn watch_lifecycle(&self) -> LifecycleWatch {
        self.supervisor.watch_lifecycle()
    }

    /// Pumps this scope's direct-child lifecycle events into `target` using
    /// its ordinary mailbox policy.
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
        spawn_lifecycle_watch_to(
            self.watch_lifecycle().direct_children(),
            target.clone(),
            map,
        )
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

impl DynamicRuntimeHandle {
    pub(crate) fn new(handle: RuntimeHandle) -> Self {
        let supervisor = handle
            .supervisor
            .dynamic()
            .expect("dynamic runtime handle must refer to a dynamic scope");
        Self { handle, supervisor }
    }

    /// Borrows this capability as the plain runtime handle shared by all
    /// scope kinds.
    pub fn as_runtime_handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    /// Erases the statically known dynamic capability from this handle.
    pub fn into_runtime_handle(self) -> RuntimeHandle {
        self.handle
    }

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
                RuntimeAttachment::subtree(&self.handle.actors, Arc::clone(&nested_actors)),
            ))
            .await?;
        runtime_subtree_membership(
            __private::dynamic_attached_children::<RuntimeAttachment>(&self.supervisor),
            &self.handle.actors,
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

    /// Adds one actor declaration and returns its stable typed ref.
    ///
    /// The actor's label is also its direct supervisor child id, so it can be
    /// removed later through the dynamic capability. See [`ActorFactory`] for
    /// the incarnation lifecycle contract. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`RuntimeHandle::wait_started`] retains
    /// the stronger readiness contract. A zero
    /// [`ActorOptions::mailbox_capacity`] is rejected with
    /// [`ControlError::Rejected`].
    pub async fn add_actor<M: Send + 'static>(
        &self,
        spec: ActorSpec<M>,
    ) -> Result<ActorRef<M>, ControlError> {
        let actor_ref = spec.actor_ref();
        spec.actor_options
            .validate()
            .map_err(|error: ActorOptionsValidationError| {
                ControlError::Rejected(SupervisorBuildError::InvalidConfig(error.message()))
            })?;
        let (default_restart, default_shutdown) = self.handle.actors.actor_defaults();
        let dynamic_options = DynamicChildOptions {
            restart: spec.restart.unwrap_or(default_restart),
            shutdown: spec.shutdown.unwrap_or(default_shutdown),
            restart_config: spec.restart_config,
            remove_on_exit: matches!(spec.terminal_membership, TerminalMembership::Remove),
        };
        let actor = self.handle.actors.make_actor(spec);
        self.add_constructed_actor((actor.actor, actor_ref), dynamic_options)
            .await
    }

    async fn add_constructed_actor<M>(
        &self,
        (actor, actor_ref): (RunnableActor, ActorRef<M>),
        options: DynamicChildOptions,
    ) -> Result<ActorRef<M>, ControlError> {
        let child = actor_child_spec(
            actor.clone(),
            &self.handle.actors,
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

impl AsRef<RuntimeHandle> for DynamicRuntimeHandle {
    fn as_ref(&self) -> &RuntimeHandle {
        self.as_runtime_handle()
    }
}

impl From<DynamicRuntimeHandle> for RuntimeHandle {
    fn from(handle: DynamicRuntimeHandle) -> Self {
        handle.into_runtime_handle()
    }
}

impl std::ops::Deref for DynamicRuntimeHandle {
    type Target = RuntimeHandle;

    fn deref(&self) -> &Self::Target {
        self.as_runtime_handle()
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
    .terminal_membership(if remove_on_exit {
        TerminalMembership::Remove
    } else {
        TerminalMembership::Retain
    })
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
    use crate::{
        Actor, ActorResult, ActorSpec, DynamicRuntime, DynamicRuntimeHandle, DynamicTree,
        MessageContext, OrderedTree, RestartPolicy, Runtime, RuntimeHandle, SupervisorBuildError,
        TerminalMembership,
    };

    #[test]
    fn tree_root_types_preserve_statically_known_membership() {
        let ordered_spawn: fn(OrderedTree) -> Result<Runtime, SupervisorBuildError> =
            OrderedTree::spawn;
        let ordered_handle: fn(&OrderedTree) -> RuntimeHandle = OrderedTree::handle;
        let dynamic_spawn: fn(DynamicTree) -> Result<DynamicRuntime, SupervisorBuildError> =
            DynamicTree::spawn;
        let dynamic_handle: fn(&DynamicTree) -> DynamicRuntimeHandle = DynamicTree::handle;

        let _ = (ordered_spawn, ordered_handle, dynamic_spawn, dynamic_handle);
    }

    struct FailsOnMessage;

    impl Actor for FailsOnMessage {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
            Err(std::io::Error::other("expected test failure").into())
        }
    }

    #[test]
    fn unavailable_runtime_handle_is_cached() {
        let first = super::RuntimeHandle::unavailable();
        let second = super::RuntimeHandle::unavailable();

        assert!(std::sync::Arc::ptr_eq(&first.actors, &second.actors));
    }

    #[tokio::test]
    async fn actor_spec_defaults_to_retained_membership_in_static_and_dynamic_scopes() {
        let static_spec = ActorSpec::new("static", || FailsOnMessage).restart(RestartPolicy::Never);
        let static_ref = static_spec.actor_ref();
        let static_runtime = OrderedTree::new()
            .actor(static_spec)
            .spawn()
            .expect("static runtime builds");
        static_runtime
            .wait_started()
            .await
            .expect("static actor starts");
        let mut static_snapshots = static_runtime.subscribe_snapshots();
        static_ref.send(()).await.expect("static message accepted");
        static_snapshots
            .wait_for(|snapshot| {
                snapshot
                    .child("static")
                    .is_some_and(|child| child.state.is_stopped())
            })
            .await
            .expect("static terminal membership remains visible");
        static_runtime
            .shutdown_and_wait()
            .await
            .expect("static runtime shuts down");

        let dynamic_runtime = DynamicTree::new().spawn().expect("dynamic runtime builds");
        let dynamic = dynamic_runtime.dynamic().expect("root is dynamic");
        let dynamic_ref = dynamic
            .add_actor(ActorSpec::new("dynamic", || FailsOnMessage).restart(RestartPolicy::Never))
            .await
            .expect("dynamic actor is inserted");
        dynamic_runtime
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut dynamic_snapshots = dynamic_runtime.subscribe_snapshots();
        dynamic_ref
            .send(())
            .await
            .expect("dynamic message accepted");
        dynamic_snapshots
            .wait_for(|snapshot| {
                snapshot
                    .child("dynamic")
                    .is_some_and(|child| child.state.is_stopped())
            })
            .await
            .expect("dynamic terminal membership remains visible");
        dynamic_runtime
            .shutdown_and_wait()
            .await
            .expect("dynamic runtime shuts down");
    }

    #[tokio::test]
    async fn dynamic_membership_removal_is_explicit() {
        let runtime = DynamicTree::new().spawn().expect("dynamic runtime builds");
        let dynamic = runtime.dynamic().expect("root is dynamic");
        let actor_ref = dynamic
            .add_actor(
                ActorSpec::new("ephemeral", || FailsOnMessage)
                    .restart(RestartPolicy::Never)
                    .terminal_membership(TerminalMembership::Remove),
            )
            .await
            .expect("dynamic actor is inserted");
        runtime.wait_started().await.expect("dynamic actor starts");
        let mut snapshots = runtime.subscribe_snapshots();
        assert!(snapshots.latest().child("ephemeral").is_some());
        actor_ref.send(()).await.expect("dynamic message accepted");
        snapshots
            .wait_for(|snapshot| snapshot.child("ephemeral").is_none())
            .await
            .expect("explicitly ephemeral membership is removed");
        runtime
            .shutdown_and_wait()
            .await
            .expect("dynamic runtime shuts down");
    }

    #[tokio::test]
    async fn subtree_membership_lookup_rejects_a_same_id_replacement() {
        let root = DynamicTree::new().spawn().expect("runtime builds");
        root.add_subtree("workers", OrderedTree::new())
            .await
            .expect("first subtree added");
        let first_lineage = root
            .handle()
            .snapshot()
            .child("workers")
            .expect("first membership is visible")
            .lineage;

        root.remove_child("workers")
            .await
            .expect("first subtree removed");
        root.add_subtree("workers", OrderedTree::new())
            .await
            .expect("replacement subtree added");
        let replacement_lineage = root
            .handle()
            .snapshot()
            .child("workers")
            .expect("replacement membership is visible")
            .lineage;

        assert_ne!(first_lineage, replacement_lineage);
        assert!(
            root.handle()
                .subtree_membership("workers", Some(first_lineage))
                .is_none(),
            "a lookup bound to the completed add must not return a same-id replacement"
        );
        assert!(
            root.handle()
                .subtree_membership("workers", Some(replacement_lineage))
                .is_some()
        );

        root.shutdown_and_wait().await.expect("clean shutdown");
    }
}
