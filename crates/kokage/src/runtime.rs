use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorRef, SealedActorSpec,
    actor::{
        ActorNode, ActorOptionsValidationError, ActorStats, RunnableActor, RunnableActorBuilder,
        SupervisorPathSegment,
    },
    supervisor::{
        __private::{self, AttachedChildIdentity, guard_from_probe},
        BuildError, CancellationToken, ChildSpec, CompletionError, CompletionOutcome, ControlError,
        DynamicSupervisorHandle, Guard, LifecycleEvent, LifecycleWatch, Restart, RunningSupervisor,
        Shutdown, ShutdownMode, SupervisorError, SupervisorHandle, SupervisorSnapshot,
        SupervisorSnapshotReceiver,
    },
};

#[derive(Debug)]
pub(crate) struct ActorRuntimeState {
    config: Mutex<ActorRuntimeConfig>,
}

#[derive(Debug)]
struct ActorRuntimeConfig {
    actor_builder: RunnableActorBuilder,
    default_restart: Restart,
    default_shutdown: Shutdown,
}

impl ActorRuntimeState {
    pub(crate) fn new(
        actor_builder: RunnableActorBuilder,
        default_restart: Restart,
        default_shutdown: Shutdown,
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
        default_restart: Restart,
        default_shutdown: Shutdown,
    ) {
        *self.config.lock().unwrap_or_else(PoisonError::into_inner) = ActorRuntimeConfig {
            actor_builder,
            default_restart,
            default_shutdown,
        };
    }

    fn actor_defaults(&self) -> (Restart, Shutdown) {
        let config = self.config.lock().unwrap_or_else(PoisonError::into_inner);
        (config.default_restart, config.default_shutdown)
    }

    pub(crate) fn actor_builder(&self) -> RunnableActorBuilder {
        // Construction runs the caller's factory, which may reach back into
        // this runtime. Release the config lock first so that re-entry cannot
        // deadlock on a non-reentrant mutex.
        self.config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .actor_builder
            .clone()
    }

    fn make_actor<M: Send + 'static>(&self, spec: SealedActorSpec<M>) -> ActorNode {
        spec.into_node(&self.actor_builder())
    }

    pub(crate) fn materialize_actor_node(&self, actor: ActorNode) -> ActorNode {
        actor.materialize(&self.actor_builder())
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
    restart: Restart,
    shutdown: Shutdown,
}

fn spawn_lifecycle_watch_to<M, F>(
    mut lifecycle: LifecycleWatch,
    target: ActorRef<M>,
    mut map: F,
) -> Guard
where
    M: Send + 'static,
    F: FnMut(LifecycleEvent) -> M + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
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
                    crate::supervisor::LifecycleEventKind::Lagged { .. }
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

    let task = task.abort_handle();
    guard_from_probe(cancellation, move || task.is_finished())
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
/// [`DynamicTree::handle`](crate::DynamicTree::handle) returns this type before
/// spawn. After spawn, recover the capability with [`RuntimeHandle::dynamic`].
/// The same method is used for scopes reached by runtime navigation, because
/// the nested scope's kind is not statically known.
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
                let builder = crate::supervisor::Supervisor::dynamic();
                let supervisor = builder.handle();
                drop(builder);
                Self::new(
                    supervisor,
                    Arc::new(ActorRuntimeState::new(
                        RunnableActorBuilder::new(),
                        Restart::default(),
                        Shutdown::default(),
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
    /// Direct children are addressed by their one scope-local id. Use a
    /// [`subtree`](Self::subtree) handle for children of a nested scope. An
    /// unknown child returns [`CompletionError::UnknownChild`]. See
    /// [`CompletionOutcome`] for the distinction between completion and the
    /// supervisor stopping first.
    ///
    /// On a dynamic scope, use [`DynamicRuntimeHandle::wait_completed`] when
    /// ids may be added later instead of validating them immediately.
    pub async fn wait_completed<I, S>(&self, ids: I) -> Result<CompletionOutcome, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_completed(ids).await
    }

    /// Shuts this runtime down once every named child has completed.
    ///
    /// Child ids follow the same rules as [`wait_completed`](Self::wait_completed).
    /// On a dynamic scope, use
    /// [`DynamicRuntimeHandle::shutdown_on_completion`] when ids may be added
    /// later.
    ///
    /// Arm this from a pre-spawn handle when fast children could complete
    /// immediately. The returned guard must be retained; dropping it cancels
    /// the completion watch and leaves the runtime running.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_on_completion<I, S>(&self, ids: I) -> Guard
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
    pub fn watch_lifecycle_to<M, F>(&self, target: &ActorRef<M>, map: F) -> Guard
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
    /// observation was enabled with
    /// [`ActorSpec::message_size`](crate::ActorSpec::message_size).
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

    /// Erases the statically known dynamic capability from this handle.
    pub fn into_runtime_handle(self) -> RuntimeHandle {
        self.handle
    }

    /// Waits for named children to appear and then complete.
    ///
    /// Absent ids are treated as future dynamic membership. The wait may
    /// therefore remain pending indefinitely while this runtime is still
    /// running. Once present, a child follows the same completion rules as
    /// [`RuntimeHandle::wait_completed`].
    ///
    /// This inherent method intentionally shadows the same-named
    /// [`RuntimeHandle`] method reached through [`Deref`](std::ops::Deref).
    /// Call `RuntimeHandle::wait_completed(&handle, ids)` or first use
    /// [`into_runtime_handle`](Self::into_runtime_handle) to validate ids
    /// immediately and receive [`CompletionError::UnknownChild`] for an absent
    /// membership.
    pub async fn wait_completed<I, S>(&self, ids: I) -> CompletionOutcome
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.handle.supervisor.wait_completed_dynamic(ids).await
    }

    /// Shuts this runtime down once named children appear and all complete.
    ///
    /// This is the fire-and-forget counterpart to
    /// [`wait_completed`](Self::wait_completed). The returned guard must be
    /// retained; dropping it cancels the completion watch and leaves the
    /// runtime running.
    ///
    /// This inherent method intentionally shadows the same-named
    /// [`RuntimeHandle`] method reached through [`Deref`](std::ops::Deref).
    /// Call `RuntimeHandle::shutdown_on_completion(&handle, ids)` or first use
    /// [`into_runtime_handle`](Self::into_runtime_handle) to validate ids
    /// immediately instead of waiting for future membership.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_on_completion<I, S>(&self, ids: I) -> Guard
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.handle.supervisor.shutdown_on_dynamic_completion(ids)
    }

    /// Builds and adds an actor-aware runtime subtree dynamically.
    ///
    /// The returned handle can add actors or further subtrees, and recursive
    /// [`RuntimeHandle::actor_stats`] include the new subtree. Removing the
    /// child detaches its actor metadata with the supervisor membership;
    /// retained subtree handles then fail control operations with
    /// [`ControlError::Unavailable`].
    ///
    /// If the subtree itself restarts, its statically declared actors
    /// are recreated, while children added later through the returned handle
    /// are lost and must be replayed by the application. If this scope's
    /// supervisor restarts, the dynamically added subtree is not recreated.
    ///
    /// Restart intensity remains tracked per child across this boundary.
    /// Dynamic additions start immediately and dynamic siblings stop
    /// concurrently under one shared maximum-grace deadline. Use
    /// [`RuntimeHandle::wait_started`] when readiness is needed.
    /// Wrap the supplied tree with [`TreeNode::restart`](crate::TreeNode::restart)
    /// or [`TreeNode::shutdown`](crate::TreeNode::shutdown) to override the
    /// subtree edge's policies in this dynamic parent.
    ///
    /// Both failure phases use [`ControlError::Rejected`]: first the supplied
    /// tree is lowered and validated, then the parent validates insertion of
    /// the resulting child. For example, a duplicate actor binding fails the
    /// first phase, while an already-occupied child id fails the second. The
    /// nested [`BuildError`] identifies the validation rule, but a
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
        let parts = parts.map_err(ControlError::Rejected)?;
        let mut child = ChildSpec::supervisor(id.clone(), parts.supervisor);
        if let Some(restart) = parts.restart {
            child = child.restart(restart);
        }
        if let Some(shutdown) = parts.shutdown {
            child = child.shutdown(shutdown);
        }
        let lineage = self
            .supervisor
            .add_child(__private::attach(
                child,
                RuntimeAttachment::subtree(&self.handle.actors, Arc::clone(&parts.actors)),
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
    /// The actor id is its direct supervisor child id, so it can be removed
    /// later through the dynamic capability. See [`crate::ActorFactory`] for
    /// the incarnation lifecycle contract. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`RuntimeHandle::wait_started`] retains
    /// the stronger readiness contract. A zero
    /// [`ActorSpec::mailbox_capacity`](crate::ActorSpec::mailbox_capacity) is rejected with
    /// [`ControlError::Rejected`].
    pub async fn add_actor<M: Send + 'static>(
        &self,
        spec: impl Into<SealedActorSpec<M>>,
    ) -> Result<ActorRef<M>, ControlError> {
        let spec = spec.into();
        let actor_ref = ActorRef::from_core(spec.binding(), None);
        spec.actor_options
            .validate()
            .map_err(|error: ActorOptionsValidationError| {
                ControlError::Rejected(BuildError::InvalidConfig(error.message()))
            })?;
        let (default_restart, default_shutdown) = self.handle.actors.actor_defaults();
        let dynamic_options = DynamicChildOptions {
            restart: spec.restart.unwrap_or(default_restart),
            shutdown: spec.shutdown.unwrap_or(default_shutdown),
        };
        let actor = self.handle.actors.make_actor(spec);
        self.add_constructed_actor(
            (
                actor
                    .actor
                    .expect("dynamic ActorSpec materialization produced a runnable actor"),
                actor_ref,
            ),
            dynamic_options,
        )
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
            ActorChildOptions::new(options.restart, options.shutdown),
        );
        self.supervisor.add_child(child).await?;

        Ok(actor_ref)
    }

    /// Removes a child from the supervisor.
    ///
    /// Removal marks the membership as removing and starts its configured
    /// shutdown. When cooperative shutdown completes within its grace period,
    /// an [`Actor`](crate::Actor) stops its normal receive loop, closes external
    /// intake, applies its [`Shutdown`](crate::Shutdown), runs `on_stop`,
    /// makes the mailbox binding terminal, and is then detached. Immediate
    /// abort, or expiry of the cooperative grace period, can skip any remaining
    /// drain or hook work before detachment. The returned future completes
    /// after detachment (or after the configured shutdown backstop aborts it).
    ///
    /// A send racing with removal may still be accepted. With
    /// [`Shutdown::drain_for`](crate::Shutdown::drain_for), work accepted before
    /// drain closes intake belongs to the queued prefix handled before
    /// `on_stop`. With
    /// [`Shutdown::discard_after_current`](crate::Shutdown::discard_after_current),
    /// accepted work that remains queued is dropped. Once the actor closes intake,
    /// `try_send` may briefly return
    /// [`TrySendError::Closed`](crate::TrySendError::Closed), while an awaited
    /// `send` waits and then returns [`SendError`](crate::SendError).
    /// Removal does not return queued messages: end-to-end delivery ownership
    /// belongs in an application acknowledgement and replay protocol.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.supervisor.remove_child(id).await
    }
}

impl std::ops::Deref for DynamicRuntimeHandle {
    type Target = RuntimeHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
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
    pub(crate) restart: Restart,
    pub(crate) shutdown: Shutdown,
}

impl ActorChildOptions {
    pub(crate) fn new(restart: Restart, shutdown: Shutdown) -> Self {
        Self { restart, shutdown }
    }
}

pub(crate) fn actor_child_spec(
    actor: RunnableActor,
    owner: &Arc<ActorRuntimeState>,
    options: ActorChildOptions,
) -> ChildSpec {
    let ActorChildOptions { restart, shutdown } = options;
    let actor_id = actor.label().to_owned();
    let attachment = RuntimeAttachment::actor(owner, actor.clone());
    let guard = Arc::new(TerminateBindingOnDrop::new(actor));
    let child_guard = Arc::clone(&guard);
    let actor_owner = Arc::clone(owner);
    __private::attach(
        ChildSpec::task(actor_id, move |ctx| {
            let actor = child_guard.actor.clone();
            let supervisor = RuntimeHandle::new(ctx.supervisor(), Arc::clone(&actor_owner));
            async move {
                actor
                    .run_until_ready(
                        ctx.shutdown_token().cancelled(),
                        ctx.abort_token().cancelled(),
                        restart,
                        shutdown.mode() == ShutdownMode::Drain,
                        supervisor,
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
    .shutdown(shutdown)
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
        Actor, ActorResult, ActorSpec, BuildError, Context, DynamicRuntimeHandle, DynamicTree,
        OrderedTree, Restart, Runtime, RuntimeHandle,
    };

    #[test]
    fn tree_root_types_preserve_statically_known_membership() {
        let ordered_spawn: fn(OrderedTree) -> Result<Runtime, BuildError> = OrderedTree::spawn;
        let ordered_handle: fn(&OrderedTree) -> RuntimeHandle = OrderedTree::handle;
        let dynamic_spawn: fn(DynamicTree) -> Result<Runtime, BuildError> = DynamicTree::spawn;
        let dynamic_handle: fn(&DynamicTree) -> DynamicRuntimeHandle = DynamicTree::handle;

        let _ = (ordered_spawn, ordered_handle, dynamic_spawn, dynamic_handle);
    }

    struct FailsOnMessage;

    impl Actor for FailsOnMessage {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ActorResult {
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
        let static_spec = ActorSpec::new("static", || FailsOnMessage).restart(Restart::never());
        let (static_spec, static_ref) = static_spec.actor_ref();
        let static_runtime = OrderedTree::new()
            .actor(static_spec)
            .spawn()
            .expect("static runtime builds");
        static_runtime
            .handle()
            .wait_started()
            .await
            .expect("static actor starts");
        let mut static_snapshots = static_runtime.handle().subscribe_snapshots();
        static_ref.send(()).await.expect("static message accepted");
        static_snapshots
            .wait_for(|snapshot| {
                snapshot
                    .child("static")
                    .is_some_and(|child| child.state.is_terminal())
            })
            .await
            .expect("static terminal membership remains visible");
        static_runtime
            .shutdown_and_wait()
            .await
            .expect("static runtime shuts down");

        let dynamic_runtime = DynamicTree::new().spawn().expect("dynamic runtime builds");
        let dynamic = dynamic_runtime
            .handle()
            .dynamic()
            .expect("dynamic root exposes membership capability");
        let dynamic_ref = dynamic
            .add_actor(ActorSpec::new("dynamic", || FailsOnMessage).restart(Restart::never()))
            .await
            .expect("dynamic actor is inserted");
        dynamic_runtime
            .handle()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut dynamic_snapshots = dynamic_runtime.handle().subscribe_snapshots();
        dynamic_ref
            .send(())
            .await
            .expect("dynamic message accepted");
        dynamic_snapshots
            .wait_for(|snapshot| {
                snapshot
                    .child("dynamic")
                    .is_some_and(|child| child.state.is_terminal())
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
        let dynamic = runtime
            .handle()
            .dynamic()
            .expect("dynamic root exposes membership capability");
        let actor_ref = dynamic
            .add_actor(
                ActorSpec::new("ephemeral", || FailsOnMessage)
                    .restart(Restart::never().remove_when_done()),
            )
            .await
            .expect("dynamic actor is inserted");
        runtime
            .handle()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut snapshots = runtime.handle().subscribe_snapshots();
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
        let dynamic = root
            .handle()
            .dynamic()
            .expect("dynamic root exposes membership capability");
        dynamic
            .add_subtree("workers", OrderedTree::new())
            .await
            .expect("first subtree added");
        let first_lineage = root
            .handle()
            .snapshot()
            .child("workers")
            .expect("first membership is visible")
            .lineage;

        dynamic
            .remove_child("workers")
            .await
            .expect("first subtree removed");
        dynamic
            .add_subtree("workers", OrderedTree::new())
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
