use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorFactory, ActorRef, ActorSpec, ExitResult,
    actor::{
        ActorNode, ActorOptionsValidationError, RawActor, RunnableActor, RunnableActorBuilder,
        ScopedActorStats,
    },
    supervisor::{
        __private::{self, AttachedChildIdentity, guard_from_tokens},
        BuildError, CancellationToken, ChildSpec, CompletionError, CompletionOnDrop, ControlError,
        DynamicSupervisorHandle, Guard, LifecycleEvent, LifecycleObservation, LifecycleWatch,
        MailboxShutdown, RestartPolicy, RunningSupervisor, ScopeKind, ScopePathSegment, Shutdown,
        SupervisorError, SupervisorHandle, SupervisorSnapshot, SupervisorSnapshotReceiver,
        TaskSpec,
    },
};

#[derive(Debug)]
pub(crate) struct ActorRuntimeState {
    config: Mutex<ActorRuntimeConfig>,
}

#[derive(Debug)]
struct ActorRuntimeConfig {
    actor_builder: RunnableActorBuilder,
    default_restart: RestartPolicy,
    default_shutdown: Shutdown,
    default_mailbox_shutdown: MailboxShutdown,
}

impl ActorRuntimeState {
    pub(crate) fn new(
        actor_builder: RunnableActorBuilder,
        default_restart: RestartPolicy,
        default_shutdown: Shutdown,
        default_mailbox_shutdown: MailboxShutdown,
    ) -> Self {
        Self {
            config: Mutex::new(ActorRuntimeConfig {
                actor_builder,
                default_restart,
                default_shutdown,
                default_mailbox_shutdown,
            }),
        }
    }

    pub(crate) fn configure(
        &self,
        actor_builder: RunnableActorBuilder,
        default_restart: RestartPolicy,
        default_shutdown: Shutdown,
        default_mailbox_shutdown: MailboxShutdown,
    ) {
        *self.config.lock().unwrap_or_else(PoisonError::into_inner) = ActorRuntimeConfig {
            actor_builder,
            default_restart,
            default_shutdown,
            default_mailbox_shutdown,
        };
    }

    fn actor_defaults(&self) -> (RestartPolicy, Shutdown, MailboxShutdown) {
        let config = self.config.lock().unwrap_or_else(PoisonError::into_inner);
        (
            config.default_restart,
            config.default_shutdown,
            config.default_mailbox_shutdown,
        )
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

    fn make_actor<M: Send + 'static>(&self, spec: ActorSpec<M>) -> ActorNode {
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
    restart: RestartPolicy,
    shutdown: Shutdown,
    mailbox_shutdown: MailboxShutdown,
    remove_when_done: bool,
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
    let (finished, finished_on_drop) = CompletionOnDrop::armed();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        let _finished_on_drop = finished_on_drop;
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

    std::mem::drop(task);
    guard_from_tokens(cancellation, finished)
}

/// Owns a spawned supervision tree.
///
/// Routine root observation and lifecycle operations delegate directly to the
/// root scope. Use [`scope`](Self::scope) when a cheaply cloneable, non-owning
/// [`ScopeRef`] must be passed elsewhere or used for membership mutation.
/// Dropping a `ScopeRef` is inert and does not keep this owner alive; dropping
/// this owner requests graceful shutdown.
#[must_use = "dropping the running tree requests graceful shutdown"]
pub struct RunningTree {
    supervisor: RunningSupervisor,
    scope: ScopeRef,
}

impl RunningTree {
    pub(crate) fn new(supervisor: RunningSupervisor, actors: Arc<ActorRuntimeState>) -> Self {
        let scope = ScopeRef::new(supervisor.handle(), actors);
        Self { supervisor, scope }
    }

    /// Returns the running tree's non-owning root scope reference.
    pub fn scope(&self) -> ScopeRef {
        self.scope.clone()
    }

    /// Returns whether the root scope has ordered or dynamic membership.
    pub fn kind(&self) -> ScopeKind {
        self.scope.kind()
    }

    /// Returns the actor-aware handle for a direct runtime subtree.
    pub fn subtree(&self, id: &str) -> Option<ScopeRef> {
        self.scope.subtree(id)
    }

    /// Returns a clone of the latest root supervisor snapshot.
    pub fn snapshot(&self) -> SupervisorSnapshot {
        self.scope.snapshot()
    }

    /// Returns a receiver that updates when the root snapshot changes.
    pub fn subscribe_snapshots(&self) -> SupervisorSnapshotReceiver {
        self.scope.subscribe_snapshots()
    }

    /// Returns an aligned root snapshot and direct-child lifecycle stream.
    pub fn observe_lifecycle(&self) -> LifecycleObservation {
        self.scope.observe_lifecycle()
    }

    /// Returns the ordered lifecycle stream for the complete root tree.
    pub fn watch_lifecycle(&self) -> LifecycleWatch {
        self.scope.watch_lifecycle()
    }

    /// Pumps direct-child lifecycle events from the root into `target`.
    pub fn watch_lifecycle_to<M, F>(&self, target: &ActorRef<M>, map: F) -> Guard
    where
        M: Send + 'static,
        F: FnMut(LifecycleEvent) -> M + Send + 'static,
    {
        self.scope.watch_lifecycle_to(target, map)
    }

    /// Returns point-in-time actor stats for the root and all nested subtrees.
    pub fn actor_stats(&self) -> Vec<ScopedActorStats> {
        self.scope.actor_stats()
    }

    /// Requests graceful shutdown without waiting for completion.
    pub fn shutdown(&self) {
        self.supervisor.shutdown();
    }

    /// Requests graceful shutdown and waits for completion.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Waits for the running tree to stop.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }

    /// Waits until every current actor child of the root has completed `on_start`.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        self.scope.wait_started().await
    }
}

impl std::fmt::Debug for RunningTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningTree").finish_non_exhaustive()
    }
}

/// A cheaply cloneable, non-owning reference and control capability for a
/// supervision scope.
///
/// As an [`ActorRef`](crate::ActorRef) addresses an actor without owning its
/// runtime, a `ScopeRef` addresses one supervision scope for control,
/// observation, and runtime-checked membership operations. Dropping any root
/// or nested reference leaves the runtime running. A spawned root remains alive
/// until its owning [`RunningTree`] is shut down or dropped.
#[derive(Clone)]
pub struct ScopeRef {
    supervisor: SupervisorHandle,
    actors: Arc<ActorRuntimeState>,
}

impl ScopeRef {
    pub(crate) fn new(supervisor: SupervisorHandle, actors: Arc<ActorRuntimeState>) -> Self {
        Self { supervisor, actors }
    }

    pub(crate) fn unavailable() -> Self {
        static UNAVAILABLE: OnceLock<ScopeRef> = OnceLock::new();

        UNAVAILABLE
            .get_or_init(|| {
                let builder = crate::supervisor::Supervisor::dynamic();
                let supervisor = builder.handle();
                drop(builder);
                Self::new(
                    supervisor,
                    Arc::new(ActorRuntimeState::new(
                        RunnableActorBuilder::new(),
                        RestartPolicy::default(),
                        Shutdown::default(),
                        MailboxShutdown::default(),
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
    ///
    /// Awaiting this from an actor callback in the same scope can block on that
    /// callback returning. The cycle ends only if the actor's shutdown grace
    /// expires and aborts it. An actor in the scope cannot receive this result:
    /// its own exit is part of the shutdown condition. Call [`shutdown`](Self::shutdown)
    /// from that actor and observe completion from outside the scope. A bounded
    /// [`Context::offload`](crate::Context::offload) is appropriate only when
    /// shutting down a different scope that can stop while the actor remains live.
    pub async fn shutdown_and_wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Returns whether this scope has ordered or dynamic membership.
    pub fn kind(&self) -> ScopeKind {
        self.supervisor.kind()
    }

    /// Returns the actor-aware handle for a direct runtime subtree.
    ///
    /// `None` means that this runtime has no registered subtree with `id`.
    pub fn subtree(&self, id: &str) -> Option<ScopeRef> {
        self.subtree_membership(id, None)
    }

    fn subtree_membership(&self, id: &str, lineage: Option<u64>) -> Option<ScopeRef> {
        runtime_subtree_membership(
            __private::attached_children::<RuntimeAttachment>(&self.supervisor),
            &self.actors,
            id,
            lineage,
        )
    }

    /// Waits for the supervisor to stop.
    ///
    /// Awaiting this from an actor callback in the same scope can deadlock when
    /// stopping the scope depends on that callback returning. An actor in the
    /// scope cannot receive this result because its own exit is part of the
    /// wait condition. Observe termination from outside the scope instead. A
    /// bounded [`Context::offload`](crate::Context::offload) is appropriate only
    /// when waiting on a different scope that can stop while the actor remains live.
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }

    /// Waits until all current actor children have completed `on_start`.
    ///
    /// An actor must not await its enclosing scope's readiness from its own
    /// `on_start`: its readiness is reported only after that callback returns.
    /// Ordered startup can create the same cycle through a later sibling scope.
    /// Use [`Context::offload`](crate::Context::offload) when readiness must
    /// return to the actor as a later message.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait_started().await
    }

    /// Waits until every named direct child has completed successfully.
    ///
    /// Completion means the current generation exited with
    /// [`ExitStatus::Completed`](crate::observe::ExitStatus::Completed) and no
    /// restart is pending. Removed children drop out of the set. Unknown ids
    /// return [`CompletionError::UnknownChild`], while a terminal scope that
    /// cannot satisfy the condition returns [`CompletionError::ScopeClosed`].
    /// The wait installs its lifecycle stream before reading state, so children
    /// that finish immediately or before the call are handled correctly.
    pub async fn wait_for_children<I, S>(&self, ids: I) -> Result<(), CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_for_children(ids).await
    }

    /// Waits for named direct children that may be inserted into a dynamic scope later.
    ///
    /// Returns [`CompletionError::NotDynamic`] for an ordered scope. Once a
    /// named membership has appeared, its removal drops it out of the set.
    pub async fn wait_for_future_children<I, S>(&self, ids: I) -> Result<(), CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.wait_for_future_children(ids).await
    }

    /// Requests shutdown once every named direct child has completed successfully.
    ///
    /// Ids are validated before this method returns. The returned guard
    /// cancels the operation when dropped; consume it with [`Guard::detach`]
    /// for fire-and-forget behavior. The background task does not keep the
    /// supervision tree alive.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_when_children_complete<I, S>(&self, ids: I) -> Result<Guard, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.shutdown_when_children_complete(ids)
    }

    /// Requests shutdown after future named members of a dynamic scope complete.
    ///
    /// Returns [`CompletionError::NotDynamic`] for an ordered scope. The
    /// returned guard cancels the operation when dropped; consume it with
    /// [`Guard::detach`] for fire-and-forget behavior.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn shutdown_when_future_children_complete<I, S>(
        &self,
        ids: I,
    ) -> Result<Guard, CompletionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.shutdown_when_future_children_complete(ids)
    }

    /// Returns a snapshot and direct-child lifecycle stream with gap-free registration.
    ///
    /// Initialize state from [`LifecycleObservation::snapshot`], then consume
    /// events whose direct-child sequence exceeds the snapshot's
    /// [`SupervisorSnapshot::lifecycle_seq`].
    pub fn observe_lifecycle(&self) -> LifecycleObservation {
        self.supervisor.observe_lifecycle()
    }

    /// Returns the ordered lifecycle stream for this runtime's entire tree.
    ///
    /// Use [`observe_lifecycle`](Self::observe_lifecycle) for a gap-free
    /// direct-child state-plus-stream setup. This lower-level method is useful
    /// when recursive transitions after subscription are needed. Call
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
    /// Unlike [`ActorRef::stats`], each returned [`ScopedActorStats`] pairs
    /// actor-local stats with the current scope path and lineage. Message-size
    /// totals remain `None` unless
    /// observation was enabled with
    /// [`ActorSpec::message_size`](crate::ActorSpec::message_size).
    pub fn actor_stats(&self) -> Vec<ScopedActorStats> {
        let mut runtime_owners = HashMap::from([(Vec::new(), Arc::clone(&self.actors))]);
        let mut stats = Vec::new();

        for attached in __private::attached_children::<RuntimeAttachment>(&self.supervisor) {
            let Some((child, scope_path)) = attached.path().split_last() else {
                continue;
            };
            let Some(owner) = runtime_owners.get(scope_path) else {
                continue;
            };
            let attachment = attached.attachment();
            if !attachment.belongs_to(owner) {
                continue;
            }

            match &attachment.kind {
                RuntimeAttachmentKind::Actor(actor) => {
                    stats.push(ScopedActorStats {
                        scope_path: scope_path.iter().map(scope_path_segment).collect(),
                        lineage: child.lineage,
                        stats: actor.stats(),
                    });
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
) -> Option<ScopeRef> {
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
        Some(ScopeRef::new(
            attached.supervisor()?.clone(),
            Arc::clone(subtree_actors),
        ))
    })
}

impl ScopeRef {
    fn dynamic_supervisor(&self) -> Result<DynamicSupervisorHandle, ControlError> {
        self.supervisor.dynamic().ok_or(ControlError::NotDynamic)
    }

    /// Builds and adds an actor-aware runtime subtree.
    ///
    /// The returned handle can add actors or further subtrees, and recursive
    /// [`ScopeRef::actor_stats`] include the new subtree. Removing the
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
    /// [`ScopeRef::wait_started`] when readiness is needed.
    /// Wrap the supplied tree with
    /// [`SubtreeSpec::restart`](crate::SubtreeSpec::restart) or
    /// [`SubtreeSpec::shutdown`](crate::SubtreeSpec::shutdown) to override the
    /// subtree edge's policies in this dynamic parent.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::NotDynamic`] when this scope has ordered
    /// membership. Both validation failure phases use
    /// [`ControlError::Rejected`]: first the supplied
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
        tree: impl Into<crate::SubtreeSpec>,
    ) -> Result<ScopeRef, ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        let id = id.into();
        let parts = tree.into().into_parts();
        let parts = parts.map_err(ControlError::Rejected)?;
        let mut child = ChildSpec::supervisor(id.clone(), parts.supervisor);
        if let Some(restart) = parts.restart {
            child = child.restart_policy(restart);
        }
        if let Some(shutdown) = parts.shutdown {
            child = child.shutdown(shutdown);
        }
        let lineage = dynamic
            .add_child_spec(child.attachment(RuntimeAttachment::subtree(
                &self.actors,
                Arc::clone(&parts.actors),
            )))
            .await?;
        runtime_subtree_membership(
            __private::dynamic_attached_children::<RuntimeAttachment>(&dynamic),
            &self.actors,
            &id,
            Some(lineage),
        )
        .ok_or(ControlError::Unavailable)
    }

    /// Adds a supervised task child with default configuration to this scope.
    pub async fn add_task<F, Fut>(&self, id: impl Into<String>, task: F) -> Result<(), ControlError>
    where
        F: Fn(crate::TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        self.add_task_spec(TaskSpec::new(id, task)).await
    }

    /// Adds an explicitly configured supervised task child to this scope.
    ///
    /// This is the task-level counterpart to adding an actor. Success means
    /// the membership was inserted and startup was scheduled. Task children do
    /// not appear in [`ScopeRef::actor_stats`], but remain visible through
    /// snapshots and lifecycle watches.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::NotDynamic`] when this scope has ordered
    /// membership. Other failures are reported by the dynamic supervisor.
    pub async fn add_task_spec(&self, task: TaskSpec) -> Result<(), ControlError> {
        self.dynamic_supervisor()?.add_child(task).await.map(|_| ())
    }

    /// Adds an actor with default configuration and returns its stable typed ref.
    pub async fn add_actor<M, F>(
        &self,
        id: impl Into<String>,
        factory: F,
    ) -> Result<ActorRef<M>, ControlError>
    where
        M: Send + 'static,
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        self.add_actor_spec(ActorSpec::new(id, factory)).await
    }

    /// Adds one explicitly configured actor declaration and returns its stable typed ref.
    ///
    /// The actor id is its direct supervisor child id, so it can be removed
    /// later through [`ScopeRef::remove_child`]. See [`crate::ActorFactory`] for
    /// the incarnation lifecycle contract. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`ScopeRef::wait_started`] retains
    /// the stronger readiness contract. A zero
    /// [`ActorSpec::mailbox_capacity`](crate::ActorSpec::mailbox_capacity) is rejected with
    /// [`ControlError::Rejected`].
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::NotDynamic`] when this scope has ordered
    /// membership. Invalid actor configuration and insertion failures are
    /// returned as [`ControlError::Rejected`]; a stopped scope returns
    /// [`ControlError::Unavailable`].
    pub async fn add_actor_spec<M: Send + 'static>(
        &self,
        spec: ActorSpec<M>,
    ) -> Result<ActorRef<M>, ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        let actor_ref = spec.actor_ref();
        spec.actor_options
            .validate()
            .map_err(|error: ActorOptionsValidationError| {
                ControlError::Rejected(BuildError::InvalidConfig(error.message()))
            })?;
        let (default_restart, default_shutdown, default_mailbox_shutdown) =
            self.actors.actor_defaults();
        let dynamic_options = DynamicChildOptions {
            restart: spec.restart.unwrap_or(default_restart),
            shutdown: spec.shutdown.unwrap_or(default_shutdown),
            mailbox_shutdown: spec.mailbox_shutdown.unwrap_or(default_mailbox_shutdown),
            remove_when_done: spec.remove_when_done,
        };
        let actor = self.actors.make_actor(spec);
        self.add_constructed_actor(
            &dynamic,
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
        dynamic: &DynamicSupervisorHandle,
        (actor, actor_ref): (RunnableActor, ActorRef<M>),
        options: DynamicChildOptions,
    ) -> Result<ActorRef<M>, ControlError> {
        let child = actor_child_spec(
            actor.clone(),
            &self.actors,
            ActorChildOptions::new(
                options.restart,
                options.shutdown,
                options.mailbox_shutdown,
                options.remove_when_done,
            ),
        );
        dynamic.add_child_spec(child).await?;

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
    /// [`MailboxShutdown::Drain`](crate::MailboxShutdown::Drain), work accepted
    /// before drain closes intake belongs to the queued prefix handled before
    /// `on_stop`. With
    /// [`MailboxShutdown::Discard`](crate::MailboxShutdown::Discard), accepted
    /// work that remains queued is dropped. Once the actor closes intake,
    /// `try_send` may briefly fail with
    /// [`SendErrorKind::NotRunning`](crate::SendErrorKind::NotRunning), while an
    /// awaited `send` waits and then fails with
    /// [`SendErrorKind::Terminated`](crate::SendErrorKind::Terminated). Either
    /// [`SendError`](crate::SendError) retains the rejected message.
    /// `send_timeout` can instead bound that wait and recover a message that
    /// was not accepted before the bound.
    /// Removal does not return queued messages: end-to-end delivery ownership
    /// belongs in an application acknowledgement and replay protocol.
    ///
    /// Awaiting removal of the current actor from one of its own lifecycle
    /// callbacks can deadlock until the shutdown grace period expires.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::NotDynamic`] when this scope has ordered
    /// membership. A stopped scope returns [`ControlError::Unavailable`], and
    /// operation failures are reported through the remaining variants.
    pub async fn remove_child(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.dynamic_supervisor()?.remove_child(id).await
    }
}

impl std::fmt::Debug for ScopeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeRef").finish_non_exhaustive()
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
    pub(crate) restart: RestartPolicy,
    pub(crate) shutdown: Shutdown,
    pub(crate) mailbox_shutdown: MailboxShutdown,
    pub(crate) remove_when_done: bool,
}

impl ActorChildOptions {
    pub(crate) fn new(
        restart: RestartPolicy,
        shutdown: Shutdown,
        mailbox_shutdown: MailboxShutdown,
        remove_when_done: bool,
    ) -> Self {
        Self {
            restart,
            shutdown,
            mailbox_shutdown,
            remove_when_done,
        }
    }
}

pub(crate) fn actor_child_spec(
    actor: RunnableActor,
    owner: &Arc<ActorRuntimeState>,
    options: ActorChildOptions,
) -> ChildSpec {
    let ActorChildOptions {
        restart,
        shutdown,
        mailbox_shutdown,
        remove_when_done,
    } = options;
    let actor_id = actor.label().to_owned();
    let attachment = RuntimeAttachment::actor(owner, actor.clone());
    let guard = Arc::new(TerminateBindingOnDrop::new(actor));
    let child_guard = Arc::clone(&guard);
    let actor_owner = Arc::clone(owner);
    let child = TaskSpec::new(actor_id, move |ctx| {
        let actor = child_guard.actor.clone();
        let supervisor = ScopeRef::new(ctx.supervisor(), Arc::clone(&actor_owner));
        async move {
            let shutdown_token = ctx.shutdown_token().clone();
            let abort_token = ctx.abort_token().clone();
            let abort_after_grace = !shutdown.is_abort();
            actor
                .run_until_ready(
                    shutdown_token.cancelled(),
                    async move {
                        abort_token.cancelled().await;
                        abort_after_grace
                    },
                    restart,
                    mailbox_shutdown.drains(),
                    supervisor,
                    || ctx.mark_ready(),
                )
                .await
                .map_err(Into::into)
        }
    })
    .into_spec()
    .attachment(attachment)
    .wait_for_ready()
    .restart_policy(restart)
    .shutdown(shutdown);
    if remove_when_done {
        child.remove_when_done()
    } else {
        child
    }
}

fn scope_path_segment(identity: &AttachedChildIdentity) -> ScopePathSegment {
    ScopePathSegment {
        id: identity.id.clone(),
        lineage: identity.lineage,
        generation: identity.generation,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Actor, ActorSpec, BuildError, Context, DynamicTree, ExitResult, RestartMode, RunningTree,
        ScopeRef, Tree,
    };

    #[test]
    fn tree_root_types_preserve_statically_known_membership() {
        let ordered_spawn: fn(Tree) -> Result<RunningTree, BuildError> = Tree::spawn;
        let ordered_scope: fn(&Tree) -> ScopeRef = Tree::scope;
        let dynamic_spawn: fn(DynamicTree) -> Result<RunningTree, BuildError> = DynamicTree::spawn;
        let dynamic_scope: fn(&DynamicTree) -> ScopeRef = DynamicTree::scope;

        let _ = (ordered_spawn, ordered_scope, dynamic_spawn, dynamic_scope);
    }

    struct FailsOnMessage;

    impl Actor for FailsOnMessage {
        type Msg = ();

        async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
            Err(std::io::Error::other("expected test failure").into())
        }
    }

    #[test]
    fn unavailable_runtime_handle_is_cached() {
        let first = super::ScopeRef::unavailable();
        let second = super::ScopeRef::unavailable();

        assert!(std::sync::Arc::ptr_eq(&first.actors, &second.actors));
    }

    #[tokio::test]
    async fn actor_spec_defaults_to_retained_membership_in_static_and_dynamic_scopes() {
        let static_spec = ActorSpec::new("static", || FailsOnMessage).restart(RestartMode::Never);
        let static_ref = static_spec.actor_ref();
        let mut static_tree = Tree::new();
        static_tree.add_actor_spec(static_spec);
        let static_runtime = static_tree.spawn().expect("static runtime builds");
        static_runtime
            .scope()
            .wait_started()
            .await
            .expect("static actor starts");
        let mut static_snapshots = static_runtime.scope().subscribe_snapshots();
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
        let dynamic = dynamic_runtime.scope();
        let dynamic_ref = dynamic
            .add_actor_spec(
                ActorSpec::new("dynamic", || FailsOnMessage).restart(RestartMode::Never),
            )
            .await
            .expect("dynamic actor is inserted");
        dynamic_runtime
            .scope()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut dynamic_snapshots = dynamic_runtime.scope().subscribe_snapshots();
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
        let dynamic = runtime.scope();
        let actor_ref = dynamic
            .add_actor_spec(
                ActorSpec::new("ephemeral", || FailsOnMessage)
                    .restart(RestartMode::Never)
                    .remove_when_done(),
            )
            .await
            .expect("dynamic actor is inserted");
        runtime
            .scope()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut snapshots = runtime.scope().subscribe_snapshots();
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
        let dynamic = root.scope();
        dynamic
            .add_subtree("workers", Tree::new())
            .await
            .expect("first subtree added");
        let first_lineage = root
            .scope()
            .snapshot()
            .child("workers")
            .expect("first membership is visible")
            .lineage;

        dynamic
            .remove_child("workers")
            .await
            .expect("first subtree removed");
        dynamic
            .add_subtree("workers", Tree::new())
            .await
            .expect("replacement subtree added");
        let replacement_lineage = root
            .scope()
            .snapshot()
            .child("workers")
            .expect("replacement membership is visible")
            .lineage;

        assert_ne!(first_lineage, replacement_lineage);
        assert!(
            root.scope()
                .subtree_membership("workers", Some(first_lineage))
                .is_none(),
            "a lookup bound to the completed add must not return a same-id replacement"
        );
        assert!(
            root.scope()
                .subtree_membership("workers", Some(replacement_lineage))
                .is_some()
        );

        root.shutdown_and_wait().await.expect("clean shutdown");
    }
}
