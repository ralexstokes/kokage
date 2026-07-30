use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
};

use crate::{
    ActorRef, ActorSpec,
    actor::{
        ActorNode, ActorOptionsValidationError, ActorStats, RunnableActor, RunnableActorBuilder,
    },
    supervisor::{
        __private::{self, AttachedChildIdentity, guard_from_tokens},
        BuildError, CancellationToken, ChildSpec, CompletionOnDrop, CompletionWatch, ControlError,
        DynamicSupervisorHandle, Guard, LifecycleEvent, LifecycleWatch, Restart, RunningSupervisor,
        ScopeKind, ScopePathSegment, Shutdown, SupervisorError, SupervisorHandle,
        SupervisorSnapshot, SupervisorSnapshotReceiver, TaskSpec,
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
/// `RunningTree` exposes only owner-lifecycle operations. Use
/// [`scope`](Self::scope) to obtain the cheaply cloneable, non-owning
/// [`ScopeRef`] used for root or nested control and observation. Dropping a
/// `ScopeRef` is inert and it does not keep this owner alive; dropping this
/// owner requests graceful shutdown. Bind `let root = running.scope();` when
/// performing repeated root operations.
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
    pub async fn wait(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }

    /// Waits until all current actor children have completed `on_start`.
    pub async fn wait_started(&self) -> Result<(), SupervisorError> {
        self.supervisor.wait_started().await
    }

    /// Creates a completion watch for direct children of this scope.
    ///
    /// The watch validates ids when [`CompletionWatch::wait`] begins. Call
    /// [`CompletionWatch::allow_future_members`] when ids may be inserted into
    /// a dynamic scope later, or [`CompletionWatch::then_shutdown`] to arm
    /// scope shutdown at the completion boundary.
    pub fn completions<I, S>(&self, ids: I) -> CompletionWatch
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.supervisor.completions(ids)
    }

    pub(crate) fn restricted_completions<I, S>(&self, ids: I) -> CompletionWatch<false>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CompletionWatch::new(self.supervisor.clone(), self.kind(), ids)
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
    /// [`ActorStats::scope_path`] and [`ActorStats::lineage`] from the
    /// current runtime membership. Message-size totals remain `None` unless
    /// observation was enabled with
    /// [`ActorSpec::message_size`](crate::ActorSpec::message_size).
    pub fn actor_stats(&self) -> Vec<ActorStats> {
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
                    let mut actor_stats = actor.stats();
                    actor_stats.scope_path =
                        Some(scope_path.iter().map(scope_path_segment).collect());
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
    /// Wrap the supplied tree with [`TreeNode::restart`](crate::TreeNode::restart)
    /// or [`TreeNode::shutdown`](crate::TreeNode::shutdown) to override the
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
        tree: impl Into<crate::TreeNode>,
    ) -> Result<ScopeRef, ControlError> {
        let dynamic = self.dynamic_supervisor()?;
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

    /// Adds an arbitrary supervised task child to this scope.
    ///
    /// This is the task-level counterpart to adding an actor. Success means
    /// the membership was inserted and startup was scheduled, and returns the
    /// lineage assigned to that membership. Task children do not appear in
    /// [`ScopeRef::actor_stats`], but remain visible through snapshots and
    /// lifecycle watches.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::NotDynamic`] when this scope has ordered
    /// membership. Other failures are reported by the dynamic supervisor.
    pub async fn add_task(&self, task: TaskSpec) -> Result<u64, ControlError> {
        self.dynamic_supervisor()?.add_child(task).await
    }

    /// Adds one actor declaration and returns its stable typed ref.
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
    pub async fn add_actor<M: Send + 'static>(
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
        let (default_restart, default_shutdown) = self.actors.actor_defaults();
        let dynamic_options = DynamicChildOptions {
            restart: spec.restart.unwrap_or(default_restart),
            shutdown: spec.shutdown.unwrap_or(default_shutdown),
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
            ActorChildOptions::new(options.restart, options.shutdown),
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
    /// [`Shutdown::drain_for`](crate::Shutdown::drain_for), work accepted before
    /// drain closes intake belongs to the queued prefix handled before
    /// `on_stop`. With
    /// [`Shutdown::discard_after_current`](crate::Shutdown::discard_after_current),
    /// accepted work that remains queued is dropped. Once the actor closes intake,
    /// `try_send` may briefly return
    /// [`TrySendError::NotRunning`](crate::TrySendError::NotRunning), while an awaited
    /// `send` waits and then returns [`SendError`](crate::SendError), retaining
    /// the rejected message. `send_timeout` can instead bound that wait and
    /// recover a message that was not accepted before the bound.
    /// Removal does not return queued messages: end-to-end delivery ownership
    /// belongs in an application acknowledgement and replay protocol.
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
    TaskSpec::new(actor_id, move |ctx| {
        let actor = child_guard.actor.clone();
        let supervisor = ScopeRef::new(ctx.supervisor(), Arc::clone(&actor_owner));
        async move {
            actor
                .run_until_ready(
                    ctx.shutdown_token().cancelled(),
                    ctx.abort_token().cancelled(),
                    restart,
                    matches!(shutdown, Shutdown::Drain { .. }),
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
    .restart(restart)
    .shutdown(shutdown)
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
        Actor, ActorSpec, BuildError, Context, DynamicTree, ExitResult, OrderedTree, Restart,
        RunningTree, ScopeRef,
    };

    #[test]
    fn tree_root_types_preserve_statically_known_membership() {
        let ordered_spawn: fn(OrderedTree) -> Result<RunningTree, BuildError> = OrderedTree::spawn;
        let ordered_scope: fn(&OrderedTree) -> ScopeRef = OrderedTree::scope;
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
        let static_spec = ActorSpec::new("static", || FailsOnMessage).restart(Restart::never());
        let static_ref = static_spec.actor_ref();
        let static_runtime = OrderedTree::new()
            .actor(static_spec)
            .spawn()
            .expect("static runtime builds");
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
            .add_actor(ActorSpec::new("dynamic", || FailsOnMessage).restart(Restart::never()))
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
            .add_actor(
                ActorSpec::new("ephemeral", || FailsOnMessage)
                    .restart(Restart::never().remove_when_done()),
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
            .add_subtree("workers", OrderedTree::new())
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
            .add_subtree("workers", OrderedTree::new())
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
