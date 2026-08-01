#[cfg(any(feature = "host", test))]
use std::sync::OnceLock;
use std::{
    collections::HashMap,
    future::Future,
    ops::Deref,
    sync::{Arc, Mutex, PoisonError, Weak},
};

use crate::{
    ActorFactory, ActorRef, ActorSpec, ExitResult,
    actor::{
        ActorNode, ActorOptionsValidationError, RawActor, RunnableActor, RunnableActorBuilder,
        ScopedActorStats,
    },
    supervisor::{
        __private::{self, AttachedChildIdentity, guard_from_tokens},
        BuildError, CancellationToken, ChildEventKind, ChildMembershipView, ChildObservationUpdate,
        ChildObservationWatch, ChildSnapshot, ChildSpec, ChildStateView, CompletionOnDrop,
        ControlError, DynamicSupervisorHandle, ExitStatus, Guard, LifecycleEvent,
        LifecycleEventKind, LifecycleObservation, LifecycleWatch, MailboxShutdown, OneShotTaskSpec,
        RestartPolicy, RunningSupervisor, ScopeKind, ScopePathSegment, Shutdown, Strategy,
        SupervisorError, SupervisorHandle, SupervisorSnapshot, SupervisorSnapshotReceiver,
        SupervisorStateView, TaskSpec,
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
                && !matches!(event.kind, LifecycleEventKind::Lagged { .. })
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

fn spawn_child_observation_to<M, F>(
    mut observation: ChildObservationWatch,
    target: ActorRef<M>,
    mut map: F,
) -> Guard
where
    M: Send + 'static,
    F: FnMut(ChildObservationUpdate) -> M + Send + 'static,
{
    let cancellation = CancellationToken::new();
    let (finished, finished_on_drop) = CompletionOnDrop::armed();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        let _finished_on_drop = finished_on_drop;
        loop {
            let Some(update) = (tokio::select! {
                biased;
                () = task_cancellation.cancelled() => None,
                () = target.wait_terminated() => None,
                update = observation.next() => update,
            }) else {
                return;
            };

            tokio::select! {
                biased;
                () = task_cancellation.cancelled() => return,
                () = target.wait_terminated() => return,
                sent = target.send_to_incarnation(map(update)) => {
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

impl LifecycleWatch {
    /// Forwards child transitions from this stream into `target` using its
    /// ordinary mailbox policy.
    ///
    /// Apply [`LifecycleWatch::direct_children`] before this method when only
    /// the watched scope's own events should be delivered. Lag markers are
    /// forwarded as well; supervisor-level transitions are skipped. The pump
    /// follows the target through ordinary actor restarts, but delivery is
    /// at-most-once: an event accepted by one incarnation is never replayed to
    /// its replacement. The pump stops when the returned guard is dropped or
    /// cancelled, when this stream ends, or when the target permanently
    /// terminates.
    pub fn forward_to<M, F>(self, target: &ActorRef<M>, map: F) -> Guard
    where
        M: Send + 'static,
        F: FnMut(LifecycleEvent) -> M + Send + 'static,
    {
        spawn_lifecycle_watch_to(self, target.clone(), map)
    }
}

impl ChildObservationWatch {
    /// Forwards transitions and recovery resets into `target` using its
    /// ordinary mailbox policy.
    ///
    /// The pump follows the target through ordinary actor restarts, but
    /// delivery is at-most-once: an update accepted by one incarnation is
    /// never replayed to its replacement. The pump stops when the returned
    /// guard is dropped or cancelled, when this stream ends, or when the
    /// target permanently terminates.
    pub fn forward_to<M, F>(self, target: &ActorRef<M>, map: F) -> Guard
    where
        M: Send + 'static,
        F: FnMut(ChildObservationUpdate) -> M + Send + 'static,
    {
        spawn_child_observation_to(self, target.clone(), map)
    }
}

/// Error returned when a [`TaskRef`] can no longer observe its task membership.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TaskError {
    /// The task stopped before reporting explicit startup readiness.
    #[error("task `{task_id}` stopped before reporting readiness")]
    StoppedBeforeReady {
        /// Id of the task that stopped.
        task_id: String,
    },
    /// The task membership ended without an observable exit.
    #[error("task `{task_id}` is no longer available")]
    Unavailable {
        /// Id of the unavailable task.
        task_id: String,
    },
}

#[derive(Clone, Debug, Default)]
struct TaskTrackingState {
    started: bool,
    outcome: Option<Result<ExitStatus, TaskError>>,
}

struct TaskRefInner {
    id: Arc<str>,
    lineage: u64,
    scope: ScopeRef,
    events: Mutex<Option<LifecycleWatch>>,
    state: tokio::sync::watch::Sender<TaskTrackingState>,
}

/// Cloneable, restart-stable handle for one supervised task membership.
///
/// A task ref follows restarts of the membership that created it, but is tied
/// to that membership's lineage. Removing a dynamic task and adding another
/// task with the same id therefore never retargets an existing ref.
#[derive(Clone)]
pub struct TaskRef {
    inner: Arc<TaskRefInner>,
}

impl TaskRef {
    fn new(scope: ScopeRef, id: impl Into<Arc<str>>, lineage: u64, events: LifecycleWatch) -> Self {
        let id = id.into();
        let events = events.direct_child(Arc::clone(&id), lineage);
        let (state, _) = tokio::sync::watch::channel(TaskTrackingState::default());
        Self {
            inner: Arc::new(TaskRefInner {
                id,
                lineage,
                scope,
                events: Mutex::new(Some(events)),
                state,
            }),
        }
    }

    /// Returns the task id within its enclosing scope.
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Returns the latest snapshot for this exact task membership.
    ///
    /// `None` means the membership has been removed or its scope is no longer
    /// available. A later task with the same id is deliberately ignored.
    pub fn snapshot(&self) -> Option<ChildSnapshot> {
        task_snapshot(
            &self.inner.scope.snapshot(),
            &self.inner.id,
            self.inner.lineage,
        )
        .cloned()
    }

    /// Waits until this task reports startup readiness.
    ///
    /// Ordinary tasks are ready as soon as their future is spawned. A task
    /// configured with [`TaskSpec::manual_readiness`] becomes ready when it calls
    /// [`crate::TaskContext::mark_ready`]. If it exits or misses its readiness
    /// deadline first, this returns [`TaskError::StoppedBeforeReady`].
    pub async fn wait_started(&self) -> Result<(), TaskError> {
        self.ensure_tracking();
        let mut state = self.inner.state.subscribe();
        loop {
            let current = state.borrow().clone();
            if current.started {
                return Ok(());
            }
            if let Some(outcome) = current.outcome {
                return match outcome {
                    Ok(_) => Err(TaskError::StoppedBeforeReady {
                        task_id: self.id().to_owned(),
                    }),
                    Err(error) => Err(error),
                };
            }
            state.changed().await.map_err(|_| TaskError::Unavailable {
                task_id: self.id().to_owned(),
            })?;
        }
    }

    /// Waits for this task membership's terminal exit.
    ///
    /// Intermediate exits followed by the task's restart policy are skipped.
    /// The returned [`ExitStatus`] distinguishes clean completion, failure,
    /// panic, and supervisor-driven cancellation or abortion.
    pub async fn wait(&self) -> Result<ExitStatus, TaskError> {
        self.ensure_tracking();
        let mut state = self.inner.state.subscribe();
        loop {
            if let Some(outcome) = state.borrow().outcome.clone() {
                return outcome;
            }
            state.changed().await.map_err(|_| TaskError::Unavailable {
                task_id: self.id().to_owned(),
            })?;
        }
    }

    fn ensure_tracking(&self) {
        let events = self
            .inner
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(events) = events else {
            return;
        };
        let inner = Arc::clone(&self.inner);
        let tracker = tokio::spawn(track_task(inner, events));
        std::mem::drop(tracker);
    }
}

impl std::fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRef")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

async fn track_task(inner: Arc<TaskRefInner>, mut events: LifecycleWatch) {
    let mut last_exit = None;
    if let Some(state) = task_state_from_snapshot(&inner.scope.snapshot(), &inner.id, inner.lineage)
    {
        if let Some(Ok(exit)) = &state.outcome {
            last_exit = Some(exit.clone());
        }
        let finished = state.outcome.is_some();
        inner.state.send_replace(state);
        if finished {
            return;
        }
    }

    while let Some(event) = events.next().await {
        let matches_task = match &event.kind {
            LifecycleEventKind::Child(child) => {
                child.child_id == inner.id.as_ref() && child.lineage == inner.lineage
            }
            _ => false,
        };

        if matches_task {
            match &event.kind {
                LifecycleEventKind::Child(child)
                    if let ChildEventKind::Exited { exit, .. } = &child.kind =>
                {
                    last_exit = Some(exit.clone());
                }
                LifecycleEventKind::Child(child)
                    if matches!(child.kind, ChildEventKind::Removed) =>
                {
                    let outcome = last_exit.clone().map_or_else(
                        || {
                            Err(TaskError::Unavailable {
                                task_id: inner.id.to_string(),
                            })
                        },
                        Ok,
                    );
                    let started = inner.state.borrow().started;
                    inner.state.send_replace(TaskTrackingState {
                        started,
                        outcome: Some(outcome),
                    });
                    return;
                }
                _ => {}
            }
        }

        if matches_task || matches!(event.kind, LifecycleEventKind::Lagged { .. }) {
            match task_state_from_snapshot(&inner.scope.snapshot(), &inner.id, inner.lineage) {
                Some(state) => {
                    let finished = state.outcome.is_some();
                    inner.state.send_replace(state);
                    if finished {
                        return;
                    }
                }
                // This is a task-filtered stream, so the retained suffix still
                // contains this membership's latest transition. Keep draining
                // it when an unusually restart-heavy task overruns the queue.
                None if matches!(event.kind, LifecycleEventKind::Lagged { .. }) => {}
                None => {}
            }
        }
    }

    let outcome = last_exit.map_or_else(
        || {
            Err(TaskError::Unavailable {
                task_id: inner.id.to_string(),
            })
        },
        Ok,
    );
    let started = inner.state.borrow().started;
    inner.state.send_replace(TaskTrackingState {
        started,
        outcome: Some(outcome),
    });
}

fn task_snapshot<'a>(
    snapshot: &'a SupervisorSnapshot,
    id: &str,
    lineage: u64,
) -> Option<&'a ChildSnapshot> {
    snapshot
        .children
        .iter()
        .find(|child| child.id == id && child.lineage == lineage)
}

fn task_state_from_snapshot(
    snapshot: &SupervisorSnapshot,
    id: &str,
    lineage: u64,
) -> Option<TaskTrackingState> {
    let child = task_snapshot(snapshot, id, lineage)?;
    let started = match child.state {
        ChildStateView::Running { .. } => true,
        ChildStateView::Stopping { started, .. } | ChildStateView::Stopped { started, .. } => {
            started
        }
        ChildStateView::Starting { .. } | ChildStateView::StartupAborted { .. } => false,
    };
    let exit = child.state.last_exit().cloned();
    let outcome = exit.and_then(|exit| {
        let can_restart = snapshot.state == SupervisorStateView::Running
            && child.membership == ChildMembershipView::Active
            && (child.restart_policy.should_restart(exit.is_failure())
                || task_is_revivable_by_group(snapshot, child));
        (child.state.is_terminal() && !can_restart && child.next_restart_in.is_none())
            .then_some(Ok(exit))
    });
    Some(TaskTrackingState { started, outcome })
}

fn task_is_revivable_by_group(snapshot: &SupervisorSnapshot, child: &ChildSnapshot) -> bool {
    if child.restart_policy.is_never() {
        return false;
    }

    match snapshot.strategy {
        Strategy::OneForOne => false,
        Strategy::OneForAll => true,
        Strategy::RestForOne => snapshot
            .children
            .first()
            .is_some_and(|first| first.lineage != child.lineage),
    }
}

/// Owns a spawned supervision tree.
///
/// Use [`scope`](Self::scope) for observation and control through a cheaply
/// cloneable, non-owning scope reference. Ordered trees use the default
/// [`ScopeRef`] parameter, while dynamic trees use [`DynamicScopeRef`].
/// Dropping a scope reference is inert and does not keep this owner alive;
/// dropping this owner requests graceful shutdown.
#[must_use = "dropping the running tree requests graceful shutdown"]
pub struct RunningTree<S = ScopeRef> {
    supervisor: RunningSupervisor,
    scope: S,
}

mod sealed {
    use super::{ActorRef, ControlError, DynamicScopeRef, ScopeRef, TaskRef};

    pub trait RunningScope: Clone {
        fn from_scope(scope: ScopeRef) -> Self;
    }

    impl RunningScope for ScopeRef {
        fn from_scope(scope: ScopeRef) -> Self {
            scope
        }
    }

    impl RunningScope for DynamicScopeRef {
        fn from_scope(scope: ScopeRef) -> Self {
            DynamicScopeRef::new(scope)
        }
    }

    pub trait ChildHandle {
        fn resolve_membership(
            &self,
            scope: &DynamicScopeRef,
        ) -> Result<(String, u64), ControlError>;
    }

    impl<M> ChildHandle for ActorRef<M> {
        fn resolve_membership(
            &self,
            scope: &DynamicScopeRef,
        ) -> Result<(String, u64), ControlError> {
            scope.resolve_actor_membership(self)
        }
    }

    impl ChildHandle for TaskRef {
        fn resolve_membership(
            &self,
            scope: &DynamicScopeRef,
        ) -> Result<(String, u64), ControlError> {
            if !scope.supervisor.same_identity(&self.inner.scope.supervisor) {
                return Err(ControlError::UnknownChildId(self.id().to_owned()));
            }
            Ok((self.id().to_owned(), self.inner.lineage))
        }
    }

    impl ChildHandle for ScopeRef {
        fn resolve_membership(
            &self,
            scope: &DynamicScopeRef,
        ) -> Result<(String, u64), ControlError> {
            if let Some(membership) = self.parent_membership.as_ref() {
                if !scope.supervisor.same_identity(&membership.parent) {
                    return Err(ControlError::UnknownChildId(membership.id.to_string()));
                }
                return Ok((membership.id.to_string(), membership.lineage));
            }

            scope.resolve_subtree_membership(self)
        }
    }

    impl ChildHandle for DynamicScopeRef {
        fn resolve_membership(
            &self,
            scope: &DynamicScopeRef,
        ) -> Result<(String, u64), ControlError> {
            self.scope.resolve_membership(scope)
        }
    }
}

impl<S> RunningTree<S> {
    pub(crate) fn new(supervisor: RunningSupervisor, actors: Arc<ActorRuntimeState>) -> Self
    where
        S: sealed::RunningScope,
    {
        let scope = ScopeRef::new(supervisor.handle(), actors);
        let scope = S::from_scope(scope);
        Self { supervisor, scope }
    }

    /// Requests graceful shutdown and waits for completion, consuming the owner.
    pub async fn shutdown(self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Waits for the running tree to stop, consuming the owner.
    pub async fn wait(self) -> Result<(), SupervisorError> {
        self.supervisor.wait().await
    }
}

impl<S: Clone> RunningTree<S> {
    /// Returns the running tree's non-owning root scope reference.
    pub fn scope(&self) -> S {
        self.scope.clone()
    }
}

impl<S> std::fmt::Debug for RunningTree<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningTree").finish_non_exhaustive()
    }
}

/// A cheaply cloneable, non-owning reference and control capability for a
/// supervision scope.
///
/// As an [`ActorRef`](crate::ActorRef) addresses an actor without owning its
/// runtime, a `ScopeRef` addresses one supervision scope for control,
/// and observation. Dropping any root
/// or nested reference leaves the runtime running. A spawned root remains alive
/// until its owning [`RunningTree`] is shut down or dropped.
#[derive(Clone)]
pub struct ScopeRef {
    supervisor: SupervisorHandle,
    actors: Arc<ActorRuntimeState>,
    parent_membership: Option<ParentScopeMembership>,
}

#[derive(Clone)]
struct ParentScopeMembership {
    parent: SupervisorHandle,
    id: Arc<str>,
    lineage: u64,
}

/// A [`ScopeRef`] that can add and remove runtime children.
///
/// Observation, waiting, and shutdown methods are inherited from `ScopeRef`;
/// mutation methods live only on this capability, so ordered scopes cannot be
/// mutated accidentally.
#[derive(Clone)]
pub struct DynamicScopeRef {
    scope: ScopeRef,
}

impl ScopeRef {
    pub(crate) fn new(supervisor: SupervisorHandle, actors: Arc<ActorRuntimeState>) -> Self {
        Self {
            supervisor,
            actors,
            parent_membership: None,
        }
    }

    fn with_parent_membership(
        supervisor: SupervisorHandle,
        actors: Arc<ActorRuntimeState>,
        parent: SupervisorHandle,
        id: impl Into<Arc<str>>,
        lineage: u64,
    ) -> Self {
        Self {
            supervisor,
            actors,
            parent_membership: Some(ParentScopeMembership {
                parent,
                id: id.into(),
                lineage,
            }),
        }
    }

    pub(crate) fn task_ref(&self, id: impl Into<Arc<str>>, lineage: u64) -> TaskRef {
        TaskRef::new(self.clone(), id, lineage, self.supervisor.watch_lifecycle())
    }

    #[cfg(any(feature = "host", test))]
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

    /// Requests a graceful shutdown of the supervisor without waiting for it.
    pub fn request_shutdown(&self) {
        self.supervisor.shutdown();
    }

    /// Requests a graceful shutdown and waits for the supervisor to stop.
    ///
    /// Awaiting this from an actor callback in the same scope can block on that
    /// callback returning. The cycle ends only if the actor's shutdown grace
    /// expires and aborts it. An actor in the scope cannot receive this result:
    /// its own exit is part of the shutdown condition. Call
    /// [`request_shutdown`](Self::request_shutdown) from that actor and observe
    /// completion from outside the scope. A bounded
    /// [`Context::offload`](crate::Context::offload) is appropriate only when
    /// shutting down a different scope that can stop while the actor remains live.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown_and_wait().await
    }

    /// Returns whether this scope has ordered or dynamic membership.
    pub fn kind(&self) -> ScopeKind {
        self.supervisor.kind()
    }

    /// Projects this scope to its dynamic-membership capability.
    ///
    /// Prefer handles returned directly by [`DynamicTree::scope`](crate::DynamicTree::scope)
    /// or [`RunningTree::scope`]. This projection is the escape hatch for
    /// dynamic scopes discovered through untyped tree traversal.
    pub fn dynamic(&self) -> Option<DynamicScopeRef> {
        (self.kind() == ScopeKind::Dynamic).then(|| DynamicScopeRef::new(self.clone()))
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
            &self.supervisor,
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

    /// Returns a snapshot and self-recovering direct-child update stream.
    ///
    /// Initialize state from [`LifecycleObservation::snapshot`], then apply
    /// every transition or complete reset returned by its event stream.
    pub fn observe_children(&self) -> LifecycleObservation {
        self.supervisor.observe_lifecycle()
    }

    /// Returns the ordered lifecycle stream for this runtime's entire tree.
    ///
    /// Use [`observe_children`](Self::observe_children) for a gap-free
    /// direct-child state-plus-stream setup. This lower-level method is useful
    /// when recursive transitions after subscription are needed. Call
    /// [`LifecycleWatch::direct_children`] for only this scope.
    pub fn lifecycle_events(&self) -> LifecycleWatch {
        self.supervisor.watch_lifecycle()
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
    pub fn snapshots(&self) -> SupervisorSnapshotReceiver {
        self.supervisor.subscribe_snapshots()
    }
}

impl DynamicScopeRef {
    pub(crate) fn new(scope: ScopeRef) -> Self {
        debug_assert_eq!(scope.kind(), ScopeKind::Dynamic);
        Self { scope }
    }
}

impl From<DynamicScopeRef> for ScopeRef {
    fn from(scope: DynamicScopeRef) -> Self {
        scope.scope
    }
}

/// A stable handle that identifies one exact dynamic child membership.
///
/// This sealed trait is implemented by [`ActorRef`], [`TaskRef`], [`ScopeRef`],
/// and [`DynamicScopeRef`]. It cannot be implemented outside Kokage.
pub trait ChildHandle: sealed::ChildHandle {}

impl<T: sealed::ChildHandle + ?Sized> ChildHandle for T {}

impl Deref for DynamicScopeRef {
    type Target = ScopeRef;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

fn runtime_subtree_membership(
    attached_children: Vec<__private::AttachedChild<RuntimeAttachment>>,
    actors: &Arc<ActorRuntimeState>,
    parent: &SupervisorHandle,
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
        Some(ScopeRef::with_parent_membership(
            attached.supervisor()?.clone(),
            Arc::clone(subtree_actors),
            parent.clone(),
            identity.id.clone(),
            identity.lineage,
        ))
    })
}

impl DynamicScopeRef {
    fn dynamic_supervisor(&self) -> Result<DynamicSupervisorHandle, ControlError> {
        let dynamic = self.supervisor.dynamic().ok_or(ControlError::Unavailable)?;
        dynamic.ensure_available()?;
        Ok(dynamic)
    }

    /// Builds and adds an actor-aware runtime subtree.
    ///
    /// The returned handle observes and controls the new subtree, and recursive
    /// [`ScopeRef::actor_stats`] include it. When the supplied tree is dynamic,
    /// obtain its [`DynamicScopeRef`] before moving it into this method if its
    /// mutation capability should be retained directly.
    /// Removing the child detaches its actor metadata with the supervisor
    /// membership; retained subtree handles then report
    /// [`ControlError::Unavailable`] from dynamic control operations.
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
    /// Both validation failure phases use [`ControlError::Rejected`]: first the supplied
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
            &self.supervisor,
            &id,
            Some(lineage),
        )
        .ok_or(ControlError::Unavailable)
    }

    /// Adds a supervised task child with default configuration to this scope.
    pub async fn add_task<F, Fut>(
        &self,
        id: impl Into<String>,
        task: F,
    ) -> Result<TaskRef, ControlError>
    where
        F: Fn(crate::TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        self.add_task_spec(TaskSpec::new(id, task)).await
    }

    /// Spawns finite one-shot work and removes its membership after completion.
    ///
    /// This is the concise one-shot counterpart to [`add_task`](Self::add_task).
    /// The factory is `FnOnce`, so the task can consume owned inputs. The task
    /// never restarts; use [`spawn_once_spec`](Self::spawn_once_spec) to
    /// configure its shutdown, readiness, or terminal membership retention.
    pub async fn spawn_once<F, Fut>(
        &self,
        id: impl Into<String>,
        task: F,
    ) -> Result<TaskRef, ControlError>
    where
        F: FnOnce(crate::TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        self.spawn_once_spec(OneShotTaskSpec::new(id, task)).await
    }

    /// Spawns explicitly configured one-shot work.
    ///
    /// This preserves the consuming `FnOnce` factory while exposing only the
    /// task settings that remain valid without restarts. Success means the
    /// membership was inserted and startup was scheduled.
    pub async fn spawn_once_spec(&self, task: OneShotTaskSpec) -> Result<TaskRef, ControlError> {
        self.add_task_child_spec(task.into_spec()).await
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
    /// Failures are reported by the dynamic supervisor.
    pub async fn add_task_spec(&self, task: TaskSpec) -> Result<TaskRef, ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        let id: Arc<str> = Arc::from(task.id());
        let events = self
            .supervisor
            .watch_lifecycle()
            .pending_direct_child(Arc::clone(&id));
        let lineage = dynamic.add_child(task).await?;
        Ok(TaskRef::new(self.scope.clone(), id, lineage, events))
    }

    async fn add_task_child_spec(&self, task: ChildSpec) -> Result<TaskRef, ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        let id: Arc<str> = Arc::from(task.id());
        let events = self
            .supervisor
            .watch_lifecycle()
            .pending_direct_child(Arc::clone(&id));
        let lineage = dynamic.add_child_spec(task).await?;
        Ok(TaskRef::new(self.scope.clone(), id, lineage, events))
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
    /// The returned ref identifies the exact membership and can be passed to
    /// [`DynamicScopeRef::remove`]. See [`crate::ActorFactory`] for the
    /// incarnation lifecycle contract. Success means membership was
    /// inserted and immediate startup was scheduled. The returned stable ref
    /// can be used immediately, while [`ScopeRef::wait_started`] retains
    /// the stronger readiness contract. A zero-capacity
    /// [`Mailbox::queue`](crate::Mailbox::queue) is rejected with
    /// [`ControlError::Rejected`].
    ///
    /// # Errors
    ///
    /// Invalid actor configuration and insertion failures are
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

    fn resolve_actor_membership<M>(
        &self,
        actor: &ActorRef<M>,
    ) -> Result<(String, u64), ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        __private::dynamic_attached_children::<RuntimeAttachment>(&dynamic)
            .into_iter()
            .find_map(|attached| {
                let [identity] = attached.path() else {
                    return None;
                };
                let attachment = attached.attachment();
                if !attachment.belongs_to(&self.actors) {
                    return None;
                }
                let RuntimeAttachmentKind::Actor(member) = &attachment.kind else {
                    return None;
                };
                Arc::ptr_eq(member.identity(), actor.identity())
                    .then(|| (identity.id.clone(), identity.lineage))
            })
            .ok_or_else(|| ControlError::UnknownChildId(actor.id().to_owned()))
    }

    fn resolve_subtree_membership(
        &self,
        subtree: &ScopeRef,
    ) -> Result<(String, u64), ControlError> {
        let dynamic = self.dynamic_supervisor()?;
        __private::dynamic_attached_children::<RuntimeAttachment>(&dynamic)
            .into_iter()
            .find_map(|attached| {
                let [identity] = attached.path() else {
                    return None;
                };
                let attachment = attached.attachment();
                if !attachment.belongs_to(&self.actors)
                    || !matches!(attachment.kind, RuntimeAttachmentKind::Subtree(_))
                    || !attached
                        .supervisor()
                        .is_some_and(|member| member.same_identity(&subtree.supervisor))
                {
                    return None;
                }
                Some((identity.id.clone(), identity.lineage))
            })
            .ok_or_else(|| ControlError::UnknownChildId("<root>".to_owned()))
    }

    /// Removes the exact child membership identified by `child`.
    ///
    /// A stale handle never removes a same-id replacement. Actor handles are
    /// resolved against the scope's current runtime attachments, and task
    /// handles carry their membership identity directly. Subtree handles
    /// returned by insertion carry that identity; a scope retained before its
    /// tree was moved into [`add_subtree`](Self::add_subtree) is resolved by its
    /// stable supervisor identity. Use [`remove_named`](Self::remove_named)
    /// when only the id is available and targeting whichever membership
    /// currently owns it is intentional.
    ///
    /// Removal marks the membership as removing and starts its configured
    /// shutdown. For an actor whose cooperative shutdown completes within its
    /// grace period, the [`Actor`](crate::Actor) stops its normal receive loop, closes external
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
    /// A stopped scope returns [`ControlError::Unavailable`]. A stale or foreign
    /// handle returns [`ControlError::UnknownChildId`]; other operation failures
    /// are reported through the remaining variants.
    pub async fn remove(&self, child: &impl ChildHandle) -> Result<(), ControlError> {
        let membership = sealed::ChildHandle::resolve_membership(child, self)?;
        let dynamic = self.dynamic_supervisor()?;
        dynamic
            .remove_child_membership(membership.0, membership.1)
            .await
    }

    /// Removes whichever current child membership owns `id`.
    ///
    /// Prefer [`remove`](Self::remove) when the insertion handle is available.
    /// This name-based operation is the escape hatch for external
    /// registries and operator input.
    pub async fn remove_named(&self, id: impl Into<String>) -> Result<(), ControlError> {
        self.dynamic_supervisor()?.remove_child(id).await
    }
}

impl std::fmt::Debug for ScopeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeRef").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for DynamicScopeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicScopeRef").finish_non_exhaustive()
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
    .automatic_readiness()
    .restart(restart)
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
        Actor, ActorSpec, BuildError, Context, ControlError, DynamicScopeRef, DynamicTree,
        ExitResult, RestartPolicy, RunningTree, ScopeRef, Tree,
    };

    #[test]
    fn tree_root_types_preserve_statically_known_membership() {
        let ordered_spawn: fn(Tree) -> Result<RunningTree, BuildError> = Tree::spawn;
        let ordered_scope: fn(&Tree) -> ScopeRef = Tree::scope;
        let dynamic_spawn: fn(DynamicTree) -> Result<RunningTree<DynamicScopeRef>, BuildError> =
            DynamicTree::spawn;
        let dynamic_scope: fn(&DynamicTree) -> DynamicScopeRef = DynamicTree::scope;

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
        let static_spec =
            ActorSpec::new("static", || FailsOnMessage).restart(RestartPolicy::never());
        let static_ref = static_spec.actor_ref();
        let mut static_tree = Tree::new();
        static_tree.add_actor_spec(static_spec);
        let static_runtime = static_tree.spawn().expect("static runtime builds");
        static_runtime
            .scope()
            .wait_started()
            .await
            .expect("static actor starts");
        let mut static_snapshots = static_runtime.scope().snapshots();
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
            .shutdown()
            .await
            .expect("static runtime shuts down");

        let dynamic_runtime = DynamicTree::new().spawn().expect("dynamic runtime builds");
        let dynamic = dynamic_runtime.scope();
        let dynamic_ref = dynamic
            .add_actor_spec(
                ActorSpec::new("dynamic", || FailsOnMessage).restart(RestartPolicy::never()),
            )
            .await
            .expect("dynamic actor is inserted");
        dynamic_runtime
            .scope()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut dynamic_snapshots = dynamic_runtime.scope().snapshots();
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
            .shutdown()
            .await
            .expect("dynamic runtime shuts down");
    }

    #[tokio::test]
    async fn dynamic_membership_removal_is_explicit() {
        let running_tree = DynamicTree::new().spawn().expect("dynamic runtime builds");
        let dynamic = running_tree.scope();
        let actor_ref = dynamic
            .add_actor_spec(
                ActorSpec::new("ephemeral", || FailsOnMessage)
                    .restart(RestartPolicy::never())
                    .remove_when_done(),
            )
            .await
            .expect("dynamic actor is inserted");
        running_tree
            .scope()
            .wait_started()
            .await
            .expect("dynamic actor starts");
        let mut snapshots = running_tree.scope().snapshots();
        assert!(snapshots.latest().child("ephemeral").is_some());
        actor_ref.send(()).await.expect("dynamic message accepted");
        snapshots
            .wait_for(|snapshot| snapshot.child("ephemeral").is_none())
            .await
            .expect("explicitly ephemeral membership is removed");
        running_tree
            .shutdown()
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
            .remove_named("workers")
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
        assert!(matches!(
            dynamic
                .dynamic_supervisor()
                .expect("dynamic supervisor")
                .remove_child_membership("workers", first_lineage)
                .await,
            Err(ControlError::UnknownChildId(id)) if id == "workers"
        ));
        assert!(
            root.scope().snapshot().child("workers").is_some(),
            "a conditional remove cannot detach the replacement"
        );

        root.shutdown().await.expect("clean shutdown");
    }
}
