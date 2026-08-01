use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::{
    actor::ExitResult,
    supervisor::{
        context::TaskContext, owner::Supervisor, restart::RestartPolicy, shutdown::Shutdown,
    },
};

/// A type-erased, thread-safe error type used as the `Err` half of
/// [`ExitResult`](crate::ExitResult).
///
/// This is re-exported as `kokage::BoxError`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub(crate) type ChildFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
pub(crate) type OpaqueAttachment = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ChildDefinition {
    pub(crate) id: String,
    pub(crate) restart: RestartPolicy,
    restart_is_default: bool,
    pub(crate) shutdown_policy: Shutdown,
    shutdown_is_default: bool,
    pub(crate) remove_when_done: bool,
    pub(crate) readiness: ChildReadiness,
    pub(crate) attachment: Option<OpaqueAttachment>,
    pub(crate) kind: ChildKind,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ChildReadiness {
    #[default]
    Immediate,
    Explicit,
}

pub(crate) enum ChildKind {
    Task(Arc<dyn ChildFactory>),
    Supervisor(Supervisor),
}

/// Specification for a supervised task.
///
/// Construct one with [`new`](Self::new), then apply restart, shutdown, and
/// membership-retention policies. Nested scopes are built through Kokage's
/// tree APIs. For finite dynamic work backed by a consuming factory, use
/// [`OneShotTaskSpec`] instead.
pub struct TaskSpec {
    pub(crate) spec: ChildSpec,
}

/// Specification for finite task work whose factory is consumed exactly once.
///
/// Construct one with [`new`](Self::new), optionally configure shutdown,
/// readiness, or terminal membership retention, then pass it to
/// [`DynamicScopeRef::spawn_once_spec`](crate::DynamicScopeRef::spawn_once_spec).
/// Restart configuration is intentionally absent because a consuming factory
/// cannot produce another incarnation.
pub struct OneShotTaskSpec {
    pub(crate) spec: ChildSpec,
}

/// Internal carrier for any supervised child: task, actor host, or nested
/// supervisor. `TaskSpec` is the public task-only constructor and unwraps to
/// this carrier; actor hosts likewise begin with `TaskSpec::new(..)` before
/// lowering through `into_spec()`, while nested supervisors are constructed
/// directly as `ChildSpec` values.
pub(crate) struct ChildSpec {
    pub(crate) inner: Arc<ChildDefinition>,
}

pub(crate) trait ChildFactory: Send + Sync + 'static {
    fn make(&self, ctx: TaskContext) -> ChildFuture;
}

struct ClosureFactory<F> {
    f: F,
}

struct OneShotFactory<F> {
    f: Mutex<Option<F>>,
}

impl<F, Fut> ChildFactory for ClosureFactory<F>
where
    F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ExitResult> + Send + 'static,
{
    fn make(&self, ctx: TaskContext) -> ChildFuture {
        Box::pin((self.f)(ctx))
    }
}

impl<F, Fut> ChildFactory for OneShotFactory<F>
where
    F: FnOnce(TaskContext) -> Fut + Send + 'static,
    Fut: Future<Output = ExitResult> + Send + 'static,
{
    fn make(&self, ctx: TaskContext) -> ChildFuture {
        let factory = self
            .f
            .lock()
            .expect("one-shot task factory lock poisoned")
            .take();
        match factory {
            Some(factory) => Box::pin(factory(ctx)),
            None => Box::pin(async {
                Err(std::io::Error::other("one-shot task factory invoked more than once").into())
            }),
        }
    }
}

fn make_child_factory<F, Fut>(f: F) -> Arc<dyn ChildFactory>
where
    F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ExitResult> + Send + 'static,
{
    Arc::new(ClosureFactory { f })
}

impl TaskSpec {
    /// Creates a supervised task specification.
    ///
    /// `id` must be unique among siblings within the same scope.
    ///
    /// `f` is an async factory that is invoked each time the task is
    /// (re)started. It receives a [`TaskContext`] and should return
    /// [`ExitResult`]: `Ok(())` for a clean exit or an error for a failure.
    pub fn new<F, Fut>(id: impl Into<String>, f: F) -> Self
    where
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        Self {
            spec: ChildSpec {
                inner: Arc::new(ChildDefinition {
                    id: id.into(),
                    restart: RestartPolicy::default(),
                    restart_is_default: true,
                    shutdown_policy: Shutdown::default(),
                    shutdown_is_default: true,
                    remove_when_done: false,
                    readiness: ChildReadiness::Immediate,
                    attachment: None,
                    kind: ChildKind::Task(make_child_factory(f)),
                }),
            },
        }
    }

    /// Overrides the enclosing scope's complete restart policy.
    #[must_use]
    pub fn restart(self, policy: RestartPolicy) -> Self {
        Self {
            spec: self.spec.restart(policy),
        }
    }

    /// Sets the shutdown policy for this task. See [`Shutdown`] for
    /// options.
    #[must_use]
    pub fn shutdown(self, policy: Shutdown) -> Self {
        Self {
            spec: self.spec.shutdown(policy),
        }
    }

    /// Removes this membership after an exit its restart policy does not restart.
    ///
    /// By default a terminal child remains visible as an inactive membership.
    /// This setting is independent of the selected [`RestartPolicy`].
    #[must_use]
    pub fn remove_when_done(self) -> Self {
        Self {
            spec: self.spec.remove_when_done(),
        }
    }

    /// Requires the task to call [`TaskContext::mark_ready`](crate::supervisor::TaskContext::mark_ready)
    /// before it is considered started.
    ///
    /// An ordered supervisor waits for this signal before starting its next
    /// declared task.
    /// If the task exits before reporting readiness, its ordinary restart
    /// policy applies. The sequence waits through a scheduled restart; if the
    /// exit is terminal, the task is marked startup-aborted and the sequence
    /// skips it. There is no built-in readiness timeout; use a timeout inside
    /// the task when initialization must be bounded. Shutdown and control
    /// commands remain responsive while a supervisor waits for readiness, so a
    /// task may await a control operation before calling `mark_ready`.
    #[must_use]
    pub fn wait_for_ready(self) -> Self {
        Self {
            spec: self.spec.wait_for_ready(),
        }
    }

    /// Returns the task's unique identifier.
    pub fn id(&self) -> &str {
        self.spec.id()
    }

    pub(crate) fn into_spec(self) -> ChildSpec {
        self.spec
    }

    pub(crate) fn resolved_policies(
        &self,
        default_restart: RestartPolicy,
        default_shutdown: Shutdown,
    ) -> (RestartPolicy, Shutdown) {
        self.spec
            .resolved_policies(default_restart, default_shutdown)
    }

    pub(crate) fn removes_when_done(&self) -> bool {
        self.spec.inner.remove_when_done
    }
}

impl OneShotTaskSpec {
    /// Creates finite work whose consuming factory runs at most once.
    ///
    /// The task never restarts and its membership is removed after completion
    /// unless [`retain_when_done`](Self::retain_when_done) is selected.
    pub fn new<F, Fut>(id: impl Into<String>, f: F) -> Self
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        Self {
            spec: ChildSpec {
                inner: Arc::new(ChildDefinition {
                    id: id.into(),
                    restart: RestartPolicy::never(),
                    restart_is_default: false,
                    shutdown_policy: Shutdown::default(),
                    shutdown_is_default: true,
                    remove_when_done: true,
                    readiness: ChildReadiness::Immediate,
                    attachment: None,
                    kind: ChildKind::Task(Arc::new(OneShotFactory {
                        f: Mutex::new(Some(f)),
                    })),
                }),
            },
        }
    }

    /// Sets the shutdown policy for this task. See [`Shutdown`] for
    /// options.
    #[must_use]
    pub fn shutdown(self, policy: Shutdown) -> Self {
        Self {
            spec: self.spec.shutdown(policy),
        }
    }

    /// Keeps the terminal membership visible after the one-shot task exits.
    ///
    /// This is useful when scope-level snapshot observers need to discover the
    /// terminal state without already holding its [`TaskRef`](crate::TaskRef).
    /// The retained membership continues to occupy its child id until it is
    /// passed to [`DynamicScopeRef::remove`](crate::DynamicScopeRef::remove)
    /// or the scope shuts down. The default removes the membership after
    /// completion; the returned `TaskRef` retains the terminal outcome either
    /// way.
    #[must_use]
    pub fn retain_when_done(self) -> Self {
        Self {
            spec: self.spec.retain_when_done(),
        }
    }

    /// Requires the task to call [`TaskContext::mark_ready`](crate::supervisor::TaskContext::mark_ready)
    /// before it is considered started.
    ///
    /// The dynamic membership remains in its starting state until this signal.
    /// If the task exits first, it is marked startup-aborted and cannot restart.
    /// There is no built-in readiness timeout; use a timeout inside the task
    /// when initialization must be bounded.
    #[must_use]
    pub fn wait_for_ready(self) -> Self {
        Self {
            spec: self.spec.wait_for_ready(),
        }
    }

    /// Returns the task's unique identifier.
    pub fn id(&self) -> &str {
        self.spec.id()
    }

    pub(crate) fn into_spec(self) -> ChildSpec {
        self.spec
    }
}

impl ChildSpec {
    fn map_inner(mut self, update: impl FnOnce(&mut ChildDefinition)) -> Self {
        let inner = ChildDefinition::make_mut_preserving_supervisor_identity(&mut self.inner);
        update(inner);
        self
    }

    /// Creates a nested-supervisor specification.
    ///
    /// `id` must be unique among siblings within the same parent supervisor.
    pub(crate) fn supervisor(id: impl Into<String>, supervisor: Supervisor) -> Self {
        Self {
            inner: Arc::new(ChildDefinition {
                id: id.into(),
                restart: RestartPolicy::default(),
                restart_is_default: true,
                shutdown_policy: Shutdown::default(),
                shutdown_is_default: true,
                remove_when_done: false,
                readiness: ChildReadiness::Explicit,
                attachment: None,
                kind: ChildKind::Supervisor(supervisor),
            }),
        }
    }

    /// Sets this child's complete restart declaration, replacing any
    /// inherited scope default.
    #[must_use]
    pub(crate) fn restart(self, restart: RestartPolicy) -> Self {
        self.map_inner(|inner| {
            inner.restart = restart;
            inner.restart_is_default = false;
        })
    }

    /// Sets this child's shutdown policy, replacing any inherited scope
    /// default.
    #[must_use]
    pub(crate) fn shutdown(self, policy: Shutdown) -> Self {
        self.map_inner(|inner| {
            inner.shutdown_policy = policy;
            inner.shutdown_is_default = false;
        })
    }

    #[must_use]
    pub(crate) fn remove_when_done(self) -> Self {
        self.map_inner(|inner| inner.remove_when_done = true)
    }

    #[must_use]
    pub(crate) fn retain_when_done(self) -> Self {
        self.map_inner(|inner| inner.remove_when_done = false)
    }

    /// Attaches process-local metadata to this supervised child.
    ///
    /// The value can be read through
    /// [`SupervisorHandle::attached_children`](crate::supervisor::SupervisorHandle::attached_children)
    /// and is deliberately excluded from serializable snapshots.
    #[must_use]
    pub(crate) fn attachment<T>(self, attachment: T) -> Self
    where
        T: Any + Send + Sync,
    {
        self.map_inner(|inner| inner.attachment = Some(Arc::new(attachment)))
    }

    /// Requires the child to report readiness before it is considered
    /// started. See [`TaskSpec::wait_for_ready`] for the full contract.
    #[must_use]
    pub(crate) fn wait_for_ready(self) -> Self {
        self.map_inner(|inner| inner.readiness = ChildReadiness::Explicit)
    }

    /// Returns the child's unique identifier.
    pub(crate) fn id(&self) -> &str {
        &self.inner.id
    }

    pub(crate) fn resolved_policies(
        &self,
        default_restart: RestartPolicy,
        default_shutdown: Shutdown,
    ) -> (RestartPolicy, Shutdown) {
        let restart = if self.inner.restart_is_default {
            default_restart
        } else {
            self.inner.restart
        };
        let shutdown = if self.inner.shutdown_is_default {
            default_shutdown
        } else {
            self.inner.shutdown_policy
        };
        (restart, shutdown)
    }
}

impl ChildDefinition {
    /// Returns the uniquely owned definition inside a linear child spec.
    pub(crate) fn make_mut_preserving_supervisor_identity(definition: &mut Arc<Self>) -> &mut Self {
        Arc::get_mut(definition).expect("a child specification is uniquely owned while edited")
    }

    pub(crate) fn apply_defaults(&mut self, restart: RestartPolicy, shutdown: Shutdown) {
        if self.restart_is_default {
            self.restart = restart;
        }
        if self.shutdown_is_default {
            self.shutdown_policy = shutdown;
        }
    }
}

impl Clone for ChildKind {
    fn clone(&self) -> Self {
        match self {
            Self::Task(factory) => Self::Task(Arc::clone(factory)),
            Self::Supervisor(supervisor) => Self::Supervisor(supervisor.instantiate_runtime()),
        }
    }
}
