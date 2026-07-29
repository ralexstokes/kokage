use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use crate::supervisor::{
    context::ChildContext, owner::Supervisor, restart::Restart, shutdown::Shutdown,
};

/// A type-erased, thread-safe error type used as the `Err` half of
/// [`ChildResult`].
///
/// This is re-exported as `kokage::host::BoxError` by the actor layer.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The result type returned by every supervised child function.
///
/// Returning `Ok(())` signals a clean exit. Returning an error signals a
/// failure, which may trigger a restart depending on the child's
/// [`Restart`].
pub type ChildResult = Result<(), BoxError>;

pub(crate) type ChildFuture = Pin<Box<dyn Future<Output = ChildResult> + Send + 'static>>;
pub(crate) type OpaqueAttachment = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ChildDefinition {
    pub(crate) id: String,
    pub(crate) restart: Restart,
    restart_is_default: bool,
    pub(crate) shutdown_policy: Shutdown,
    shutdown_is_default: bool,
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

/// Specification for a supervised child task.
///
/// Construct one with [`task`](Self::task), then apply restart and shutdown
/// policies. Nested scopes are built through Kokage's tree APIs.
///
pub struct ChildSpec {
    pub(crate) inner: Arc<ChildDefinition>,
}

pub(crate) trait ChildFactory: Send + Sync + 'static {
    fn make(&self, ctx: ChildContext) -> ChildFuture;
}

struct ClosureFactory<F> {
    f: F,
}

impl<F, Fut> ChildFactory for ClosureFactory<F>
where
    F: Fn(ChildContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ChildResult> + Send + 'static,
{
    fn make(&self, ctx: ChildContext) -> ChildFuture {
        Box::pin((self.f)(ctx))
    }
}

fn make_child_factory<F, Fut>(f: F) -> Arc<dyn ChildFactory>
where
    F: Fn(ChildContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ChildResult> + Send + 'static,
{
    Arc::new(ClosureFactory { f })
}

impl ChildSpec {
    fn map_inner(mut self, update: impl FnOnce(&mut ChildDefinition)) -> Self {
        let inner = ChildDefinition::make_mut_preserving_supervisor_identity(&mut self.inner);
        update(inner);
        self
    }

    /// Creates a supervised task specification.
    ///
    /// `id` must be unique among siblings within the same supervisor.
    ///
    /// `f` is an async factory that is invoked each time the child is
    /// (re)started. It receives a [`ChildContext`] and should return
    /// `Ok(())` for a clean exit or an error for a failure.
    pub fn task<F, Fut>(id: impl Into<String>, f: F) -> Self
    where
        F: Fn(ChildContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ChildResult> + Send + 'static,
    {
        Self {
            inner: Arc::new(ChildDefinition {
                id: id.into(),
                restart: Restart::default(),
                restart_is_default: true,
                shutdown_policy: Shutdown::default(),
                shutdown_is_default: true,
                readiness: ChildReadiness::Immediate,
                attachment: None,
                kind: ChildKind::Task(make_child_factory(f)),
            }),
        }
    }

    /// Creates a nested-supervisor specification.
    ///
    /// `id` must be unique among siblings within the same parent supervisor.
    pub(crate) fn supervisor(id: impl Into<String>, supervisor: Supervisor) -> Self {
        Self {
            inner: Arc::new(ChildDefinition {
                id: id.into(),
                restart: Restart::default(),
                restart_is_default: true,
                shutdown_policy: Shutdown::default(),
                shutdown_is_default: true,
                readiness: ChildReadiness::Explicit,
                attachment: None,
                kind: ChildKind::Supervisor(supervisor),
            }),
        }
    }

    /// Sets this child's complete restart declaration. See [`Restart`] for
    /// options.
    ///
    /// This replaces the inherited mode, budget, backoff, and terminal-removal
    /// behavior. Restate any scope-level values this child should retain.
    #[must_use]
    pub fn restart(self, restart: Restart) -> Self {
        self.map_inner(|inner| {
            inner.restart = restart;
            inner.restart_is_default = false;
        })
    }

    /// Sets the shutdown policy for this child. See [`Shutdown`] for
    /// options.
    #[must_use]
    pub fn shutdown(self, policy: Shutdown) -> Self {
        self.map_inner(|inner| {
            inner.shutdown_policy = policy;
            inner.shutdown_is_default = false;
        })
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

    /// Requires the child to call [`ChildContext::mark_ready`](crate::supervisor::ChildContext::mark_ready)
    /// before it is considered started.
    ///
    /// An ordered supervisor waits for this signal before starting its next
    /// declared child.
    /// If the child exits before reporting readiness, its ordinary restart
    /// policy applies. The sequence waits through a scheduled restart; if the
    /// exit is terminal, the child is marked startup-aborted and the sequence
    /// skips it. There is no built-in readiness timeout; use a timeout inside
    /// the child when initialization must be bounded. Shutdown and control
    /// commands remain responsive while a supervisor waits for readiness, so a
    /// child may await a control operation before calling `mark_ready`.
    #[must_use]
    pub fn wait_for_ready(self) -> Self {
        self.map_inner(|inner| inner.readiness = ChildReadiness::Explicit)
    }

    /// Returns the child's unique identifier.
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub(crate) fn resolved_policies(
        &self,
        default_restart: Restart,
        default_shutdown: Shutdown,
    ) -> (Restart, Shutdown) {
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

    pub(crate) fn apply_defaults(&mut self, restart: Restart, shutdown: Shutdown) {
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
