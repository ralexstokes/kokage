use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use crate::{
    context::ChildContext,
    restart::{RestartConfig, RestartPolicy},
    shutdown::ShutdownPolicy,
    supervisor::Supervisor,
};

/// A type-erased, thread-safe error type used as the `Err` half of
/// [`ChildResult`].
///
/// This is re-exported as `tokio_otp::host::BoxError` by the actor layer.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The result type returned by every supervised child function.
///
/// Returning `Ok(())` signals a clean exit. Returning an error signals a
/// failure, which may trigger a restart depending on the child's
/// [`RestartPolicy`].
pub type ChildResult = Result<(), BoxError>;

pub(crate) type ChildFuture = Pin<Box<dyn Future<Output = ChildResult> + Send + 'static>>;
pub(crate) type OpaqueAttachment = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ChildDefinition {
    pub(crate) id: String,
    pub(crate) restart: RestartPolicy,
    restart_is_default: bool,
    pub(crate) remove_on_exit: bool,
    pub(crate) restart_intensity: Option<RestartConfig>,
    pub(crate) shutdown_policy: ShutdownPolicy,
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

#[derive(Clone)]
pub(crate) enum ChildKind {
    Task(Arc<dyn ChildFactory>),
    Supervisor(Supervisor),
}

/// Specification for a supervised child task or nested supervisor.
///
/// Construct one with [`task`](Self::task) or [`supervisor`](Self::supervisor),
/// then apply the same restart, shutdown, and intensity policies to either
/// kind of child.
///
/// Cloning a task spec shares its factory. Cloning a nested-supervisor spec
/// copies the supervisor configuration while reserving a fresh supervisor
/// identity, matching [`Supervisor`]'s clone contract.
pub struct ChildSpec {
    pub(crate) inner: Arc<ChildDefinition>,
}

impl Clone for ChildSpec {
    fn clone(&self) -> Self {
        let inner = match &self.inner.kind {
            ChildKind::Task(_) => Arc::clone(&self.inner),
            ChildKind::Supervisor(_) => Arc::new((*self.inner).clone()),
        };
        Self { inner }
    }
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
                restart: RestartPolicy::default(),
                restart_is_default: true,
                remove_on_exit: false,
                restart_intensity: None,
                shutdown_policy: ShutdownPolicy::default(),
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
    pub fn supervisor(id: impl Into<String>, supervisor: Supervisor) -> Self {
        Self {
            inner: Arc::new(ChildDefinition {
                id: id.into(),
                restart: RestartPolicy::default(),
                restart_is_default: true,
                remove_on_exit: false,
                restart_intensity: None,
                shutdown_policy: ShutdownPolicy::default(),
                shutdown_is_default: true,
                readiness: ChildReadiness::Explicit,
                attachment: None,
                kind: ChildKind::Supervisor(supervisor),
            }),
        }
    }

    /// Sets the restart policy for this child. See [`RestartPolicy`] for options.
    #[must_use]
    pub fn restart(self, restart: RestartPolicy) -> Self {
        self.map_inner(|inner| {
            inner.restart = restart;
            inner.restart_is_default = false;
        })
    }

    /// Sets whether this child is removed after an exit that its restart
    /// policy declines to restart.
    ///
    /// This defaults to `false`, preserving the terminal child in supervisor
    /// snapshots. It is primarily useful for children added at runtime, where
    /// removal also makes the child id available for reuse. Restarted exits do
    /// not remove the child.
    ///
    /// Under [`Strategy::OneForAll`](crate::Strategy::OneForAll) and
    /// [`Strategy::RestForOne`](crate::Strategy::RestForOne), opting a
    /// non-[`RestartPolicy::Never`] child into removal makes a non-restarted
    /// exit permanent: a later group restart cannot revive the removed child.
    /// If the exit is instead observed while a group restart is already
    /// draining that child, it is part of the restart cycle and the child is
    /// respawned rather than removed.
    ///
    /// Removing a child also removes its exit status from supervisor
    /// snapshots, and so from any
    /// [`wait_completed`](crate::SupervisorHandle::wait_completed) set that
    /// awaits it.
    #[must_use]
    pub fn remove_on_exit(self, remove_on_exit: bool) -> Self {
        self.map_inner(|inner| inner.remove_on_exit = remove_on_exit)
    }

    /// Sets the shutdown policy for this child. See [`ShutdownPolicy`] for
    /// options.
    #[must_use]
    pub fn shutdown(self, policy: ShutdownPolicy) -> Self {
        self.map_inner(|inner| {
            inner.shutdown_policy = policy;
            inner.shutdown_is_default = false;
        })
    }

    /// Overrides the supervisor-level [`RestartConfig`] for this child.
    ///
    /// When set, this child tracks its own sliding restart window instead of
    /// sharing the supervisor's default.
    #[must_use]
    pub fn restart_intensity(self, intensity: RestartConfig) -> Self {
        self.map_inner(|inner| inner.restart_intensity = Some(intensity))
    }

    /// Attaches process-local metadata to this supervised child.
    ///
    /// The value can be read through
    /// [`SupervisorHandle::attached_children`](crate::SupervisorHandle::attached_children)
    /// and is deliberately excluded from serializable snapshots.
    #[must_use]
    pub(crate) fn attachment<T>(self, attachment: T) -> Self
    where
        T: Any + Send + Sync,
    {
        self.map_inner(|inner| inner.attachment = Some(Arc::new(attachment)))
    }

    /// Requires the child to call [`ChildContext::mark_ready`](crate::ChildContext::mark_ready)
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

    pub(crate) fn restart_intensity_override(&self) -> Option<RestartConfig> {
        self.inner.restart_intensity
    }
}

impl ChildDefinition {
    /// Returns a mutable definition without ever invoking `Supervisor::clone`
    /// as an accidental consequence of `Arc` copy-on-write.
    ///
    /// Task definitions may be shared because cloning a task spec intentionally
    /// shares its factory. Nested-supervisor specs, by contrast, receive a
    /// unique definition in `ChildSpec::clone`; losing that invariant is a bug
    /// because cloning the embedded supervisor would mint a different stable
    /// identity.
    pub(crate) fn make_mut_preserving_supervisor_identity(definition: &mut Arc<Self>) -> &mut Self {
        if matches!(&definition.kind, ChildKind::Supervisor(_)) {
            Arc::get_mut(definition)
                .expect("nested supervisor child definitions must be uniquely owned while edited")
        } else {
            Arc::make_mut(definition)
        }
    }

    pub(crate) fn apply_defaults(&mut self, restart: RestartPolicy, shutdown: ShutdownPolicy) {
        if self.restart_is_default {
            self.restart = restart;
        }
        if self.shutdown_is_default {
            self.shutdown_policy = shutdown;
        }
    }
}
