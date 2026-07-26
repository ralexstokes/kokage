use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use crate::{
    context::ChildContext,
    restart::{RestartIntensity, RestartPolicy},
    shutdown::ShutdownPolicy,
    supervisor::Supervisor,
};

/// A type-erased, thread-safe error type used as the `Err` half of
/// [`ChildResult`].
///
/// This is identical to and interchangeable with `tokio_otp::BoxError`.
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
    pub(crate) restart_intensity: Option<RestartIntensity>,
    pub(crate) shutdown_policy: ShutdownPolicy,
    shutdown_is_default: bool,
    pub(crate) significant: bool,
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

/// Specification for a supervised child task.
///
/// A `ChildSpec` pairs an async factory function with restart, shutdown, and
/// intensity policies. The factory is called each time the supervisor (re)starts
/// the child, receiving a fresh [`ChildContext`] with a new generation counter
/// and cancellation token.
///
/// The inner state is reference-counted, so cloning a `ChildSpec` is cheap and
/// shares the same factory.
#[derive(Clone)]
pub struct ChildSpec {
    pub(crate) inner: Arc<ChildDefinition>,
}

/// Specification for a nested supervisor child.
///
/// A `SupervisorSpec` pairs an already built [`Supervisor`] with the child id
/// it takes in its parent and the same restart, shutdown, and intensity
/// policies available on [`ChildSpec`]. It is the supervisor-shaped peer of
/// `ChildSpec`: both are complete, self-describing declarations accepted by
/// [`SupervisorBuilder::supervisor`](crate::SupervisorBuilder::supervisor) and
/// [`SupervisorHandle::add_supervisor`](crate::SupervisorHandle::add_supervisor).
#[derive(Clone)]
pub struct SupervisorSpec {
    pub(crate) id: String,
    pub(crate) supervisor: Supervisor,
    pub(crate) restart: RestartPolicy,
    restart_is_default: bool,
    pub(crate) restart_intensity: Option<RestartIntensity>,
    pub(crate) shutdown_policy: ShutdownPolicy,
    shutdown_is_default: bool,
    pub(crate) significant: bool,
    pub(crate) attachment: Option<OpaqueAttachment>,
}

impl SupervisorSpec {
    /// Creates a new nested-supervisor specification with the standard
    /// policy defaults.
    ///
    /// `id` must be unique among siblings within the same parent supervisor.
    pub fn new(id: impl Into<String>, supervisor: Supervisor) -> Self {
        Self {
            id: id.into(),
            supervisor,
            restart: RestartPolicy::default(),
            restart_is_default: true,
            restart_intensity: None,
            shutdown_policy: ShutdownPolicy::default(),
            shutdown_is_default: true,
            significant: false,
            attachment: None,
        }
    }

    /// Sets the restart policy for the nested supervisor child.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self.restart_is_default = false;
        self
    }

    /// Sets the shutdown policy for the nested supervisor child.
    #[must_use]
    pub fn shutdown(mut self, policy: ShutdownPolicy) -> Self {
        self.shutdown_policy = policy;
        self.shutdown_is_default = false;
        self
    }

    /// Overrides the parent supervisor's restart intensity for this child.
    #[must_use]
    pub fn restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = Some(intensity);
        self
    }

    /// Marks this nested supervisor as significant to its parent.
    ///
    /// A clean exit can trigger the parent's configured
    /// [`AutoShutdown`](crate::AutoShutdown) mode. Significant children cannot
    /// use [`RestartPolicy::Always`].
    #[must_use]
    pub fn significant(mut self) -> Self {
        self.significant = true;
        self
    }

    /// Attaches process-local metadata to this supervised child.
    ///
    /// The value can be read through
    /// [`SupervisorHandle::attached_children`](crate::SupervisorHandle::attached_children)
    /// and is deliberately excluded from serializable snapshots.
    #[must_use]
    pub fn attachment<T>(mut self, attachment: T) -> Self
    where
        T: Any + Send + Sync,
    {
        self.attachment = Some(Arc::new(attachment));
        self
    }

    /// Returns the nested supervisor's unique identifier in its parent.
    pub fn id(&self) -> &str {
        &self.id
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
        let inner = Arc::make_mut(&mut self.inner);
        update(inner);
        self
    }

    /// Creates a new child specification.
    ///
    /// `id` must be unique among siblings within the same supervisor.
    ///
    /// `f` is an async factory that is invoked each time the child is
    /// (re)started. It receives a [`ChildContext`] and should return
    /// `Ok(())` for a clean exit or an error for a failure.
    pub fn new<F, Fut>(id: impl Into<String>, f: F) -> Self
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
                significant: false,
                readiness: ChildReadiness::Immediate,
                attachment: None,
                kind: ChildKind::Task(make_child_factory(f)),
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
    /// Removing a significant child also removes its exit status from
    /// supervisor snapshots and from
    /// [`AutoShutdown::AllSignificant`](crate::AutoShutdown::AllSignificant)
    /// accounting. In particular, a removed significant child's failed exit
    /// cannot block auto-shutdown after the remaining significant children
    /// complete.
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

    /// Overrides the supervisor-level [`RestartIntensity`] for this child.
    ///
    /// When set, this child tracks its own sliding restart window instead of
    /// sharing the supervisor's default.
    #[must_use]
    pub fn restart_intensity(self, intensity: RestartIntensity) -> Self {
        self.map_inner(|inner| inner.restart_intensity = Some(intensity))
    }

    /// Marks this child as significant to its supervisor.
    ///
    /// A clean exit can trigger the supervisor's configured
    /// [`AutoShutdown`](crate::AutoShutdown) mode. Significant children cannot
    /// use [`RestartPolicy::Always`], because a clean exit must be final before
    /// it can complete the supervisor's purpose.
    #[must_use]
    pub fn significant(self) -> Self {
        self.map_inner(|inner| inner.significant = true)
    }

    /// Attaches process-local metadata to this supervised child.
    ///
    /// The value can be read through
    /// [`SupervisorHandle::attached_children`](crate::SupervisorHandle::attached_children)
    /// and is deliberately excluded from serializable snapshots.
    #[must_use]
    pub fn attachment<T>(self, attachment: T) -> Self
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

    /// Returns the child's restart policy.
    pub fn restart_policy(&self) -> RestartPolicy {
        self.inner.restart
    }

    pub(crate) fn restart_intensity_override(&self) -> Option<RestartIntensity> {
        self.inner.restart_intensity
    }

    /// Returns the child's shutdown policy.
    pub fn shutdown_policy(&self) -> ShutdownPolicy {
        self.inner.shutdown_policy
    }

    /// Returns whether this child is significant to its supervisor.
    pub fn is_significant(&self) -> bool {
        self.inner.significant
    }
}

impl ChildDefinition {
    pub(crate) fn apply_defaults(&mut self, restart: RestartPolicy, shutdown: ShutdownPolicy) {
        if self.restart_is_default {
            self.restart = restart;
        }
        if self.shutdown_is_default {
            self.shutdown_policy = shutdown;
        }
    }

    pub(crate) fn supervisor(spec: SupervisorSpec) -> Self {
        Self {
            id: spec.id,
            restart: spec.restart,
            restart_is_default: spec.restart_is_default,
            remove_on_exit: false,
            restart_intensity: spec.restart_intensity,
            shutdown_policy: spec.shutdown_policy,
            shutdown_is_default: spec.shutdown_is_default,
            significant: spec.significant,
            readiness: ChildReadiness::Explicit,
            attachment: spec.attachment,
            kind: ChildKind::Supervisor(spec.supervisor),
        }
    }
}

impl SupervisorSpec {
    pub(crate) fn apply_defaults(&mut self, restart: RestartPolicy, shutdown: ShutdownPolicy) {
        if self.restart_is_default {
            self.restart = restart;
        }
        if self.shutdown_is_default {
            self.shutdown_policy = shutdown;
        }
    }
}
