use std::{
    future::Future,
    io::Error as IoError,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{CancelOnDrop, CancellationToken, Restart, Shutdown};
use thiserror::Error;
use tokio::{sync::oneshot, time::sleep};
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;

use crate::{
    ScopeRef,
    actor::{
        binding::{
            ActorStats, BindingCore, BindingGuard, BindingLifecycle, MailboxMode, MailboxRef,
            mailbox,
        },
        context::{ActorLifetime, ActorRef, RawContext},
        factory::ActorFactory,
        monitor::{DownReason, MonitorExitGuard},
        observability::{ActorExitStatus, ScopeObservability},
        raw::{BoxError, RawActor},
    },
};

pub(crate) const DEFAULT_MAILBOX_CAPACITY: usize = 64;

pub(crate) type BoxedActorFuture =
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;

pub(crate) struct RunnerStart {
    pub(crate) shutdown: CancellationToken,
    pub(crate) mailbox_capacity: usize,
    pub(crate) observability: ScopeObservability,
    pub(crate) restart_policy: Restart,
    pub(crate) drain_messages: bool,
    pub(crate) ready: oneshot::Sender<()>,
    pub(crate) supervisor: ScopeRef,
}

/// Type-erased actor runner.
///
/// This is the only dyn layer in the crate: each implementation knows its own
/// message type and owns the typed binding core, so starting an actor binds a
/// typed mailbox without any downcast.
pub(crate) trait ErasedRunner: Send + Sync {
    fn start(&self, start: RunnerStart) -> BoxedActorFuture;
}

/// Consuming type-erasure boundary used by [`crate::ActorSpec`].
pub(crate) trait ErasedActorFactory<M>: Send {
    fn into_runner(
        self: Box<Self>,
        binding: Arc<BindingCore<M>>,
        mailbox_mode: MailboxMode<M>,
    ) -> Arc<dyn ErasedRunner>;
}

impl<M, F> ErasedActorFactory<M> for F
where
    M: Send + 'static,
    F: ActorFactory,
    F::Actor: RawActor<Msg = M>,
{
    fn into_runner(
        self: Box<Self>,
        binding: Arc<BindingCore<M>>,
        mailbox_mode: MailboxMode<M>,
    ) -> Arc<dyn ErasedRunner> {
        Arc::new(TypedRunner {
            factory: Arc::new(*self),
            binding,
            mailbox_mode,
        })
    }
}

pub(crate) struct TypedRunner<F: ActorFactory> {
    pub(crate) factory: Arc<F>,
    pub(crate) binding: Arc<BindingCore<<F::Actor as RawActor>::Msg>>,
    pub(crate) mailbox_mode: MailboxMode<<F::Actor as RawActor>::Msg>,
}

impl<F> ErasedRunner for TypedRunner<F>
where
    F: ActorFactory,
{
    fn start(&self, start: RunnerStart) -> BoxedActorFuture {
        let factory = self.factory.clone();
        let binding = self.binding.clone();
        let mailbox_mode = self.mailbox_mode.clone();

        Box::pin(async move {
            let actor_shutdown = start.shutdown;
            let monitors = binding.outbound_monitors();
            let observability = start.observability;
            let (sender, mailbox) = mailbox(&mailbox_mode, start.mailbox_capacity);
            let actor_id = binding.actor_id().clone();
            let incarnation = MailboxRef::new(actor_id.clone(), sender);
            let bound_mailbox = BindingGuard::bind(
                binding.clone(),
                incarnation.clone(),
                observability.clone(),
                start.restart_policy,
            );
            let myself = ActorRef::from_core(&binding, Some(actor_id.clone()));
            let monitor_hub = binding.monitor_hub();
            let mut ctx = RawContext {
                id: actor_id,
                mailbox,
                myself,
                shutdown: actor_shutdown,
                drain_messages: start.drain_messages,
                observability,
                timers: Default::default(),
                lifetime: ActorLifetime::new(),
                monitors,
                ready: Some(start.ready),
                continuations: Default::default(),
                stop_requested: false,
                offloads: Default::default(),
                scope_waits: Default::default(),
                scope_wait_gates: Default::default(),
                supervisor: start.supervisor,
            };
            let mut monitor_exit = MonitorExitGuard::new(monitor_hub);
            // Binding is deliberately deferred until this actor future's first
            // poll so construction happens inside the bound, instrumented
            // future. Constructor panics then follow the same binding,
            // monitoring, and supervision path as startup and run panics.
            let mut actor = factory.build();
            if !actor.readiness_gated() {
                ctx.mark_ready();
            }
            let _bound_mailbox = bound_mailbox;
            let result = actor.run(ctx).await;
            let reason = if result.is_ok() {
                DownReason::Normal
            } else {
                DownReason::Failure
            };
            monitor_exit.report(reason);
            result
        })
    }
}

/// Errors returned from [`RunnableActor::run_until`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ActorRunError {
    /// Another instance of the same runnable actor is already active.
    #[error("actor `{actor_id}` is already running")]
    #[non_exhaustive]
    AlreadyRunning {
        /// Stable id of the actor whose existing run is still active.
        actor_id: String,
    },
    /// The actor returned an error.
    #[error("actor `{actor_id}` returned an error")]
    #[non_exhaustive]
    Failed {
        /// Stable id of the actor that failed.
        actor_id: String,
        /// Error returned by the actor.
        #[source]
        source: BoxError,
    },
    /// The actor did not finish its drain and stop hooks within the host's
    /// shutdown bound.
    #[error("actor `{actor_id}` shutdown timed out")]
    #[non_exhaustive]
    ShutdownTimedOut {
        /// Stable id of the actor whose shutdown timed out.
        actor_id: String,
    },
    /// The declaration carries a zero mailbox capacity.
    ///
    /// Supervised placement rejects this at spawn; a direct host learns it
    /// here, when the run starts.
    #[error("actor `{actor_id}` has a zero mailbox capacity")]
    #[non_exhaustive]
    ZeroMailboxCapacity {
        /// Stable id of the actor whose declaration is invalid.
        actor_id: String,
    },
}

/// The shutdown bound a standalone host should pass to
/// [`RunnableActor::run_until`] when it has no deadline of its own.
///
/// This matches the default grace of
/// [`Shutdown`](crate::Shutdown), so an actor behaves the same
/// whether it is hosted by hand or by an [`OrderedTree`](crate::OrderedTree).
pub const DEFAULT_SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// A single actor declaration, ready to be run independently.
///
/// Retains a stable mailbox binding, so [`ActorRef`] handles
/// keep working across restarts. Use [`run_until`](Self::run_until) to drive
/// one actor incarnation.
#[derive(Clone)]
pub struct RunnableActor {
    inner: Arc<RunnableActorInner>,
}

struct RunnableActorInner {
    actor_id: Arc<str>,
    binding_lifecycle: Arc<dyn BindingLifecycle>,
    runner: Arc<dyn ErasedRunner>,
    mailbox_capacity: usize,
    observability: ScopeObservability,
    running: AtomicBool,
}

pub(crate) struct RunnableActorParts {
    pub(crate) actor_id: Arc<str>,
    pub(crate) binding_lifecycle: Arc<dyn BindingLifecycle>,
    pub(crate) runner: Arc<dyn ErasedRunner>,
    pub(crate) mailbox_capacity: usize,
    pub(crate) observability: ScopeObservability,
}

impl RunnableActor {
    pub(crate) fn new(parts: RunnableActorParts) -> Self {
        Self {
            inner: Arc::new(RunnableActorInner {
                actor_id: parts.actor_id,
                binding_lifecycle: parts.binding_lifecycle,
                runner: parts.runner,
                mailbox_capacity: parts.mailbox_capacity,
                observability: parts.observability,
                running: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the actor label.
    pub fn label(&self) -> &str {
        &self.inner.actor_id
    }

    pub(crate) fn stats(&self) -> ActorStats {
        self.inner.binding_lifecycle.stats()
    }

    /// Marks the actor's binding terminated.
    ///
    /// Call this when no further run will be started so senders fail fast with
    /// [`SendError`](crate::SendError) instead of waiting for a rebind that
    /// will never come.
    pub fn terminate_binding(&self) {
        self.apply_run_disposition(RunDisposition::Terminate);
    }

    /// Runs this actor with a fresh mailbox until shutdown resolves.
    ///
    /// `restart` controls the binding disposition after the run, while
    /// `shutdown` controls both the actor drain behavior and the bound for the
    /// drain and stop-hook path, exactly as it does under a supervision tree.
    ///
    /// When the shutdown grace expires before the actor finishes draining, the
    /// inner task is aborted and the run resolves to
    /// [`ActorRunError::ShutdownTimedOut`] — a timeout is reported rather than
    /// laundered into a clean exit, so `.expect(..)` on the result will panic
    /// for an actor that overruns its bound.
    ///
    /// - A clean exit leaves the binding rebindable only for
    ///   [`Restart::always`].
    /// - Failure, panic, or unexpected task cancellation leaves it rebindable
    ///   for [`Restart::always`] and [`Restart::on_failure`].
    /// - Requested shutdown terminates the binding for every policy.
    /// - Dropping the `run_until` future aborts the incarnation and leaves the
    ///   binding rebindable for `Always` and `OnFailure`; `Never` terminates it.
    ///
    /// [`Restart::default`] uses [`Restart::on_failure`], so
    /// `run_until(shutdown, Default::default(), shutdown)` leaves a failed run
    /// rebindable. Pass [`Restart::never`] explicitly for a
    /// binding that terminates after every exit.
    ///
    /// A hand-written host must call
    /// [`terminate_binding`](Self::terminate_binding) when it gives up after a
    /// policy that left the binding waiting to rebind.
    ///
    /// Actors run through this unsupervised entry point receive a terminal
    /// [`ScopeRef`] from [`RawContext::supervisor`](crate::host::RawContext::supervisor):
    /// control operations return `ControlError::Unavailable` and observation
    /// streams are closed.
    pub async fn run_until<F>(
        &self,
        shutdown: F,
        restart: Restart,
        shutdown_policy: Shutdown,
    ) -> Result<(), ActorRunError>
    where
        F: Future<Output = ()>,
    {
        let shutdown_observed = CancellationToken::new();
        let deadline_start = shutdown_observed.clone();
        let bounded_shutdown = async move {
            shutdown.await;
            shutdown_observed.cancel();
        };
        let abort = async move {
            deadline_start.cancelled().await;
            if let Some(grace) = shutdown_policy.grace() {
                sleep(grace).await;
            }
        };
        self.run_until_ready(
            bounded_shutdown,
            abort,
            restart,
            matches!(shutdown_policy, Shutdown::Drain { .. }),
            ScopeRef::unavailable(),
            || {},
        )
        .await
    }

    pub(crate) async fn run_until_ready<F, A, R>(
        &self,
        shutdown: F,
        abort: A,
        restart: Restart,
        drain_messages: bool,
        supervisor: ScopeRef,
        ready: R,
    ) -> Result<(), ActorRunError>
    where
        F: Future<Output = ()>,
        A: Future<Output = ()>,
        R: FnOnce(),
    {
        if self.inner.mailbox_capacity == 0 {
            return Err(ActorRunError::ZeroMailboxCapacity {
                actor_id: self.inner.actor_id.to_string(),
            });
        }
        let _active_run = ActiveActorRun::start(&self.inner)?;
        let actor_id = self.inner.actor_id.clone();
        let actor_shutdown = CancellationToken::new();
        let mut shutdown = std::pin::pin!(shutdown);
        let mut abort = std::pin::pin!(abort);
        let actor_span = self.inner.observability.actor_span(&actor_id);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let mut actor_task = AbortOnDropHandle::new(tokio::spawn(
            self.inner
                .runner
                .start(RunnerStart {
                    shutdown: actor_shutdown.clone(),
                    mailbox_capacity: self.inner.mailbox_capacity,
                    observability: self.inner.observability.clone(),
                    restart_policy: restart,
                    drain_messages,
                    ready: ready_tx,
                    supervisor,
                })
                .instrument(actor_span),
        ));
        let _cancel_actor_on_drop = CancelOnDrop::new(actor_shutdown.clone());

        self.inner.observability.emit_actor_started(&actor_id);

        let mut shutdown_requested = false;
        let mut shutdown_timed_out = false;
        let mut ready = Some(ready);
        let result = loop {
            tokio::select! {
                biased;
                result = &mut ready_rx, if ready.is_some() => {
                    let ready = ready.take();
                    if result.is_ok() && let Some(ready) = ready {
                        ready();
                    }
                }
                _ = abort.as_mut(), if !shutdown_timed_out => {
                    shutdown_requested = true;
                    shutdown_timed_out = true;
                    actor_shutdown.cancel();
                    actor_task.abort();
                }
                joined = &mut actor_task => break joined,
                _ = shutdown.as_mut(), if !shutdown_requested => {
                    shutdown_requested = true;
                    actor_shutdown.cancel();
                }
            }
        };

        match result {
            Ok(Ok(())) => {
                let status = if shutdown_requested {
                    ActorExitStatus::Shutdown
                } else {
                    ActorExitStatus::Stopped
                };
                self.apply_run_disposition(run_disposition(restart, shutdown_requested, status));
                self.inner
                    .observability
                    .emit_actor_exited(&actor_id, status, None);
                Ok(())
            }
            Ok(Err(source)) => {
                let error = ActorRunError::Failed {
                    actor_id: actor_id.to_string(),
                    source,
                };
                self.apply_run_disposition(run_disposition(
                    restart,
                    shutdown_requested,
                    ActorExitStatus::Failed,
                ));
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Failed,
                    Some(&error.to_string()),
                );
                Err(error)
            }
            Err(err) if err.is_panic() => {
                self.apply_run_disposition(run_disposition(
                    restart,
                    shutdown_requested,
                    ActorExitStatus::Panicked,
                ));
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Panicked,
                    None,
                );
                std::panic::resume_unwind(err.into_panic());
            }
            Err(_err) if shutdown_timed_out => {
                let error = ActorRunError::ShutdownTimedOut {
                    actor_id: actor_id.to_string(),
                };
                self.apply_run_disposition(run_disposition(
                    restart,
                    shutdown_requested,
                    ActorExitStatus::ShutdownTimedOut,
                ));
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::ShutdownTimedOut,
                    Some(&error.to_string()),
                );
                Err(error)
            }
            Err(_err) => {
                let source: BoxError = Box::new(IoError::other(format!(
                    "actor `{actor_id}` task was cancelled"
                )));
                let error = ActorRunError::Failed {
                    actor_id: actor_id.to_string(),
                    source,
                };
                self.apply_run_disposition(run_disposition(
                    restart,
                    shutdown_requested,
                    ActorExitStatus::Cancelled,
                ));
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Cancelled,
                    Some(&error.to_string()),
                );
                Err(error)
            }
        }
    }

    fn apply_run_disposition(&self, disposition: RunDisposition) {
        match disposition {
            RunDisposition::ExpectRebind => self.inner.binding_lifecycle.unbind(),
            RunDisposition::Terminate => self.inner.binding_lifecycle.terminate(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunDisposition {
    ExpectRebind,
    Terminate,
}

fn run_disposition(
    policy: Restart,
    shutdown_requested: bool,
    status: ActorExitStatus,
) -> RunDisposition {
    if shutdown_requested || status == ActorExitStatus::Shutdown {
        return RunDisposition::Terminate;
    }

    let is_failure = !matches!(status, ActorExitStatus::Stopped | ActorExitStatus::Shutdown);
    if policy.should_restart(is_failure) {
        RunDisposition::ExpectRebind
    } else {
        RunDisposition::Terminate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_disposition_matches_documented_restart_semantics() {
        assert_eq!(
            run_disposition(Restart::always(), false, ActorExitStatus::Stopped),
            RunDisposition::ExpectRebind
        );
        for policy in [Restart::on_failure(), Restart::never()] {
            assert_eq!(
                run_disposition(policy, false, ActorExitStatus::Stopped),
                RunDisposition::Terminate
            );
        }

        for status in [
            ActorExitStatus::Failed,
            ActorExitStatus::Panicked,
            ActorExitStatus::Cancelled,
            ActorExitStatus::ShutdownTimedOut,
        ] {
            for policy in [Restart::always(), Restart::on_failure()] {
                assert_eq!(
                    run_disposition(policy, false, status),
                    RunDisposition::ExpectRebind
                );
            }
            assert_eq!(
                run_disposition(Restart::never(), false, status),
                RunDisposition::Terminate
            );
        }

        for policy in [Restart::always(), Restart::on_failure(), Restart::never()] {
            assert_eq!(
                run_disposition(policy, false, ActorExitStatus::Shutdown),
                RunDisposition::Terminate
            );
            assert_eq!(
                run_disposition(policy, true, ActorExitStatus::Failed),
                RunDisposition::Terminate
            );
        }
    }
}

impl std::fmt::Debug for RunnableActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunnableActor")
            .field("label", &self.label())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RunnableActorBuilder {
    observability: ScopeObservability,
    mailbox_capacity: usize,
}

impl RunnableActorBuilder {
    pub(crate) fn new() -> Self {
        Self {
            observability: ScopeObservability::new(),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
        }
    }

    pub(crate) fn with_mailbox_capacity(mailbox_capacity: usize) -> Self {
        Self {
            observability: ScopeObservability::new(),
            mailbox_capacity,
        }
    }

    pub(crate) fn actor_from_parts<M: Send + 'static>(
        &self,
        actor_id: Arc<str>,
        binding: Arc<BindingCore<M>>,
        factory: Box<dyn ErasedActorFactory<M>>,
        mailbox_mode: MailboxMode<M>,
        mailbox_capacity: Option<usize>,
    ) -> RunnableActor {
        let runner = factory.into_runner(Arc::clone(&binding), mailbox_mode);
        RunnableActor::new(RunnableActorParts {
            actor_id,
            binding_lifecycle: binding,
            runner,
            mailbox_capacity: mailbox_capacity.unwrap_or(self.mailbox_capacity),
            observability: self.observability.clone(),
        })
    }
}

impl Default for RunnableActorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct ActiveActorRun {
    inner: Arc<RunnableActorInner>,
}

impl ActiveActorRun {
    fn start(inner: &Arc<RunnableActorInner>) -> Result<Self, ActorRunError> {
        if inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ActorRunError::AlreadyRunning {
                actor_id: inner.actor_id.to_string(),
            });
        }

        Ok(Self {
            inner: Arc::clone(inner),
        })
    }
}

impl Drop for ActiveActorRun {
    fn drop(&mut self) {
        self.inner.running.store(false, Ordering::Release);
    }
}
