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

use crate::supervisor::{
    CancelOnDrop, CancellationToken, MailboxShutdown, RestartPolicy, Shutdown,
};
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
        monitor::MonitorExitGuard,
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
    pub(crate) restart_policy: RestartPolicy,
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
            let status = if let Err(error) = &result {
                crate::observe::ExitStatus::Failed {
                    message: error.to_string(),
                    cancelled: false,
                }
            } else {
                crate::observe::ExitStatus::Completed { cancelled: false }
            };
            monitor_exit.report(status);
            result
        })
    }
}

/// Errors returned while directly hosting an actor.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ActorRunError {
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
/// [`ActorHost::run_once`] or [`ActorHost::run_incarnation`] when it has no
/// deadline of its own.
///
/// This matches the default grace of
/// [`Shutdown`](crate::Shutdown), so an actor behaves the same
/// whether it is hosted by hand or by an [`Tree`](crate::Tree).
pub const DEFAULT_SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// The result of one directly hosted actor incarnation.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "inspect the incarnation exit before deciding whether to restart"]
pub enum IncarnationExit {
    /// The actor stopped without an error or a shutdown request.
    Stopped,
    /// The actor incarnation failed.
    Failed(ActorRunError),
    /// The host's shutdown future resolved and the actor stopped cleanly.
    ShutdownRequested,
}

impl IncarnationExit {
    /// Reports whether the incarnation failed.
    ///
    /// This is the restart question a custom supervision loop asks, and it
    /// keeps answering it correctly as variants are added, unlike a `match`
    /// with a catch-all arm forced by `#[non_exhaustive]`.
    #[must_use]
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Reports whether the host's shutdown future resolved before the actor
    /// stopped.
    ///
    /// A supervision loop that restarts on a clean stop still ends on this.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        matches!(self, Self::ShutdownRequested)
    }

    /// Converts this exit into the result [`ActorHost::run_once`] returns,
    /// discarding the distinction between a clean stop and a requested
    /// shutdown.
    pub fn into_result(self) -> Result<(), ActorRunError> {
        match self {
            Self::Stopped | Self::ShutdownRequested => Ok(()),
            Self::Failed(error) => Err(error),
        }
    }
}

/// An owning host for directly running one actor declaration.
///
/// The host retains the actor's stable mailbox binding across calls to
/// [`run_incarnation`](Self::run_incarnation). Dropping it terminates that
/// binding, so [`ActorRef`] senders never wait for a rebind that cannot occur.
/// Directly hosted actors receive an unavailable [`ScopeRef`]: control
/// operations fail and observation streams are closed.
#[must_use = "dropping the actor host terminates its binding"]
pub struct ActorHost {
    actor: RunnableActor,
    mailbox_shutdown: MailboxShutdown,
}

impl ActorHost {
    pub(crate) fn new(actor: RunnableActor, mailbox_shutdown: MailboxShutdown) -> Self {
        Self {
            actor,
            mailbox_shutdown,
        }
    }

    /// Returns the actor label.
    pub fn label(&self) -> &str {
        self.actor.label()
    }

    /// Runs one actor incarnation and then terminates its binding.
    ///
    /// This method consumes the host. The binding therefore becomes terminal
    /// when the run returns, panics, or is cancelled by dropping its future.
    /// Use [`run_incarnation`](Self::run_incarnation) when a custom supervision
    /// loop may start another incarnation.
    pub async fn run_once<F>(
        mut self,
        shutdown: F,
        shutdown_policy: Shutdown,
    ) -> Result<(), ActorRunError>
    where
        F: Future<Output = ()>,
    {
        self.run_incarnation(shutdown, shutdown_policy)
            .await
            .into_result()
    }

    /// Runs one actor incarnation while retaining ownership of its binding.
    ///
    /// On return the binding is ready for a later incarnation. Inspect the
    /// exit and either run another incarnation or drop the host to make the
    /// binding terminal. Dropping this method's future aborts the active
    /// incarnation but leaves the host available to the caller.
    ///
    /// `shutdown_policy` bounds the complete shutdown path. The actor's
    /// [`MailboxShutdown`](crate::MailboxShutdown) declaration decides whether
    /// queued messages are drained or discarded. Exceeding the bound returns
    /// [`IncarnationExit::Failed`] containing
    /// [`ActorRunError::ShutdownTimedOut`]. Actor panics resume unwinding; a
    /// custom loop that deliberately recovers from panics must catch that
    /// unwind around the borrowed run future.
    ///
    /// # Restarting a failed incarnation
    ///
    /// A panic is not an [`IncarnationExit`] variant, so a loop that supervises
    /// panicking actors wraps the borrowed future in
    /// [`AssertUnwindSafe`](std::panic::AssertUnwindSafe) and catches the
    /// unwind itself. The host survives either way, so the next incarnation
    /// binds a fresh mailbox behind the same [`ActorRef`] handles.
    ///
    /// ```
    /// use std::{
    ///     future::pending,
    ///     panic::AssertUnwindSafe,
    ///     sync::{
    ///         Arc,
    ///         atomic::{AtomicUsize, Ordering},
    ///     },
    /// };
    ///
    /// use futures_util::FutureExt;
    /// use kokage::{
    ///     ActorSpec, ExitResult, Shutdown,
    ///     raw::{DEFAULT_SHUTDOWN_BOUND, RawActor, RawContext},
    /// };
    ///
    /// struct Flaky {
    ///     runs: Arc<AtomicUsize>,
    /// }
    ///
    /// impl RawActor for Flaky {
    ///     type Msg = ();
    ///
    ///     async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
    ///         if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
    ///             return Err("first incarnation fails".into());
    ///         }
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let runs = Arc::new(AtomicUsize::new(0));
    /// let mut host = ActorSpec::new("worker", {
    ///     let runs = Arc::clone(&runs);
    ///     move || Flaky {
    ///         runs: Arc::clone(&runs),
    ///     }
    /// })
    /// .into_host();
    ///
    /// for _ in 0..3 {
    ///     let exit = AssertUnwindSafe(
    ///         host.run_incarnation(pending::<()>(), Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND)),
    ///     )
    ///     .catch_unwind()
    ///     .await;
    ///
    ///     match exit {
    ///         // Panicked, or exited with an error: run another incarnation.
    ///         Err(_panic) => continue,
    ///         Ok(exit) if exit.is_failure() => continue,
    ///         // Stopped cleanly or was asked to shut down: give up the host.
    ///         Ok(_) => break,
    ///     }
    /// }
    ///
    /// assert_eq!(runs.load(Ordering::SeqCst), 2);
    /// // Dropping `host` here terminates the binding, so senders holding an
    /// // `ActorRef` fail fast instead of waiting for a rebind.
    /// # }
    /// ```
    pub async fn run_incarnation<F>(
        &mut self,
        shutdown: F,
        shutdown_policy: Shutdown,
    ) -> IncarnationExit
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
        self.actor
            .run_incarnation_until_ready(
                bounded_shutdown,
                abort,
                RestartPolicy::always(),
                self.mailbox_shutdown.drains(),
                ScopeRef::unavailable(),
                || {},
            )
            .await
    }
}

impl Drop for ActorHost {
    fn drop(&mut self) {
        self.actor.terminate_binding();
    }
}

impl std::fmt::Debug for ActorHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHost")
            .field("label", &self.label())
            .finish_non_exhaustive()
    }
}

/// Internal cloneable representation used by supervision machinery.
#[derive(Clone)]
pub(crate) struct RunnableActor {
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

    pub(crate) fn terminate_binding(&self) {
        self.apply_run_disposition(RunDisposition::Terminate);
    }

    pub(crate) async fn run_until_ready<F, A, R>(
        &self,
        shutdown: F,
        abort: A,
        restart: RestartPolicy,
        drain_messages: bool,
        supervisor: ScopeRef,
        ready: R,
    ) -> Result<(), ActorRunError>
    where
        F: Future<Output = ()>,
        A: Future<Output = ()>,
        R: FnOnce(),
    {
        let exit = self
            .run_incarnation_until_ready(
                shutdown,
                abort,
                restart,
                drain_messages,
                supervisor,
                ready,
            )
            .await;
        // Not reached for a panicking actor: that path resumes unwinding from
        // inside the call above. `BindingGuard::drop` already makes the same
        // transition this would, so the binding still lands where
        // `run_disposition` would have put it.
        self.apply_run_disposition(run_disposition(restart, &exit));
        exit.into_result()
    }

    async fn run_incarnation_until_ready<F, A, R>(
        &self,
        shutdown: F,
        abort: A,
        restart_on_drop: RestartPolicy,
        drain_messages: bool,
        supervisor: ScopeRef,
        ready: R,
    ) -> IncarnationExit
    where
        F: Future<Output = ()>,
        A: Future<Output = ()>,
        R: FnOnce(),
    {
        if self.inner.mailbox_capacity == 0 {
            return IncarnationExit::Failed(ActorRunError::ZeroMailboxCapacity {
                actor_id: self.inner.actor_id.to_string(),
            });
        }
        let _active_run = ActiveActorRun::start(&self.inner);
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
                    restart_policy: restart_on_drop,
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
                self.inner
                    .observability
                    .emit_actor_exited(&actor_id, status, None);
                if shutdown_requested {
                    IncarnationExit::ShutdownRequested
                } else {
                    IncarnationExit::Stopped
                }
            }
            Ok(Err(source)) => {
                let error = ActorRunError::Failed {
                    actor_id: actor_id.to_string(),
                    source,
                };
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Failed,
                    Some(&error.to_string()),
                );
                IncarnationExit::Failed(error)
            }
            Err(err) if err.is_panic() => {
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Panicked,
                    None,
                );
                // Unwinding skips every caller's post-run bookkeeping. The
                // binding is safe regardless: `TypedRunner::start` binds
                // before it builds the actor, so a live `BindingGuard`
                // always unwinds with the actor task and clears the binding
                // exactly as this exit's `run_disposition` would have.
                std::panic::resume_unwind(err.into_panic());
            }
            Err(_err) if shutdown_timed_out => {
                let error = ActorRunError::ShutdownTimedOut {
                    actor_id: actor_id.to_string(),
                };
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::ShutdownTimedOut,
                    Some(&error.to_string()),
                );
                IncarnationExit::Failed(error)
            }
            Err(_err) => {
                let source: BoxError = Box::new(IoError::other(format!(
                    "actor `{actor_id}` task was cancelled"
                )));
                let error = ActorRunError::Failed {
                    actor_id: actor_id.to_string(),
                    source,
                };
                self.inner.observability.emit_actor_exited(
                    &actor_id,
                    ActorExitStatus::Cancelled,
                    Some(&error.to_string()),
                );
                IncarnationExit::Failed(error)
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

fn run_disposition(policy: RestartPolicy, exit: &IncarnationExit) -> RunDisposition {
    if matches!(exit, IncarnationExit::ShutdownRequested) {
        return RunDisposition::Terminate;
    }

    let is_failure = matches!(exit, IncarnationExit::Failed(_));
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
        let stopped = IncarnationExit::Stopped;
        assert_eq!(
            run_disposition(RestartPolicy::always(), &stopped),
            RunDisposition::ExpectRebind
        );
        for policy in [RestartPolicy::on_failure(), RestartPolicy::never()] {
            assert_eq!(run_disposition(policy, &stopped), RunDisposition::Terminate);
        }

        let failed = IncarnationExit::Failed(ActorRunError::Failed {
            actor_id: "worker".to_owned(),
            source: Box::new(IoError::other("boom")),
        });
        for policy in [RestartPolicy::always(), RestartPolicy::on_failure()] {
            assert_eq!(
                run_disposition(policy, &failed),
                RunDisposition::ExpectRebind
            );
        }
        assert_eq!(
            run_disposition(RestartPolicy::never(), &failed),
            RunDisposition::Terminate
        );

        let shutdown = IncarnationExit::ShutdownRequested;
        for policy in [
            RestartPolicy::always(),
            RestartPolicy::on_failure(),
            RestartPolicy::never(),
        ] {
            assert_eq!(
                run_disposition(policy, &shutdown),
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
    fn start(inner: &Arc<RunnableActorInner>) -> Self {
        assert!(
            inner
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "actor `{}` started concurrent incarnations",
            inner.actor_id,
        );

        Self {
            inner: Arc::clone(inner),
        }
    }
}

impl Drop for ActiveActorRun {
    fn drop(&mut self) {
        self.inner.running.store(false, Ordering::Release);
    }
}
