use std::{
    future::Future,
    io::Error as IoError,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::supervisor::{CancelOnDrop, CancellationToken, ExitStatus, RestartPolicy};
#[cfg(feature = "host")]
use crate::supervisor::{MailboxShutdown, Shutdown};
use thiserror::Error;
use tokio::sync::oneshot;
#[cfg(feature = "host")]
use tokio::time::sleep;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;

use crate::{
    ScopeRef,
    actor::{
        binding::{
            ActorStats, BindingCore, BindingGuard, BindingLifecycle, Mailbox, MailboxRef, mailbox,
        },
        context::{ActorLifetime, ActorReadySignal, ActorRef, RawContext},
        factory::ActorFactory,
        monitor::MonitorRun,
        observability::{ActorExitStatus, ScopeObservability},
        raw::{BoxError, RawActor},
    },
};

pub(crate) const DEFAULT_MAILBOX_CAPACITY: usize = 64;

pub(crate) type BoxedActorFuture =
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;

#[derive(Debug, Error)]
#[error("manual readiness timed out after {0:?}")]
struct ManualReadinessTimedOut(Duration);

pub(crate) struct RunnerStart {
    pub(crate) shutdown: CancellationToken,
    pub(crate) mailbox_capacity: usize,
    pub(crate) observability: ScopeObservability,
    pub(crate) restart_policy: RestartPolicy,
    pub(crate) drain_messages: bool,
    pub(crate) ready: oneshot::Sender<()>,
    pub(crate) supervisor: ScopeRef,
    exit_reporter: ActorExitReporter,
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
        mailbox: Mailbox<M>,
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
        mailbox: Mailbox<M>,
    ) -> Arc<dyn ErasedRunner> {
        Arc::new(TypedRunner {
            factory: Arc::new(*self),
            binding,
            mailbox,
        })
    }
}

pub(crate) struct TypedRunner<F: ActorFactory> {
    pub(crate) factory: Arc<F>,
    pub(crate) binding: Arc<BindingCore<<F::Actor as RawActor>::Msg>>,
    pub(crate) mailbox: Mailbox<<F::Actor as RawActor>::Msg>,
}

impl<F> ErasedRunner for TypedRunner<F>
where
    F: ActorFactory,
{
    fn start(&self, start: RunnerStart) -> BoxedActorFuture {
        let factory = self.factory.clone();
        let binding = self.binding.clone();
        let mailbox_config = self.mailbox.clone();

        Box::pin(async move {
            let actor_shutdown = start.shutdown;
            let observability = start.observability;
            let (sender, mailbox) = mailbox(&mailbox_config, start.mailbox_capacity);
            let actor_id = binding.actor_id().clone();
            let incarnation = MailboxRef::new(actor_id.clone(), sender);
            let exit_report = ActorTaskExitGuard::new(start.exit_reporter);
            let Some(bound_mailbox) = BindingGuard::bind(
                binding.clone(),
                incarnation.clone(),
                exit_report.monitor_run(),
                observability.clone(),
                start.restart_policy,
            ) else {
                tracing::debug!(
                    actor_id = %actor_id,
                    "actor incarnation binding was superseded before startup"
                );
                return Ok(());
            };
            // Binding is deliberately established before construction so a
            // constructor panic follows the same monitoring and supervision
            // path as startup and run panics.
            let mut actor = factory.build();
            let manual_readiness = actor.manual_readiness();
            let ready = ActorReadySignal::new(start.ready, manual_readiness.is_some());
            let monitors = bound_mailbox.monitor_lease();
            let myself = ActorRef::from_core(&binding, Some(actor_id.clone()));
            let ctx = RawContext {
                id: actor_id.clone(),
                mailbox,
                myself,
                shutdown: actor_shutdown,
                drain_messages: start.drain_messages,
                observability,
                timers: Default::default(),
                lifetime: ActorLifetime::new(),
                monitors,
                ready: Some(ready.clone()),
                continuations: Default::default(),
                stop_requested: false,
                offloads: Default::default(),
                supervisor: start.supervisor,
            };
            let _bound_mailbox = bound_mailbox;
            let result = {
                let future = actor.run(ctx);
                tokio::pin!(future);
                let immediate_ready = ready.clone();
                let mut first_poll = true;
                let managed = std::future::poll_fn(|cx| {
                    let result = future.as_mut().poll(cx);
                    if first_poll {
                        first_poll = false;
                        immediate_ready.mark_immediate();
                    }
                    result
                });
                if let Some(timeout) = manual_readiness {
                    tokio::pin!(managed);
                    tokio::select! {
                        biased;
                        result = &mut managed => result,
                        () = ready.wait() => managed.await,
                        () = tokio::time::sleep(timeout) => {
                            Err(ManualReadinessTimedOut(timeout).into())
                        }
                    }
                } else {
                    managed.await
                }
            };
            drop(actor);
            exit_report.report_result(&result);
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
    /// The actor did not report manual startup readiness within its declared
    /// bound.
    #[error("actor `{actor_id}` did not report readiness within {timeout:?}")]
    #[non_exhaustive]
    ReadinessTimedOut {
        /// Stable id of the actor that missed its readiness deadline.
        actor_id: String,
        /// Bound declared by [`RawActor::manual_readiness`].
        timeout: Duration,
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
#[cfg(feature = "host")]
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
    #[cfg(feature = "host")]
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// Reports whether the host's shutdown future resolved before the actor
    /// stopped.
    ///
    /// A supervision loop that restarts on a clean stop still ends on this.
    #[must_use]
    #[cfg(feature = "host")]
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
#[cfg(feature = "host")]
pub struct ActorHost {
    actor: RunnableActor,
    mailbox_shutdown: MailboxShutdown,
}

#[cfg(feature = "host")]
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
            !shutdown_policy.is_abort()
        };
        self.actor
            .run_incarnation_until_ready(
                bounded_shutdown,
                abort,
                IncarnationControl {
                    restart: RestartPolicy::always(),
                    drain_messages: self.mailbox_shutdown.drains(),
                    dropped_is_cancelled: false,
                },
                ScopeRef::unavailable(),
                || {},
            )
            .await
    }
}

#[cfg(feature = "host")]
impl Drop for ActorHost {
    fn drop(&mut self) {
        self.actor.terminate_binding();
    }
}

#[cfg(feature = "host")]
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

#[derive(Clone)]
struct ActorExitReporter {
    inner: Arc<ActorExitReporterInner>,
}

struct ActorExitReporterInner {
    monitor_run: MonitorRun,
    state: Mutex<ActorExitReportState>,
}

struct ActorExitReportState {
    fallback: ExitStatus,
    shutdown_requested: bool,
    reported: bool,
}

impl ActorExitReporter {
    fn new(monitor_run: MonitorRun, dropped_is_cancelled: bool) -> Self {
        Self {
            inner: Arc::new(ActorExitReporterInner {
                monitor_run,
                state: Mutex::new(ActorExitReportState {
                    fallback: ExitStatus::Aborted {
                        after_grace: false,
                        cancelled: dropped_is_cancelled,
                    },
                    shutdown_requested: false,
                    reported: false,
                }),
            }),
        }
    }

    fn shutdown_requested(&self) {
        let mut state = self.state();
        if !state.reported {
            state.shutdown_requested = true;
            if let ExitStatus::Aborted { cancelled, .. } = &mut state.fallback {
                *cancelled = true;
            }
        }
    }

    fn aborted(&self, after_grace: bool) {
        let mut state = self.state();
        if !state.reported {
            state.shutdown_requested = true;
            state.fallback = ExitStatus::Aborted {
                after_grace,
                cancelled: true,
            };
        }
    }

    fn report_result(&self, result: &Result<(), BoxError>) {
        let message = result.as_ref().err().map(ToString::to_string);
        let Some(cancelled) = self.claim() else {
            return;
        };
        let status = match message {
            Some(message) => ExitStatus::Failed { message, cancelled },
            None => ExitStatus::Completed { cancelled },
        };
        self.inner.monitor_run.exited(status);
    }

    fn report_panicked(&self) {
        let Some(cancelled) = self.claim() else {
            return;
        };
        self.inner
            .monitor_run
            .exited(ExitStatus::Panicked { cancelled });
    }

    fn report_fallback(&self) {
        let status = {
            let mut state = self.state();
            if state.reported {
                return;
            }
            state.reported = true;
            state.fallback.clone()
        };
        self.inner.monitor_run.exited(status);
    }

    fn claim(&self) -> Option<bool> {
        let mut state = self.state();
        if state.reported {
            return None;
        }
        state.reported = true;
        Some(state.shutdown_requested)
    }

    fn state(&self) -> MutexGuard<'_, ActorExitReportState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

struct ActorTaskExitGuard {
    reporter: ActorExitReporter,
}

impl ActorTaskExitGuard {
    fn new(reporter: ActorExitReporter) -> Self {
        Self { reporter }
    }

    fn report_result(&self, result: &Result<(), BoxError>) {
        self.reporter.report_result(result);
    }

    fn monitor_run(&self) -> &MonitorRun {
        &self.reporter.inner.monitor_run
    }
}

impl Drop for ActorTaskExitGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.reporter.report_panicked();
        } else {
            self.reporter.report_fallback();
        }
    }
}

struct IncarnationControl {
    restart: RestartPolicy,
    drain_messages: bool,
    dropped_is_cancelled: bool,
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

    pub(crate) fn identity(&self) -> &Arc<()> {
        self.inner.binding_lifecycle.identity()
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
        A: Future<Output = bool>,
        R: FnOnce(),
    {
        let exit = self
            .run_incarnation_until_ready(
                shutdown,
                abort,
                IncarnationControl {
                    restart,
                    drain_messages,
                    dropped_is_cancelled: true,
                },
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
        control: IncarnationControl,
        supervisor: ScopeRef,
        ready: R,
    ) -> IncarnationExit
    where
        F: Future<Output = ()>,
        A: Future<Output = bool>,
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
        let monitor_run = self.inner.binding_lifecycle.monitor_run();
        let exit_reporter = ActorExitReporter::new(monitor_run, control.dropped_is_cancelled);
        let actor_span = self.inner.observability.actor_span(&actor_id);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let mut actor_task = AbortOnDropHandle::new(tokio::spawn(
            self.inner
                .runner
                .start(RunnerStart {
                    shutdown: actor_shutdown.clone(),
                    mailbox_capacity: self.inner.mailbox_capacity,
                    observability: self.inner.observability.clone(),
                    restart_policy: control.restart,
                    drain_messages: control.drain_messages,
                    ready: ready_tx,
                    supervisor,
                    exit_reporter: exit_reporter.clone(),
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
                after_grace = abort.as_mut(), if !shutdown_timed_out => {
                    exit_reporter.aborted(after_grace);
                    shutdown_requested = true;
                    shutdown_timed_out = true;
                    actor_shutdown.cancel();
                    actor_task.abort();
                }
                joined = &mut actor_task => break joined,
                _ = shutdown.as_mut(), if !shutdown_requested => {
                    exit_reporter.shutdown_requested();
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
                let error = match source.downcast::<ManualReadinessTimedOut>() {
                    Ok(timeout) => ActorRunError::ReadinessTimedOut {
                        actor_id: actor_id.to_string(),
                        timeout: timeout.0,
                    },
                    Err(source) => ActorRunError::Failed {
                        actor_id: actor_id.to_string(),
                        source,
                    },
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
        mailbox: Mailbox<M>,
    ) -> RunnableActor {
        let mailbox_capacity = mailbox.capacity_or(self.mailbox_capacity);
        let runner = factory.into_runner(Arc::clone(&binding), mailbox);
        RunnableActor::new(RunnableActorParts {
            actor_id,
            binding_lifecycle: binding,
            runner,
            mailbox_capacity,
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
