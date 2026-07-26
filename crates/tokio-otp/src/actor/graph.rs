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

use thiserror::Error;
use tokio::{sync::oneshot, time::sleep};
use tokio_supervisor::RestartPolicy;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::Instrument;

use crate::{
    RuntimeHandle,
    actor::{
        binding::{
            ActorStats, BindingCore, BindingGuard, BindingLifecycle, MailboxMode, MailboxRef,
            mailbox,
        },
        builder::{ActorOptions, DEFAULT_MAILBOX_CAPACITY},
        context::{ActorContext, ActorRef, ActorSteps, ActorTimers},
        factory::ActorFactory,
        monitor::{DownReason, MonitorExitGuard},
        observability::{ActorExitStatus, GraphObservability, anonymous_graph_name},
        raw::{BoxError, RawActor},
    },
};

pub(crate) type BoxedActorFuture =
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;

pub(crate) struct RunnerStart {
    pub(crate) shutdown: CancellationToken,
    pub(crate) mailbox_capacity: usize,
    pub(crate) observability: GraphObservability,
    pub(crate) restart_policy: RestartPolicy,
    pub(crate) ready: oneshot::Sender<()>,
    pub(crate) supervisor: RuntimeHandle,
    pub(crate) children: Option<RuntimeHandle>,
}

/// Type-erased actor runner.
///
/// This is the only dyn layer in the crate: each implementation knows its own
/// message type and owns the typed binding core, so starting an actor binds a
/// typed mailbox without any downcast.
pub(crate) trait ErasedRunner: Send + Sync {
    fn start(&self, start: RunnerStart) -> BoxedActorFuture;
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
            let timers = ActorTimers::new(&actor_shutdown);
            let monitors = binding.outbound_monitors();
            let observability = start.observability;
            let (sender, mailbox) = mailbox(&mailbox_mode, start.mailbox_capacity);
            let actor_id = binding.actor_id().clone();
            let steps = ActorSteps::new();
            let incarnation = MailboxRef::new(actor_id.clone(), sender, steps.gauge());
            let bound_mailbox = BindingGuard::bind(
                binding.clone(),
                incarnation.clone(),
                observability.clone(),
                start.restart_policy,
            );
            let myself = ActorRef::from_core(&binding, Some(actor_id.clone()));
            let monitor_hub = binding.monitor_hub();
            let mut ctx = ActorContext {
                id: actor_id,
                mailbox,
                myself,
                incarnation,
                shutdown: actor_shutdown,
                observability,
                timers,
                state_timeout: None,
                monitors,
                ready: Some(start.ready),
                continuations: Default::default(),
                steps,
                supervisor: start.supervisor,
                children: start.children,
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
            result.map(|_| ())
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
}

/// The shutdown bound a standalone host should pass to
/// [`RunnableActor::run_until`] when it has no deadline of its own.
///
/// This matches the default grace of
/// [`ShutdownPolicy`](crate::ShutdownPolicy), so an actor behaves the same
/// whether it is hosted by hand or by [`Runtime`](crate::Runtime).
pub const DEFAULT_SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// An actor graph containing wiring and independently runnable actors.
///
/// Stable typed refs remain functional across independent actor restarts.
/// Execution is performed by driving the actors returned by [`actors`](Self::actors),
/// normally as separate supervisor children.
#[derive(Clone)]
pub struct Graph {
    inner: Arc<GraphInner>,
}

struct GraphInner {
    name: Arc<str>,
    actors: Vec<RunnableActor>,
    observability: GraphObservability,
    mailbox_capacity: usize,
}

impl Graph {
    pub(crate) fn new(
        name: Arc<str>,
        actors: Vec<RunnableActor>,
        observability: GraphObservability,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            inner: Arc::new(GraphInner {
                name,
                actors,
                observability,
                mailbox_capacity,
            }),
        }
    }

    /// Returns the graph name used in tracing fields.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns all runnable actors in graph declaration order.
    pub fn actors(&self) -> &[RunnableActor] {
        &self.inner.actors
    }

    /// Returns point-in-time stats for every actor in graph declaration order.
    pub fn stats(&self) -> Vec<ActorStats> {
        self.inner.actors.iter().map(RunnableActor::stats).collect()
    }

    pub(crate) fn dynamic_factory(&self) -> RunnableActorFactory {
        RunnableActorFactory {
            observability: self.inner.observability.clone(),
            mailbox_capacity: self.inner.mailbox_capacity,
        }
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("name", &self.name())
            .finish_non_exhaustive()
    }
}

/// A single actor in a graph, ready to be run independently.
///
/// Retains stable mailbox bindings from the graph, so [`ActorRef`] handles
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
    observability: GraphObservability,
    running: AtomicBool,
}

pub(crate) struct RunnableActorParts {
    pub(crate) actor_id: Arc<str>,
    pub(crate) binding_lifecycle: Arc<dyn BindingLifecycle>,
    pub(crate) runner: Arc<dyn ErasedRunner>,
    pub(crate) mailbox_capacity: usize,
    pub(crate) observability: GraphObservability,
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

    /// Returns a point-in-time snapshot of this actor's stats.
    pub fn stats(&self) -> ActorStats {
        self.inner.binding_lifecycle.stats()
    }

    /// Marks the actor's binding terminated.
    ///
    /// Call this when no further run will be started so senders fail fast with
    /// `ActorTerminated` instead of waiting for a rebind that will never come.
    pub fn terminate_binding(&self) {
        self.apply_run_disposition(RunDisposition::Terminate);
    }

    /// Runs this actor with a fresh mailbox until shutdown resolves.
    ///
    /// `restart` controls the binding disposition after the run, while
    /// `shutdown_bound` limits the whole drain and stop-hook path for this
    /// standalone host. Supervised runtimes use each child specification's
    /// [`ShutdownPolicy`](crate::ShutdownPolicy) grace instead.
    /// [`DEFAULT_SHUTDOWN_BOUND`] is a reasonable bound for a host without a
    /// deadline of its own.
    ///
    /// When `shutdown_bound` expires before the actor finishes draining, the
    /// inner task is aborted and the run resolves to
    /// [`ActorRunError::ShutdownTimedOut`] — a timeout is reported rather than
    /// laundered into a clean exit, so `.expect(..)` on the result will panic
    /// for an actor that overruns its bound.
    ///
    /// - A clean exit leaves the binding rebindable only for
    ///   [`Always`](RestartPolicy::Always).
    /// - Failure, panic, or unexpected task cancellation leaves it rebindable
    ///   for [`Always`](RestartPolicy::Always) and
    ///   [`OnFailure`](RestartPolicy::OnFailure).
    /// - Requested shutdown terminates the binding for every policy.
    /// - Dropping the `run_until` future aborts the incarnation and leaves the
    ///   binding rebindable for `Always` and `OnFailure`; `Never` terminates it.
    ///
    /// [`RestartPolicy::default()`] is `OnFailure`, so
    /// `run_until(shutdown, Default::default(), shutdown_bound)` leaves a
    /// failed run rebindable.
    /// Callers migrating code that expected a default policy to terminate
    /// after every exit must pass [`RestartPolicy::Never`] explicitly.
    ///
    /// A hand-written host must call
    /// [`terminate_binding`](Self::terminate_binding) when it gives up after a
    /// policy that left the binding waiting to rebind.
    ///
    /// Actors run through this unsupervised entry point receive a terminal
    /// [`RuntimeHandle`] from [`ActorContext::supervisor`](crate::ActorContext::supervisor):
    /// control operations return `ControlError::Unavailable` and observation
    /// streams are closed. Their [`ActorContext::children`](crate::ActorContext::children)
    /// value is `None`.
    pub async fn run_until<F>(
        &self,
        shutdown: F,
        restart: RestartPolicy,
        shutdown_bound: Duration,
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
            sleep(shutdown_bound).await;
        };
        self.run_until_ready(
            bounded_shutdown,
            abort,
            restart,
            RuntimeHandle::unavailable(),
            None,
            || {},
        )
        .await
    }

    pub(crate) async fn run_until_ready<F, A, R>(
        &self,
        shutdown: F,
        abort: A,
        restart: RestartPolicy,
        supervisor: RuntimeHandle,
        children: Option<RuntimeHandle>,
        ready: R,
    ) -> Result<(), ActorRunError>
    where
        F: Future<Output = ()>,
        A: Future<Output = ()>,
        R: FnOnce(),
    {
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
                    ready: ready_tx,
                    supervisor,
                    children,
                })
                .instrument(actor_span),
        ));
        let _cancel_actor_on_drop = actor_shutdown.clone().drop_guard();

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
    policy: RestartPolicy,
    shutdown_requested: bool,
    status: ActorExitStatus,
) -> RunDisposition {
    if shutdown_requested || status == ActorExitStatus::Shutdown {
        return RunDisposition::Terminate;
    }

    match (policy, status) {
        (RestartPolicy::Always, ActorExitStatus::Stopped) => RunDisposition::ExpectRebind,
        (RestartPolicy::Always | RestartPolicy::OnFailure, ActorExitStatus::Failed)
        | (RestartPolicy::Always | RestartPolicy::OnFailure, ActorExitStatus::Panicked)
        | (RestartPolicy::Always | RestartPolicy::OnFailure, ActorExitStatus::Cancelled)
        | (RestartPolicy::Always | RestartPolicy::OnFailure, ActorExitStatus::ShutdownTimedOut) => {
            RunDisposition::ExpectRebind
        }
        (RestartPolicy::Never, _)
        | (RestartPolicy::OnFailure, ActorExitStatus::Stopped)
        | (_, ActorExitStatus::Shutdown) => RunDisposition::Terminate,
        // `RestartPolicy` is intentionally extensible. A future policy must
        // opt in to more precise binding behavior when this crate adopts it.
        _ => {
            tracing::warn!(
                "unknown restart policy; defaulting actor rebind behavior to on-failure"
            );
            if status == ActorExitStatus::Stopped {
                RunDisposition::Terminate
            } else {
                RunDisposition::ExpectRebind
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_disposition_matches_documented_restart_semantics() {
        assert_eq!(
            run_disposition(RestartPolicy::Always, false, ActorExitStatus::Stopped),
            RunDisposition::ExpectRebind
        );
        for policy in [RestartPolicy::OnFailure, RestartPolicy::Never] {
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
            for policy in [RestartPolicy::Always, RestartPolicy::OnFailure] {
                assert_eq!(
                    run_disposition(policy, false, status),
                    RunDisposition::ExpectRebind
                );
            }
            assert_eq!(
                run_disposition(RestartPolicy::Never, false, status),
                RunDisposition::Terminate
            );
        }

        for policy in [
            RestartPolicy::Always,
            RestartPolicy::OnFailure,
            RestartPolicy::Never,
        ] {
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

/// Registration surface for constructing runtime-added actors that share
/// execution settings.
///
/// Its [`actor`](Self::actor) methods accept an [`ActorFactory`], which is the
/// reusable recipe that constructs each actor incarnation.
#[derive(Clone, Debug)]
pub struct RunnableActorFactory {
    observability: GraphObservability,
    mailbox_capacity: usize,
}

impl RunnableActorFactory {
    /// Creates a factory with the same defaults [`GraphBuilder`](crate::GraphBuilder)
    /// uses for a graph without an explicit name.
    pub fn new() -> Self {
        Self {
            observability: GraphObservability::new(anonymous_graph_name()),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
        }
    }

    /// Constructs a runnable actor from a reusable incarnation factory and
    /// returns its stable typed ref.
    ///
    /// See [`ActorFactory`] for the incarnation lifecycle contract.
    pub fn actor<F>(
        &self,
        label: impl Into<String>,
        factory: F,
    ) -> (RunnableActor, ActorRef<<F::Actor as RawActor>::Msg>)
    where
        F: ActorFactory,
    {
        self.actor_with_options(label, factory, ActorOptions::new())
    }

    /// Constructs a runnable actor from a reusable incarnation factory with
    /// explicit per-actor options and returns its stable typed ref.
    ///
    /// See [`ActorFactory`] for the incarnation lifecycle contract.
    pub fn actor_with_options<F>(
        &self,
        label: impl Into<String>,
        factory: F,
        options: ActorOptions<<F::Actor as RawActor>::Msg>,
    ) -> (RunnableActor, ActorRef<<F::Actor as RawActor>::Msg>)
    where
        F: ActorFactory,
    {
        let actor_id: Arc<str> = label.into().into();
        let binding = Arc::new(match options.size_hint {
            Some(size_hint) => BindingCore::<<F::Actor as RawActor>::Msg>::with_message_size(
                actor_id.clone(),
                size_hint,
            ),
            None => BindingCore::<<F::Actor as RawActor>::Msg>::new(actor_id.clone()),
        });
        self.actor_with_binding(actor_id, factory, binding, options.mailbox_mode)
    }

    fn actor_with_binding<F>(
        &self,
        actor_id: Arc<str>,
        factory: F,
        binding: Arc<BindingCore<<F::Actor as RawActor>::Msg>>,
        mailbox_mode: MailboxMode<<F::Actor as RawActor>::Msg>,
    ) -> (RunnableActor, ActorRef<<F::Actor as RawActor>::Msg>)
    where
        F: ActorFactory,
    {
        let actor_ref = ActorRef::from_core(&binding, None);
        let runnable = RunnableActor::new(RunnableActorParts {
            actor_id,
            binding_lifecycle: binding.clone(),
            runner: Arc::new(TypedRunner {
                factory: Arc::new(factory),
                binding,
                mailbox_mode,
            }),
            mailbox_capacity: self.mailbox_capacity,
            observability: self.observability.clone(),
        });
        (runnable, actor_ref)
    }
}

impl Default for RunnableActorFactory {
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
