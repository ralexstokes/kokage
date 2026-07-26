//! The build scheduler: the child whose completion ends the run.
//!
//! Every other actor in the farm is a service with no natural end. The
//! scheduler is not: when the last target reaches a terminal state it returns
//! [`Flow::Stop`](tokio_otp::prelude::Stop), which is a clean exit, which makes
//! it *completed* in the supervisor's sense. `main` arms
//! [`RuntimeHandle::shutdown_on_completion`](tokio_otp::RuntimeHandle::shutdown_on_completion)
//! on that one child before spawning, and the whole farm follows it down.
//!
//! Two consequences shape this module:
//!
//! * The scheduler must be OTP's *transient*, spelled
//!   [`RestartPolicy::OnFailure`](tokio_otp::RestartPolicy::OnFailure) here.
//!   Under `Always` a clean stop is just a restart, and the build would loop
//!   forever; under `Never` a genuine crash would strand the run.
//! * Nobody can ask the scheduler anything after it completes, because the
//!   completion is what tears the runtime down. The final state is therefore
//!   *pushed* to the journal from `on_stop` rather than pulled by the caller.

use std::{sync::Arc, time::Duration};

use tokio_otp::{
    Actor, ActorRef, ActorResult, BoxError, LiveContext, MessageContext, StartContext,
    StateTimeoutSlot, StopContext,
    prelude::{Continue, Stop},
};

use crate::{
    lease::Lease,
    messages::{
        BuildStatus, ExecOutcome, Phase, PoolMsg, ProgressMsg, SchedulerMsg, TargetProgress,
        TargetState,
    },
    plan::{BuildPlan, Digest, TargetId, digest},
    shared::BuildJournal,
};

/// How often the scheduler re-examines the frontier on its own.
///
/// Completions are not a reliable edge: the pool forwards them through a
/// bounded [`offload`](tokio_otp::LiveContext::offload) that can time out,
/// and a stale lease makes a dispatch pass produce nothing. A re-armed sweep means
/// neither can wedge the build.
const FRONTIER_SWEEP: Duration = Duration::from_millis(20);

/// Walks the build graph and keeps the pool fed.
pub struct Scheduler {
    plan: Arc<BuildPlan>,
    pool: ActorRef<PoolMsg>,
    progress: ActorRef<ProgressMsg>,
    lease: Arc<Lease>,
    journal: Arc<BuildJournal>,
    status: BuildStatus,
    /// The single outstanding frontier sweep.
    ///
    /// One slot rather than a bare timer handle: re-arming has to retract the
    /// sweep it replaces, including one that already reached the mailbox.
    sweep: StateTimeoutSlot,
}

impl Scheduler {
    /// Creates a scheduler for one build of `plan`.
    pub fn new(
        plan: Arc<BuildPlan>,
        pool: ActorRef<PoolMsg>,
        progress: ActorRef<ProgressMsg>,
        lease: Arc<Lease>,
        journal: Arc<BuildJournal>,
    ) -> Self {
        Self {
            plan,
            pool,
            progress,
            lease,
            journal,
            status: BuildStatus::default(),
            sweep: StateTimeoutSlot::new(),
        }
    }
}

impl Actor for Scheduler {
    type Msg = SchedulerMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        for action in self.plan.actions() {
            self.status
                .states
                .insert(action.target, TargetState::Blocked);
        }
        // Walking the graph is warm-up work, not initialization: readiness is
        // reported first and this runs before any mailbox traffic.
        ctx.continue_with(SchedulerMsg::Dispatch);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            SchedulerMsg::Dispatch => return self.dispatch(ctx).await,
            SchedulerMsg::Finished { target, outcome } => {
                let state = match outcome {
                    ExecOutcome::Built(artifact) => TargetState::Built {
                        digest: artifact.digest,
                        cached: false,
                    },
                    ExecOutcome::Cached(artifact) => TargetState::Built {
                        digest: artifact.digest,
                        cached: true,
                    },
                    ExecOutcome::Quarantined { attempts } => TargetState::Failed { attempts },
                };
                self.status.states.insert(target, state);
                ctx.continue_with(SchedulerMsg::Dispatch);
            }
            SchedulerMsg::Status { reply } => reply.send(self.status.clone()),
        }
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_summary(self.status.clone());
        Ok(())
    }
}

impl Scheduler {
    async fn dispatch(&mut self, ctx: &impl LiveContext<SchedulerMsg>) -> ActorResult {
        // The lease is the farm's right to be doing this work at all. It is
        // held by a plain `ChildSpec` sibling with no mailbox, so the only way
        // to consult it is the shared `Arc` — and the only way to wait for it
        // is to re-arm a timer.
        if !self.lease.is_held() {
            self.status.lease_stalls += 1;
            self.arm_sweep(ctx);
            return Ok(Continue);
        }

        self.propagate_skips().await;

        for action in self.plan.actions() {
            if self.status.states.get(action.target) != Some(&TargetState::Blocked) {
                continue;
            }
            let Some(dep_digests) = self.resolve_deps(action.deps) else {
                continue;
            };
            let digest = digest(action, dep_digests);
            self.status
                .states
                .insert(action.target, TargetState::Running);
            self.status.submitted += 1;
            // Awaited on purpose: this is the one direction of the
            // scheduler/pool cycle that applies backpressure. The pool never
            // awaits a send back, so a full queue slows the scheduler down
            // instead of deadlocking the pair.
            self.pool
                .send(PoolMsg::Submit {
                    action: action.clone(),
                    digest,
                })
                .await?;
        }

        if self.status.states.values().all(TargetState::is_terminal) {
            self.status.finished = true;
            // A clean stop is what `shutdown_on_completion` is watching for.
            return Ok(Stop);
        }
        self.arm_sweep(ctx);
        Ok(Continue)
    }

    /// Re-arms the single outstanding frontier sweep.
    ///
    /// A [`StateTimeoutSlot`] over `send_after_retractable` rather than a bare
    /// `send_after`: arming replaces the pending sweep instead of stacking
    /// another one, and it retracts a sweep the mailbox already accepted, so a
    /// build that walks its frontier twenty times still has exactly one sweep
    /// in flight.
    fn arm_sweep(&mut self, ctx: &impl LiveContext<SchedulerMsg>) {
        self.sweep
            .set(ctx.send_after_retractable(SchedulerMsg::Dispatch, FRONTIER_SWEEP));
    }

    /// Returns each dependency's content address, or `None` if any is still
    /// unresolved.
    fn resolve_deps(&self, deps: &[TargetId]) -> Option<Vec<(TargetId, Digest)>> {
        deps.iter()
            .map(|dep| match self.status.states.get(dep) {
                Some(TargetState::Built { digest, .. }) => Some((*dep, *digest)),
                _ => None,
            })
            .collect()
    }

    /// Marks everything downstream of a failure as skipped, to a fixpoint.
    async fn propagate_skips(&mut self) {
        loop {
            let doomed: Vec<TargetId> = self
                .plan
                .actions()
                .iter()
                .filter(|action| {
                    self.status.states.get(action.target) == Some(&TargetState::Blocked)
                        && action.deps.iter().any(|dep| {
                            matches!(
                                self.status.states.get(dep),
                                Some(TargetState::Failed { .. } | TargetState::Skipped)
                            )
                        })
                })
                .map(|action| action.target)
                .collect();
            if doomed.is_empty() {
                return;
            }
            for target in doomed {
                self.status.states.insert(target, TargetState::Skipped);
                let _ = self
                    .progress
                    .send(ProgressMsg::Update(TargetProgress {
                        target,
                        phase: Phase::Skipped,
                    }))
                    .await;
            }
        }
    }
}
