//! Dependency-frontier scheduler whose clean completion ends the runtime.

use std::{sync::Arc, time::Duration};

use tokio_otp::{
    Actor, ActorRef, ActorResult, BoxError, LiveContext, MessageContext, StartContext, StopContext,
    prelude::{Continue, Stop},
};

use crate::{
    lease::Lease,
    messages::{BuildStatus, ExecOutcome, PoolMsg, SchedulerMsg, TargetState},
    model::{Action, BuildPlan, Digest, TargetId, digest},
    shared::BuildJournal,
};

pub const SCHEDULER_ID: &str = "scheduler";

pub struct Scheduler {
    plan: Arc<BuildPlan>,
    pool: ActorRef<PoolMsg>,
    lease: Arc<Lease>,
    journal: Arc<BuildJournal>,
    status: BuildStatus,
}

impl Scheduler {
    pub fn new(
        plan: Arc<BuildPlan>,
        pool: ActorRef<PoolMsg>,
        lease: Arc<Lease>,
        journal: Arc<BuildJournal>,
    ) -> Self {
        let targets = plan
            .actions()
            .iter()
            .map(|action| (action.target, TargetState::Blocked))
            .collect();
        Self {
            plan,
            pool,
            lease,
            journal,
            status: BuildStatus {
                targets,
                ..BuildStatus::default()
            },
        }
    }
}

impl Actor for Scheduler {
    type Msg = SchedulerMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        ctx.continue_with(SchedulerMsg::Schedule);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            SchedulerMsg::Schedule => self.schedule(ctx).await,
            SchedulerMsg::Finished { target, outcome } => {
                let (digest, cached) = match outcome {
                    ExecOutcome::Built(artifact) => (artifact.digest, false),
                    ExecOutcome::Cached(artifact) => (artifact.digest, true),
                };
                self.status
                    .targets
                    .insert(target, TargetState::Built { digest, cached });
                ctx.continue_with(SchedulerMsg::Schedule);
                Ok(Continue)
            }
            SchedulerMsg::Snapshot { reply } => {
                reply.send(self.status.clone());
                Ok(Continue)
            }
        }
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_status(self.status.clone());
        Ok(())
    }
}

impl Scheduler {
    async fn schedule(&mut self, ctx: &MessageContext<'_, SchedulerMsg>) -> ActorResult {
        if !self.lease.is_held() {
            self.status.lease_stalls += 1;
            ctx.send_after(SchedulerMsg::Schedule, Duration::from_millis(10));
            return Ok(Continue);
        }

        let ready: Vec<(Action, Digest)> = self
            .plan
            .actions()
            .iter()
            .filter(|action| self.status.targets.get(action.target) == Some(&TargetState::Blocked))
            .filter_map(|action| {
                self.resolved_dependencies(action)
                    .map(|dependencies| (action.clone(), digest(action, dependencies)))
            })
            .collect();

        for (action, digest) in ready {
            self.status
                .targets
                .insert(action.target, TargetState::Running);
            self.status.submissions += 1;
            self.pool.send(PoolMsg::Submit { action, digest }).await?;
        }

        if self
            .status
            .targets
            .values()
            .all(|state| matches!(state, TargetState::Built { .. }))
        {
            self.status.complete = true;
            return Ok(Stop);
        }
        Ok(Continue)
    }

    fn resolved_dependencies(&self, action: &Action) -> Option<Vec<(TargetId, Digest)>> {
        action
            .dependencies
            .iter()
            .map(|target| match self.status.targets.get(target) {
                Some(TargetState::Built { digest, .. }) => Some((*target, *digest)),
                _ => None,
            })
            .collect()
    }
}
