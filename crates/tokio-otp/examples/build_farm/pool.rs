//! A leader actor that owns and resizes a dynamic worker scope.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio_otp::{
    Actor, ActorRef, ActorResult, BoxError, DynamicActorOptions, LiveContext, MessageContext,
    RestartIntensity, RestartPolicy, StartContext, StopContext, prelude::Continue,
};

use crate::{
    messages::{
        CALL_DEADLINE, CONTROL_DEADLINE, DispatchOutcome, ExecOutcome, Phase, PoolMsg, PoolReport,
        ProgressMsg, SchedulerMsg, WorkerMsg,
    },
    model::{Action, Digest},
    shared::BuildJournal,
    worker::WorkerFactory,
};

pub const POOL_NODE_ID: &str = "build-pool";
pub const POOL_LEADER_ID: &str = "pool";

struct WorkerSlot {
    actor: ActorRef<WorkerMsg>,
    busy: bool,
}

pub struct Pool {
    scheduler: ActorRef<SchedulerMsg>,
    progress: ActorRef<ProgressMsg>,
    worker_factory: WorkerFactory,
    journal: Arc<BuildJournal>,
    labels: Arc<AtomicU64>,
    workers: BTreeMap<String, WorkerSlot>,
    queue: VecDeque<(Action, Digest)>,
    pending_adds: usize,
    report: PoolReport,
}

impl Pool {
    pub fn new(
        scheduler: ActorRef<SchedulerMsg>,
        progress: ActorRef<ProgressMsg>,
        worker_factory: WorkerFactory,
        journal: Arc<BuildJournal>,
        labels: Arc<AtomicU64>,
    ) -> Self {
        Self {
            scheduler,
            progress,
            worker_factory,
            journal,
            labels,
            workers: BTreeMap::new(),
            queue: VecDeque::new(),
            pending_adds: 0,
            report: PoolReport::default(),
        }
    }
}

impl Actor for Pool {
    type Msg = PoolMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        // The owned scope starts after its leader, so defer the first mutation
        // until the ordinary message loop is live.
        ctx.continue_with(PoolMsg::Pump);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self::Msg>,
    ) -> ActorResult {
        match message {
            PoolMsg::Submit { action, digest } => {
                let _ = self
                    .progress
                    .send(ProgressMsg::Update {
                        target: action.target,
                        phase: Phase::Queued,
                    })
                    .await;
                self.queue.push_back((action, digest));
            }
            PoolMsg::Pump | PoolMsg::WorkerRemoved | PoolMsg::Forwarded => {}
            PoolMsg::WorkerAdded { label, actor } => {
                self.pending_adds = self.pending_adds.saturating_sub(1);
                if let Some(actor) = actor {
                    self.workers
                        .insert(label, WorkerSlot { actor, busy: false });
                    self.report.added_workers += 1;
                    self.report.peak_workers = self.report.peak_workers.max(self.workers.len());
                }
            }
            PoolMsg::DispatchFinished {
                label,
                action,
                digest,
                outcome,
            } => self.finish_dispatch(label, action, digest, outcome, ctx),
            PoolMsg::Report { reply } => {
                reply.send(self.snapshot());
                return Ok(Continue);
            }
        }

        self.reconcile(ctx);
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_pool(self.snapshot());
        Ok(())
    }
}

impl Pool {
    fn snapshot(&self) -> PoolReport {
        PoolReport {
            live_workers: self.workers.len(),
            ..self.report
        }
    }

    fn reconcile(&mut self, ctx: &MessageContext<'_, PoolMsg>) {
        // A draining actor keeps handling offload completions. Starting a new
        // control offload for every completion would make the drain perpetual.
        if ctx.is_shutting_down() {
            return;
        }

        let busy = self.workers.values().filter(|worker| worker.busy).count();
        let desired = (self.queue.len() + busy).clamp(1, 3);
        while self.workers.len() + self.pending_adds < desired {
            self.add_worker(ctx);
        }

        while let Some(label) = self
            .workers
            .iter()
            .find(|(_, worker)| !worker.busy)
            .map(|(label, _)| label.clone())
        {
            let Some((action, digest)) = self.queue.pop_front() else {
                break;
            };
            let actor = {
                let worker = self
                    .workers
                    .get_mut(&label)
                    .expect("worker came from roster");
                worker.busy = true;
                worker.actor.clone()
            };
            self.dispatch(label, actor, action, digest, ctx);
        }

        while self.workers.len() > desired {
            let Some(label) = self
                .workers
                .iter()
                .find(|(_, worker)| !worker.busy)
                .map(|(label, _)| label.clone())
            else {
                break;
            };
            self.remove_worker(label, ctx);
        }
    }

    fn add_worker(&mut self, ctx: &MessageContext<'_, PoolMsg>) {
        let Some(children) = ctx.children() else {
            return;
        };
        let label = format!("worker-{}", self.labels.fetch_add(1, Ordering::Relaxed));
        let requested = label.clone();
        let factory = self.worker_factory.clone();
        self.pending_adds += 1;
        ctx.offload(
            CONTROL_DEADLINE,
            async move {
                children
                    .add_actor(
                        requested,
                        factory,
                        DynamicActorOptions::new()
                            .restart(RestartPolicy::Always)
                            .restart_intensity(RestartIntensity::new(5, Duration::from_secs(10))),
                    )
                    .await
                    .ok()
            },
            move |result| PoolMsg::WorkerAdded {
                label,
                actor: result.ok().flatten(),
            },
        );
    }

    fn dispatch(
        &mut self,
        label: String,
        worker: ActorRef<WorkerMsg>,
        action: Action,
        digest: Digest,
        ctx: &MessageContext<'_, PoolMsg>,
    ) {
        let request = action.clone();
        let completed_by = label.clone();
        self.report.dispatches += 1;
        ctx.offload(
            CALL_DEADLINE,
            async move {
                worker
                    .call(CALL_DEADLINE, |reply| WorkerMsg::Execute {
                        action: request,
                        digest,
                        reply,
                    })
                    .await
            },
            move |result| PoolMsg::DispatchFinished {
                label: completed_by,
                action,
                digest,
                outcome: match result {
                    Ok(Ok(outcome)) => DispatchOutcome::Finished(outcome),
                    Ok(Err(_)) | Err(_) => DispatchOutcome::Lost,
                },
            },
        );
    }

    fn finish_dispatch(
        &mut self,
        label: String,
        action: Action,
        digest: Digest,
        outcome: DispatchOutcome,
        ctx: &MessageContext<'_, PoolMsg>,
    ) {
        if let Some(worker) = self.workers.get_mut(&label) {
            worker.busy = false;
        }
        match outcome {
            DispatchOutcome::Finished(outcome) => self.forward(action.target, outcome, ctx),
            DispatchOutcome::Lost => {
                self.report.lost_dispatches += 1;
                self.queue.push_front((action, digest));
            }
        }
    }

    fn forward(
        &self,
        target: &'static str,
        outcome: ExecOutcome,
        ctx: &MessageContext<'_, PoolMsg>,
    ) {
        let scheduler = self.scheduler.clone();
        ctx.offload(
            CONTROL_DEADLINE,
            async move {
                let _ = scheduler
                    .send(SchedulerMsg::Finished { target, outcome })
                    .await;
            },
            |_| PoolMsg::Forwarded,
        );
    }

    fn remove_worker(&mut self, label: String, ctx: &MessageContext<'_, PoolMsg>) {
        self.workers.remove(&label);
        self.report.removed_workers += 1;
        let Some(children) = ctx.children() else {
            return;
        };
        ctx.offload(
            CONTROL_DEADLINE,
            async move {
                let _ = children.remove_child(label).await;
            },
            |_| PoolMsg::WorkerRemoved,
        );
    }
}
