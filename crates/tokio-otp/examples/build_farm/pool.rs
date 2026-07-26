//! The worker pool leader.
//!
//! This actor is the leader of a
//! [`SupervisionTree::ActorWithScope`](tokio_otp::SupervisionTree::ActorWithScope)
//! node, so [`MessageContext::children`] hands it a
//! [`RuntimeHandle`](tokio_otp::RuntimeHandle) for a dynamic scope it owns.
//! Scaling the pool is therefore ordinary supervisor membership: `add_actor` to
//! grow, `remove_child` to shrink or to retire a wedged worker.
//!
//! # Why everything here is pipelined
//!
//! The leader must never await anything on its own handle loop:
//!
//! * `children().add_actor(..)` and `children().remove_child(..)` are control
//!   operations on the scope this actor leads. `remove_child` waits for the
//!   removed worker's cooperative shutdown, which can take the worker's whole
//!   grace period — that is the exact window in which the pool has to keep
//!   accepting completions from its other workers.
//! * `worker.call(..)` is a full compile round-trip. Awaiting it inline would
//!   turn a four-worker pool into a one-worker pool.
//! * Even `scheduler.send(..)` is pipelined, because the scheduler sends
//!   `Submit` to this pool: two actors awaiting each other's bounded mailboxes
//!   is the cycle hazard [`GraphBuilder`](tokio_otp::GraphBuilder) warns about,
//!   and pipelining one direction breaks it.
//!
//! [`LiveContext::offload`] is the tool for all of it. Note the shape it forces:
//! a step always posts a message back, so a fire-and-forget effect still needs
//! somewhere to land — hence [`PoolMsg::Noted`].

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio_otp::{
    Actor, ActorRef, ActorResult, BoxError, CancellationHandle, DynamicActorOptions, LiveContext,
    MessageContext, MonitorEvent, RestartIntensity, RestartPolicy, StartContext, StopContext,
    prelude::Continue,
};

use crate::{
    messages::{
        CONTROL_DEADLINE, DISPATCH_DEADLINE, DispatchOutcome, ExecOutcome, Phase, PoolMsg,
        PoolReport, ProgressMsg, SchedulerMsg, TargetProgress, WorkerLifecycle, WorkerMsg,
    },
    plan::{Action, Digest, TargetId},
    shared::BuildJournal,
    worker::WorkerFactory,
};

/// Pool sizing bounds.
#[derive(Clone, Copy, Debug)]
pub struct PoolLimits {
    /// Workers kept alive even when the queue is empty.
    pub min: usize,
    /// Hard ceiling on concurrent workers.
    pub max: usize,
}

struct WorkerSlot {
    actor: ActorRef<WorkerMsg>,
    busy: Option<TargetId>,
}

/// Owns the dynamic worker scope and hands actions to idle workers.
pub struct PoolManager {
    scheduler: ActorRef<SchedulerMsg>,
    progress: ActorRef<ProgressMsg>,
    worker_factory: WorkerFactory,
    limits: PoolLimits,
    journal: Arc<BuildJournal>,
    /// Worker-label allocator.
    ///
    /// Durable on purpose: a restarted leader rebuilds an empty roster, and
    /// reusing `worker-0` while the previous `worker-0` is still detaching
    /// would collide on the child id.
    labels: Arc<AtomicU64>,

    workers: BTreeMap<String, WorkerSlot>,
    watches: BTreeMap<String, CancellationHandle>,
    queue: VecDeque<(Action, Digest)>,
    pending_adds: usize,
    started: BTreeSet<String>,
    report: PoolReport,
}

impl PoolManager {
    /// Creates a leader for a pool bounded by `limits`.
    pub fn new(
        scheduler: ActorRef<SchedulerMsg>,
        progress: ActorRef<ProgressMsg>,
        worker_factory: WorkerFactory,
        limits: PoolLimits,
        journal: Arc<BuildJournal>,
        labels: Arc<AtomicU64>,
    ) -> Self {
        Self {
            scheduler,
            progress,
            worker_factory,
            limits,
            journal,
            labels,
            workers: BTreeMap::new(),
            watches: BTreeMap::new(),
            queue: VecDeque::new(),
            pending_adds: 0,
            started: BTreeSet::new(),
            report: PoolReport::default(),
        }
    }
}

impl Actor for PoolManager {
    type Msg = PoolMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self::Msg>) -> ActorResult {
        // The owned scope starts only after this hook returns, so the first
        // scale-up cannot happen here. Queue it as a continuation: it runs
        // before ordinary mailbox traffic, after readiness is reported.
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
                    .send(ProgressMsg::Update(TargetProgress {
                        target: action.target,
                        phase: Phase::Queued,
                    }))
                    .await;
                self.queue.push_back((action, digest));
            }
            PoolMsg::Pump => {}
            PoolMsg::Dispatched {
                worker,
                action,
                digest,
                outcome,
            } => self.dispatched(worker, action, digest, outcome, ctx),
            PoolMsg::WorkerAdded { label, actor } => {
                self.pending_adds -= 1;
                if let Some(actor) = actor {
                    self.watch_worker(&label, &actor, ctx);
                    self.workers.insert(label, WorkerSlot { actor, busy: None });
                    self.report.peak_workers = self.report.peak_workers.max(self.workers.len());
                }
            }
            PoolMsg::WorkerRemoved { label } => {
                tracing::debug!(worker = %label, "worker detached from the owned scope");
            }
            PoolMsg::Noted => {}
            PoolMsg::WorkerLifecycle { label, event } => match event {
                // A second `Up` for a label the roster already saw start is a
                // restart. The count is best effort: watch events arrive as
                // ordinary mailbox traffic, so one racing the drain's intake
                // close is simply refused.
                WorkerLifecycle::Up => {
                    if !self.started.insert(label) {
                        self.report.worker_restarts += 1;
                    }
                }
                WorkerLifecycle::Down => {}
                // Restart intensity exhausted: the ref will never rebind, so
                // drop the slot. Any dispatch it was holding resolves through
                // `Dispatched` as `Lost` and is requeued there — requeueing
                // here as well would run the action twice.
                WorkerLifecycle::Terminated => self.forget(&label),
            },
            PoolMsg::Report { reply } => {
                reply.send(self.snapshot());
                return Ok(Continue);
            }
        }

        self.pump(ctx);
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self::Msg>) -> Result<(), BoxError> {
        self.journal.record_pool(self.snapshot());
        Ok(())
    }
}

impl PoolManager {
    fn snapshot(&self) -> PoolReport {
        PoolReport {
            workers: self.workers.len(),
            queued: self.queue.len(),
            ..self.report.clone()
        }
    }

    /// Reconciles roster size against demand, then fills idle workers.
    fn pump(&mut self, ctx: &MessageContext<'_, PoolMsg>) {
        // This guard is load-bearing, not defensive. Under `DrainPolicy::Drain`
        // the framework loop keeps calling `handle` for queued messages *and*
        // for offload completions until both are exhausted. Reconciling here
        // would answer each completion by starting another `add_actor` offload,
        // whose completion is another message, and the drain never converges —
        // it would burn the whole shutdown grace and end in
        // `ExitStatusView::ShutdownTimedOut`, skipping `on_stop` entirely.
        // Draining means finishing what is in hand, not staying open for
        // business.
        if ctx.is_shutting_down() {
            return;
        }

        let busy = self
            .workers
            .values()
            .filter(|slot| slot.busy.is_some())
            .count();
        let demand = self.queue.len() + busy;
        let desired = demand.clamp(self.limits.min, self.limits.max);

        if self.workers.len() + self.pending_adds < desired {
            self.scale_up(ctx);
        }

        while !self.queue.is_empty() {
            let Some(label) = self
                .workers
                .iter()
                .find(|(_, slot)| slot.busy.is_none())
                .map(|(label, _)| label.clone())
            else {
                break;
            };
            let (action, digest) = self.queue.pop_front().expect("queue is non-empty");
            self.dispatch(label, action, digest, ctx);
        }

        if self.queue.is_empty() {
            self.scale_down(ctx);
        }
    }

    fn scale_up(&mut self, ctx: &MessageContext<'_, PoolMsg>) {
        let Some(children) = ctx.children() else {
            // Only an `ActorWithScope` leader owns a scope. Anywhere else this
            // actor is a queue with no way to execute anything.
            return;
        };
        let label = format!("worker-{}", self.labels.fetch_add(1, Ordering::Relaxed));
        let factory = self.worker_factory.clone();
        let requested = label.clone();
        let options = DynamicActorOptions::new()
            .restart(RestartPolicy::Always)
            .restart_intensity(RestartIntensity::new(4, Duration::from_secs(30)))
            .remove_on_exit(true);

        self.pending_adds += 1;
        ctx.offload(
            CONTROL_DEADLINE,
            async move { children.add_actor(requested, factory, options).await.ok() },
            move |outcome| PoolMsg::WorkerAdded {
                label,
                actor: outcome.ok().flatten(),
            },
        );
    }

    fn scale_down(&mut self, ctx: &MessageContext<'_, PoolMsg>) {
        while self.workers.len() > self.limits.min {
            let Some(label) = self
                .workers
                .iter()
                .find(|(_, slot)| slot.busy.is_none())
                .map(|(label, _)| label.clone())
            else {
                return;
            };
            self.release(&label, ctx);
        }
    }

    fn dispatch(
        &mut self,
        label: String,
        action: Action,
        digest: Digest,
        ctx: &impl LiveContext<PoolMsg>,
    ) {
        let Some(slot) = self.workers.get_mut(&label) else {
            self.queue.push_front((action, digest));
            return;
        };
        slot.busy = Some(action.target);
        self.report.dispatched += 1;

        let worker = slot.actor.clone();
        let request = action.clone();
        let reported = label.clone();
        ctx.offload(
            DISPATCH_DEADLINE,
            async move {
                // The inner bound is deliberately looser than the offload's.
                // The offload deadline is the one that must win, because only
                // it reports back as `OffloadDeadline`; a `call` timeout would
                // arrive indistinguishable from the worker having died.
                worker
                    .call(DISPATCH_DEADLINE * 4, |reply| WorkerMsg::Execute {
                        action: request,
                        digest,
                        reply,
                    })
                    .await
            },
            move |outcome| PoolMsg::Dispatched {
                worker: reported,
                action,
                digest,
                outcome: match outcome {
                    Ok(Ok(exec)) => DispatchOutcome::Finished(exec),
                    // The worker crashed with the request in flight, so its
                    // reply channel was dropped.
                    Ok(Err(_)) => DispatchOutcome::Lost,
                    Err(_deadline) => DispatchOutcome::Stalled,
                },
            },
        );
    }

    fn dispatched(
        &mut self,
        label: String,
        action: Action,
        digest: Digest,
        outcome: DispatchOutcome,
        ctx: &MessageContext<'_, PoolMsg>,
    ) {
        if let Some(slot) = self.workers.get_mut(&label) {
            slot.busy = None;
        }

        match outcome {
            DispatchOutcome::Finished(exec) => self.forward(action.target, exec, ctx),
            DispatchOutcome::Lost => {
                self.report.lost += 1;
                self.queue.push_front((action, digest));
            }
            DispatchOutcome::Stalled => {
                self.report.stalled += 1;
                self.report.retired += 1;
                self.queue.push_front((action, digest));
                // Re-dispatching to the same worker would just stall again:
                // it is still inside the blocking call that missed the
                // deadline. Removing it cancels that call and frees the thread.
                self.release(&label, ctx);
            }
        }
    }

    /// Sends a terminal outcome to the scheduler without blocking this loop.
    fn forward(&self, target: TargetId, outcome: ExecOutcome, ctx: &impl LiveContext<PoolMsg>) {
        let scheduler = self.scheduler.clone();
        ctx.offload(
            CONTROL_DEADLINE,
            async move {
                let _ = scheduler
                    .send(SchedulerMsg::Finished { target, outcome })
                    .await;
            },
            |_| PoolMsg::Noted,
        );
    }

    /// Drops a worker from the roster and removes it from the owned scope.
    fn release(&mut self, label: &str, ctx: &MessageContext<'_, PoolMsg>) {
        self.forget(label);
        let Some(children) = ctx.children() else {
            return;
        };
        let removing = label.to_owned();
        let removed = label.to_owned();
        ctx.offload(
            CONTROL_DEADLINE,
            async move {
                let _ = children.remove_child(removing).await;
            },
            move |_| PoolMsg::WorkerRemoved { label: removed },
        );
    }

    fn forget(&mut self, label: &str) {
        self.workers.remove(label);
        if let Some(watch) = self.watches.remove(label) {
            watch.cancel();
        }
    }

    fn watch_worker(
        &mut self,
        label: &str,
        actor: &ActorRef<WorkerMsg>,
        ctx: &impl LiveContext<PoolMsg>,
    ) {
        let watched = label.to_owned();
        let handle = ctx.watch(actor, move |event| PoolMsg::WorkerLifecycle {
            label: watched.clone(),
            event: match event {
                MonitorEvent::Up { .. } => WorkerLifecycle::Up,
                MonitorEvent::Terminated { .. } => WorkerLifecycle::Terminated,
                // `Down` may or may not be followed by a restart, and a
                // `Lagged` gap only means transitions were missed. Neither
                // changes the roster: the dispatch offload reports what
                // actually happened to the work.
                _ => WorkerLifecycle::Down,
            },
        });
        self.watches.insert(label.to_owned(), handle);
    }
}
