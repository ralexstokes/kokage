//! Finite dependency scheduling over a dynamic scope of one-shot task workers.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream::FuturesUnordered};
use kokage::{
    ActorRef, ControlError, DynamicScopeRef, ExitResult, OneShotTaskSpec, TaskRef,
    observe::ExitStatus,
};
use tokio::{sync::oneshot, time::timeout};

use crate::{
    lease::{Lease, LeaseState},
    messages::{Artifact, CasMsg, Phase, ProgressMsg},
    model::{Action, Behavior, BuildPlan, Digest, TargetId, artifact_size, digest},
    shared::{AttemptBook, BuildJournal, BuildReport, TargetState},
};

const CALL_BOUND: Duration = Duration::from_secs(1);
const WORKER_BACKSTOP: Duration = Duration::from_secs(3);
const MAX_TARGET_ATTEMPTS: u32 = 3;

enum WorkerOutcome {
    Exited(Result<ExitStatus, kokage::TaskError>),
    Parked,
    BackstopElapsed,
}

struct WorkerCompletion {
    action: Action,
    digest: Digest,
    worker: TaskRef,
    outcome: WorkerOutcome,
}

pub struct Scheduler {
    pub plan: Arc<BuildPlan>,
    pub cas: ActorRef<CasMsg>,
    pub progress: ActorRef<ProgressMsg>,
    pub workers: DynamicScopeRef,
    pub lease: Arc<Lease>,
    pub attempts: Arc<AttemptBook>,
    pub journal: Arc<BuildJournal>,
}

pub async fn run(scheduler: Scheduler) -> ExitResult {
    let Scheduler {
        plan,
        cas,
        progress,
        workers,
        lease,
        attempts,
        journal,
    } = scheduler;
    let mut lease_state = lease.subscribe();
    let mut observed_outages = 0;
    let mut report = BuildReport {
        targets: plan
            .actions()
            .iter()
            .map(|action| (action.target, TargetState::Blocked))
            .collect(),
        ..BuildReport::default()
    };

    while !report
        .targets
        .values()
        .all(|state| matches!(state, TargetState::Built { .. }))
    {
        wait_for_lease(&mut lease_state, &mut observed_outages, &mut report).await?;

        let ready = ready_actions(&plan, &report.targets);
        if ready.is_empty() {
            return Err(std::io::Error::other("build dependency graph made no progress").into());
        }

        let mut pending = FuturesUnordered::new();
        for (action, action_digest) in ready {
            if cas
                .call(
                    |reply| CasMsg::Lookup {
                        digest: action_digest,
                        reply,
                    },
                    CALL_BOUND,
                )
                .await?
                .is_some()
            {
                report.targets.insert(
                    action.target,
                    TargetState::Built {
                        digest: action_digest,
                        cached: true,
                    },
                );
                report.cache_hits += 1;
                progress
                    .send(ProgressMsg {
                        target: action.target,
                        phase: Phase::Cached,
                    })
                    .await?;
                continue;
            }

            let attempt = attempts.begin(action.target);
            if attempt > MAX_TARGET_ATTEMPTS {
                return Err(std::io::Error::other(format!(
                    "{} exceeded the per-target limit of {MAX_TARGET_ATTEMPTS} attempts",
                    action.target
                ))
                .into());
            }
            report.submissions += 1;
            progress
                .send(ProgressMsg {
                    target: action.target,
                    phase: if attempt == 1 {
                        Phase::Queued
                    } else {
                        Phase::Retrying
                    },
                })
                .await?;

            let worker_id = format!("{}-attempt-{attempt}", action.target);
            let task_action = action.clone();
            let task_cas = cas.clone();
            let task_progress = progress.clone();
            let (parked, parked_rx) = oneshot::channel();
            let worker = workers
                .spawn_once_spec(OneShotTaskSpec::new(worker_id, move |ctx| {
                    run_worker(
                        ctx.shutdown_token().clone(),
                        task_action,
                        action_digest,
                        attempt,
                        task_cas,
                        task_progress,
                        parked,
                    )
                }))
                .await?;
            let waiting = worker.clone();
            pending.push(async move {
                WorkerCompletion {
                    action,
                    digest: action_digest,
                    worker,
                    outcome: observe_worker(waiting, parked_rx).await,
                }
            });
            // This counts the scheduler-owned in-flight set rather than a
            // membership snapshot that may still contain an unreaped exit.
            report.peak_workers = report.peak_workers.max(pending.len());
        }

        while let Some(completion) = pending.next().await {
            match completion.outcome {
                WorkerOutcome::Exited(Ok(exit)) if exit.is_completed() => {
                    let artifact = cas
                        .call(
                            |reply| CasMsg::Lookup {
                                digest: completion.digest,
                                reply,
                            },
                            CALL_BOUND,
                        )
                        .await?
                        .ok_or_else(|| {
                            std::io::Error::other(format!(
                                "worker completed without storing {}",
                                completion.action.target
                            ))
                        })?;
                    report.targets.insert(
                        completion.action.target,
                        TargetState::Built {
                            digest: artifact.digest,
                            cached: false,
                        },
                    );
                }
                WorkerOutcome::Exited(Ok(_)) => {
                    report.failed_attempts += 1;
                }
                WorkerOutcome::Exited(Err(error)) => return Err(error.into()),
                WorkerOutcome::Parked => {
                    report.retired_workers += 1;
                    match workers.remove(&completion.worker).await {
                        Ok(())
                        | Err(ControlError::UnknownChildId(_))
                        | Err(ControlError::UnknownChildHandle) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                WorkerOutcome::BackstopElapsed => {
                    return Err(std::io::Error::other(format!(
                        "worker {} exceeded the {WORKER_BACKSTOP:?} liveness backstop",
                        completion.action.target
                    ))
                    .into());
                }
            }
        }
    }

    report.complete = true;
    journal.record(report);
    Ok(())
}

async fn wait_for_lease(
    state: &mut tokio::sync::watch::Receiver<LeaseState>,
    observed_outages: &mut u64,
    report: &mut BuildReport,
) -> Result<(), std::io::Error> {
    loop {
        let current = *state.borrow_and_update();
        report.lease_outages += current.outages.saturating_sub(*observed_outages);
        *observed_outages = current.outages;
        if current.held {
            return Ok(());
        }
        state
            .changed()
            .await
            .map_err(|_| std::io::Error::other("lease state channel closed"))?;
    }
}

async fn observe_worker(worker: TaskRef, mut parked: oneshot::Receiver<()>) -> WorkerOutcome {
    let after_signal_close = worker.clone();
    match timeout(WORKER_BACKSTOP, async move {
        tokio::select! {
            exit = worker.wait() => WorkerOutcome::Exited(exit),
            signal = &mut parked => match signal {
                Ok(()) => WorkerOutcome::Parked,
                Err(_) => WorkerOutcome::Exited(after_signal_close.wait().await),
            },
        }
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => WorkerOutcome::BackstopElapsed,
    }
}

fn ready_actions(
    plan: &BuildPlan,
    states: &BTreeMap<TargetId, TargetState>,
) -> Vec<(Action, Digest)> {
    plan.actions()
        .iter()
        .filter(|action| states.get(action.target) == Some(&TargetState::Blocked))
        .filter_map(|action| {
            action
                .dependencies
                .iter()
                .map(|target| match states.get(target) {
                    Some(TargetState::Built { digest, .. }) => Some((*target, *digest)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(|dependencies| (action.clone(), digest(action, dependencies)))
        })
        .collect()
}

async fn run_worker(
    shutdown: kokage::CancellationToken,
    action: Action,
    digest: Digest,
    attempt: u32,
    cas: ActorRef<CasMsg>,
    progress: ActorRef<ProgressMsg>,
    parked: oneshot::Sender<()>,
) -> ExitResult {
    progress
        .send(ProgressMsg {
            target: action.target,
            phase: Phase::Running,
        })
        .await?;

    if action.behavior == Behavior::FailOnce && attempt == 1 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        return Err(std::io::Error::other(format!(
            "toolchain crashed compiling {}",
            action.target
        ))
        .into());
    }

    if action.behavior == Behavior::StallOnce && attempt == 1 {
        let _ = parked.send(());
        shutdown.cancelled().await;
        return Ok(());
    }

    drop(parked);

    for _ in 0..4 {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(12)) => {}
        }
    }

    let artifact = Artifact {
        target: action.target,
        digest,
        bytes: artifact_size(&action),
    };
    cas.call(|reply| CasMsg::Store { artifact, reply }, CALL_BOUND)
        .await?;
    progress
        .send(ProgressMsg {
            target: action.target,
            phase: Phase::Built,
        })
        .await?;
    Ok(())
}
