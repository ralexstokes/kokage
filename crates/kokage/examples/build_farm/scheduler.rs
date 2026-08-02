//! Finite dependency scheduling over a dynamic scope of one-shot task workers.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream::FuturesUnordered};
use kokage::{
    ActorRef, DynamicScopeRef, ExitResult, OneShotTaskSpec, TaskRef, observe::ExitStatus,
};
use tokio::time::timeout;

use crate::{
    lease::Lease,
    messages::{Artifact, CasMsg, Phase, ProgressMsg},
    model::{Action, Behavior, BuildPlan, Digest, TargetId, artifact_size, digest},
    shared::{AttemptBook, BuildJournal, BuildReport, TargetState},
};

const CALL_BOUND: Duration = Duration::from_secs(1);
const WORKER_BOUND: Duration = Duration::from_millis(120);

struct WorkerCompletion {
    action: Action,
    digest: Digest,
    worker: TaskRef,
    outcome: Result<Result<ExitStatus, kokage::TaskError>, tokio::time::error::Elapsed>,
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
        while !lease.is_held() {
            report.lease_waits += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

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
            let worker = workers
                .spawn_once_spec(OneShotTaskSpec::new(worker_id, move |ctx| {
                    run_worker(
                        ctx.shutdown_token().clone(),
                        task_action,
                        action_digest,
                        attempt,
                        task_cas,
                        task_progress,
                    )
                }))
                .await?;
            report.peak_workers = report.peak_workers.max(workers.snapshot().children.len());
            let waiting = worker.clone();
            pending.push(async move {
                WorkerCompletion {
                    action,
                    digest: action_digest,
                    worker,
                    outcome: timeout(WORKER_BOUND, waiting.wait()).await,
                }
            });
        }

        while let Some(completion) = pending.next().await {
            match completion.outcome {
                Ok(Ok(exit)) if exit.is_completed() => {
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
                Ok(Ok(_)) => {
                    report.failed_attempts += 1;
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    report.retired_workers += 1;
                    workers.remove(&completion.worker).await?;
                }
            }
        }
    }

    report.complete = true;
    journal.record(report);
    Ok(())
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
        shutdown.cancelled().await;
        return Ok(());
    }

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
