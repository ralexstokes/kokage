use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::{
    observe::{ChildLifecycleEvent, ChildLifecycleEventKind, LifecycleEvent, LifecycleEventKind},
    prelude::*,
};

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use tokio_otp::prelude::{
            Actor, ActorContext, ActorOptions, ActorRef, ActorResult, ActorSpec, CallError,
            DynamicTree, GraphBuilder, LiveContext, MessageContext, OrderedTree, Reply,
            RestartConfig, RestartPolicy, RuntimeHandle, SendError, ShutdownPolicy, StartContext,
            StopContext, Strategy, TrySendError,
        };
    }

    mod advanced_root {
        use tokio_otp::{
            ActorFactory, ActorSlot, BackoffPolicy, BlockingCancelled, CancellationHandle,
            CancellationToken, ControlError, Down, DownReason, DrainPolicy, DynamicActorOptions,
            DynamicScope, Graph, GraphBuildError, GraphConfig, GraphLookupError, Lifetime,
            MailboxMode, MonitorEvent, OffloadDeadline, OffloadHandle, RestrictedScope, ScopeKind,
            ScopeWaitHandle, Supervision, SupervisorBuildError, SupervisorError, TimerKey,
            TreeNode,
        };
        use tokio_supervisor::{ChildContext, ChildResult, Supervisor, SupervisorHandle};
    }

    mod host {
        use tokio_otp::host::{
            ActorRunError, BoxError, ChildContext, ChildResult, ChildSpec, DEFAULT_SHUTDOWN_BOUND,
            RawActor, RunnableActor,
        };
    }

    mod observe {
        use tokio_otp::observe::{
            ActorStats, ChildExitView, ChildLifecycleEvent, ChildLifecycleEventKind,
            ChildLifecycleWatch, ChildMembershipView, ChildOutline, ChildSnapshot, ChildStateView,
            CompletionGuard, CompletionOutcome, ExitStatusView, LifecycleEvent, LifecycleEventKind,
            LifecyclePathSegment, LifecycleWatch, LifecycleWatchGuard, SupervisionOutline,
            SupervisorLifecycleEvent, SupervisorPathSegment, SupervisorSnapshot,
            SupervisorStateView,
        };
    }

    mod derive_private {
        use tokio_otp::__private::{SupervisionFactories, qualified_label};
    }
}

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

async fn named_task(ctx: tokio_otp::host::ChildContext) -> tokio_otp::host::ChildResult {
    ctx.shutdown_token().cancelled().await;
    Ok(())
}

#[test]
fn host_task_surface_supports_a_named_factory_without_tokio_supervisor_imports() {
    let _child = tokio_otp::host::ChildSpec::task("worker", named_task);
}

#[test]
fn actor_supervisor_path_segments_are_nameable() {
    fn path_len(path: &[tokio_otp::observe::SupervisorPathSegment]) -> usize {
        path.len()
    }

    let path: Vec<tokio_otp::observe::SupervisorPathSegment> = Vec::new();
    assert_eq!(path_len(&path), 0);
}

#[test]
fn closed_policy_sets_can_be_matched_exhaustively() {
    fn strategy_name(strategy: Strategy) -> &'static str {
        match strategy {
            Strategy::OneForOne => "one-for-one",
            Strategy::OneForAll => "one-for-all",
            Strategy::RestForOne => "rest-for-one",
        }
    }

    fn restart_name(policy: RestartPolicy) -> &'static str {
        match policy {
            RestartPolicy::Always => "always",
            RestartPolicy::OnFailure => "on-failure",
            RestartPolicy::Never => "never",
        }
    }

    fn drain_name(policy: tokio_otp::DrainPolicy) -> &'static str {
        match policy {
            tokio_otp::DrainPolicy::Discard => "discard",
            tokio_otp::DrainPolicy::Drain => "drain",
        }
    }

    fn scope_name(kind: tokio_otp::ScopeKind) -> &'static str {
        match kind {
            tokio_otp::ScopeKind::Ordered => "ordered",
            tokio_otp::ScopeKind::Dynamic => "dynamic",
        }
    }

    assert_eq!(strategy_name(Strategy::default()), "one-for-one");
    assert_eq!(restart_name(RestartPolicy::default()), "on-failure");
    assert_eq!(drain_name(tokio_otp::DrainPolicy::default()), "drain");
    assert_eq!(scope_name(tokio_otp::ScopeKind::default()), "ordered");
}

#[derive(Clone)]
struct BlockingWorker {
    observed: mpsc::UnboundedSender<String>,
}

impl Actor for BlockingWorker {
    type Msg = ();

    async fn handle(&mut self, _message: (), ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        let observed = self.observed.clone();
        let actor_id = ctx.id().to_owned();
        ctx.run_blocking(move |token| {
            assert!(!token.is_cancelled());
            observed.send(actor_id).expect("test receiver dropped");
        })
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn umbrella_prelude_supports_blocking_and_supervisor_helpers() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (worker_slot, worker) = graph.slot("worker");
    graph.define(worker_slot, move || BlockingWorker {
        observed: observed_tx.clone(),
    });

    let handle = OrderedTree::graph(graph.build().expect("valid graph"))
        .strategy(Strategy::OneForOne)
        .spawn()
        .expect("runtime builds");
    let mut events = handle.watch_lifecycle_recursive();
    worker.send(()).await.expect("worker accepts message");
    let observed = timeout(EVENT_TIMEOUT, observed_rx.recv())
        .await
        .expect("timed out waiting for blocking task")
        .expect("blocking task reported completion");
    assert_eq!(observed, "worker");

    let started = timeout(EVENT_TIMEOUT, async {
        loop {
            let event = events.next().await.expect("lifecycle remains open");
            if matches!(
                event,
                LifecycleEvent {
                    kind: LifecycleEventKind::Child(ChildLifecycleEvent {
                        ref child_id,
                        kind: ChildLifecycleEventKind::Started { generation: 0 },
                        ..
                    }),
                    ..
                } if child_id == "worker"
            ) {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for started event");
    assert!(matches!(
        started,
        LifecycleEvent {
            kind: LifecycleEventKind::Child(ChildLifecycleEvent {
                ref child_id,
                kind: ChildLifecycleEventKind::Started { generation: 0 },
                ..
            }),
            ..
        } if child_id == "worker"
    ));

    let snapshot = handle.snapshot();
    assert!(
        snapshot
            .child("worker")
            .expect("worker child should exist")
            .state
            .is_running()
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}
