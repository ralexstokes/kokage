use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::{ChildStateView, LifecycleEvent, prelude::*};

#[allow(unused_imports)]
mod coverage_probe {
    mod expected {
        use tokio_otp::prelude::{
            Actor, ActorContext, ActorFactory, ActorOptions, ActorRef, ActorResult, ActorSpec,
            AmbientContext, BoxError, CallError, GraphBuilder, GraphConfig, LiveContext,
            MessageContext, RawActor, Reply, RestartIntensity, RestartPolicy, Runtime,
            RuntimeHandle, SendError, ShutdownPolicy, StartContext, StopContext, Strategy,
            Supervision, SupervisionTree,
        };
    }

    mod advanced_root {
        use tokio_otp::{
            ActorSupervisorPathSegment, AddSubtreeError, BackoffPolicy, BlockingCancelled,
            CancellationHandle, CancellationToken, ChildMembershipView, ChildOutline,
            ChildSnapshot, ChildSpec, ChildStateView, CompletionOutcome, ControlError,
            DEFAULT_SHUTDOWN_BOUND, Down, DownReason, DrainPolicy, DynamicScope, ExitStatusView,
            Graph, GraphBuildError, LifecycleEvent, LifecyclePathSegment, LifecycleWatch,
            LifecycleWatchGuard, Lifetime, MailboxMode, MessageSize, MonitorEvent, OffloadDeadline,
            OffloadHandle, ReservedSupervisionTree, RestrictedScope, ScopeKind, ShutdownMode,
            SupervisionFactories, SupervisionOutline, SupervisorBuildError, SupervisorError,
            SupervisorSnapshot, SupervisorStateView, TimerKey, TryRecvError,
        };
        use tokio_supervisor::{
            ChildContext, ChildResult, DynamicSupervisorBuilder, Supervisor, SupervisorBuilder,
            SupervisorHandle, SupervisorSpec,
        };
    }
}

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

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

    let runtime = SupervisionTree::graph(&graph.build().expect("valid graph"))
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();
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
                LifecycleEvent::Started {
                    ref child_id,
                    generation: 0,
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
        LifecycleEvent::Started {
            ref child_id,
            generation: 0,
            ..
        } if child_id == "worker"
    ));

    let snapshot = handle.snapshot();
    assert_eq!(
        snapshot
            .child("worker")
            .expect("worker child should exist")
            .state,
        ChildStateView::Running
    );

    handle
        .shutdown_and_wait()
        .await
        .expect("shutdown should succeed");
}
