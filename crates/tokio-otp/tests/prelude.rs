use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::prelude::*;

#[allow(unused_imports)]
mod coverage_probe {
    mod actor {
        use tokio_otp::prelude::{
            Actor, ActorOptions, ActorRef, ActorResult, BoxError, CallError, CancellationHandle,
            CancellationToken, Continue, Down, DownReason, DrainPolicy, Flow, Graph, GraphBuilder,
            HandleContext, MailboxMode, MessageSize, MonitorEvent, RawActor, Reply, SendError,
            Stop, Topology,
        };
    }

    mod supervisor {
        use tokio_otp::prelude::{
            AttachedChild, AttachedChildIdentity, BackoffPolicy, ChildMembershipView,
            ChildSnapshot, ChildStateView, CompletionGuard, CompletionOutcome, ControlOperation,
            ExitStatusView, LifecycleEvent, LifecycleEventKind, LifecyclePathSegment,
            LifecycleWatch, RecursiveLifecycleEvent, RecursiveLifecycleEventKind,
            RecursiveLifecycleWatch, RestartIntensity, RestartPolicy, ScopeKind, ShutdownMode,
            ShutdownPolicy, Strategy, SupervisorSnapshot, SupervisorSnapshotReceiverExt as _,
            SupervisorStateView,
        };
    }

    mod otp {
        use tokio_otp::prelude::{LifecycleWatchGuard, Runtime, RuntimeBuilder, RuntimeHandle};
    }

    mod advanced_root {
        use tokio_otp::{
            ChildContext, ChildResult, ChildSpec, ControlError, LifecycleEvent, LifecycleEventKind,
            LifecyclePathSegment, LifecycleWatch, LiveContext, RecursiveLifecycleEvent,
            RecursiveLifecycleEventKind, RecursiveLifecycleWatch, Supervisor, SupervisorBuildError,
            SupervisorBuilder, SupervisorError, SupervisorHandle, SupervisorSpec, SupervisorToken,
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

    async fn handle(&mut self, _message: (), ctx: &mut HandleContext<'_, ()>) -> ActorResult {
        let observed = self.observed.clone();
        let actor_id = ctx.id().to_owned();
        ctx.run_blocking(move |token| {
            assert!(!token.is_cancelled());
            observed.send(actor_id).expect("test receiver dropped");
        })
        .await?;
        Ok(Continue)
    }
}

#[tokio::test]
async fn umbrella_prelude_supports_blocking_and_supervisor_helpers() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let worker = graph.actor("worker", move || BlockingWorker {
        observed: observed_tx.clone(),
    });

    let runtime = Runtime::builder()
        .graph(graph.build().expect("valid graph"))
        .strategy(Strategy::OneForOne)
        .build()
        .expect("runtime builds");
    let handle = runtime.spawn();
    let mut events = handle.watch_lifecycle_recursive();
    let mut snapshots = handle.subscribe_snapshots();

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
                event.kind,
                RecursiveLifecycleEventKind::Child(LifecycleEvent {
                    ref child_id,
                    kind: LifecycleEventKind::Started { generation: 0 },
                    ..
                }) if child_id == "worker"
            ) {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for started event");
    assert!(matches!(
        started.kind,
        RecursiveLifecycleEventKind::Child(LifecycleEvent {
            ref child_id,
            kind: LifecycleEventKind::Started { generation: 0 },
            ..
        }) if child_id == "worker"
    ));

    let snapshot = timeout(
        EVENT_TIMEOUT,
        snapshots.wait_for_snapshot(|snapshot| {
            snapshot
                .child("worker")
                .is_some_and(|child| child.state == ChildStateView::Running)
        }),
    )
    .await
    .expect("timed out waiting for running snapshot")
    .expect("snapshot stream should remain open");
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
