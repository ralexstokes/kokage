use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_otp::prelude::*;

#[allow(unused_imports)]
mod coverage_probe {
    mod actor {
        use tokio_otp::prelude::{
            Actor, ActorOptions, ActorRef, ActorResult, AmbientContext, BoxError, CallError,
            CancellationHandle, CancellationToken, Continue, DEFAULT_SHUTDOWN_BOUND, Down,
            DownReason, DrainPolicy, Flow, Graph, GraphBuilder, Lifetime, MailboxMode,
            MessageContext, MessageSize, MonitorEvent, RawActor, Reply, SendError, Stop,
            Supervision, TimerKey,
        };
    }

    mod supervisor {
        use tokio_otp::prelude::{
            BackoffPolicy, ChildMembershipView, ChildSnapshot, ChildStateView, CompletionOutcome,
            ExitStatusView, LifecycleEvent, LifecyclePathSegment, LifecycleWatch, RestartIntensity,
            RestartPolicy, ScopeKind, ShutdownMode, ShutdownPolicy, Strategy, SupervisorSnapshot,
            SupervisorStateView,
        };
    }

    mod otp {
        use tokio_otp::prelude::{LifecycleWatchGuard, Runtime, RuntimeBuilder, RuntimeHandle};
    }

    mod advanced_root {
        use tokio_otp::{
            ActorSupervisorPathSegment, ChildSpec, ControlError, LifecycleEvent,
            LifecyclePathSegment, LifecycleWatch, LiveContext, SupervisorBuildError,
            SupervisorError,
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
        Ok(Continue)
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

    let runtime = Runtime::builder()
        .graph(graph.build().expect("valid graph"))
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
