use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, Notify};
use tokio_otp::{SupervisorError, prelude::*};

#[derive(Clone)]
struct Probe {
    name: &'static str,
    order: Arc<Mutex<Vec<&'static str>>>,
    release: Option<Arc<Notify>>,
}

impl Actor for Probe {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut ActorContext<Self::Msg>) -> ActorResult {
        self.order.lock().await.push(self.name);
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if self.name == "first" {
            ctx.continue_with("continue");
        }
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut ActorContext<Self::Msg>,
    ) -> ActorResult {
        self.order.lock().await.push(message);
        Ok(Continue)
    }
}

#[tokio::test]
async fn actors_gate_sequential_start_on_on_start_and_run_continuations_first() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let first_order = order.clone();
    let first_release = release.clone();
    let first = graph.add(move || Probe {
        name: "first",
        order: first_order.clone(),
        release: Some(first_release.clone()),
    });
    let second_order = order.clone();
    graph.add(move || Probe {
        name: "second",
        order: second_order.clone(),
        release: None,
    });

    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .start_mode(StartMode::Sequential)
        .build()
        .unwrap()
        .spawn();

    first.send("mailbox").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(&*order.lock().await, &["first"]);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if order.lock().await.len() >= 4 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let observed = order.lock().await.clone();
    assert_eq!(observed[0], "first");
    assert!(observed.contains(&"second"));
    let continuation = observed
        .iter()
        .position(|item| *item == "continue")
        .unwrap();
    let mailbox = observed.iter().position(|item| *item == "mailbox").unwrap();
    assert!(continuation < mailbox);
    handle.shutdown_and_wait().await.unwrap();
}

#[derive(Clone)]
struct FailsOnStart;

impl Actor for FailsOnStart {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut ActorContext<Self::Msg>) -> ActorResult {
        Err(std::io::Error::other("actor init failed").into())
    }

    async fn handle(&mut self, (): (), _ctx: &mut ActorContext<Self::Msg>) -> ActorResult {
        Ok(Continue)
    }
}

#[tokio::test]
async fn failed_actor_start_disarms_readiness_without_panicking() {
    let mut graph = GraphBuilder::new();
    graph.add(|| FailsOnStart);
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .restart(RestartPolicy::Never)
        .build()
        .unwrap()
        .spawn();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
            .await
            .unwrap(),
        Err(SupervisorError::StartupAborted(_))
    ));
    let child = handle.snapshot().children.into_iter().next().unwrap();
    assert!(matches!(child.last_exit, Some(ExitStatusView::Failed(_))));
    handle.shutdown_and_wait().await.unwrap();
}

#[derive(Clone)]
struct DrainContinuation {
    handled: Arc<Mutex<Vec<&'static str>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for DrainContinuation {
    type Msg = &'static str;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut ActorContext<Self::Msg>,
    ) -> ActorResult {
        self.handled.lock().await.push(message);
        if message == "hold" || message == "hold-and-continue" {
            if message == "hold-and-continue" {
                ctx.continue_with("continued-before-shutdown");
            }
            self.started.notify_one();
            self.release.notified().await;
            while !ctx.shutdown_token().is_cancelled() {
                tokio::task::yield_now().await;
            }
        }
        if message == "trigger" {
            ctx.continue_with("continued");
        }
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_drops_continuations_queued_by_drained_messages() {
    let handled = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let actor_handled = handled.clone();
    let actor_started = started.clone();
    let actor_release = release.clone();
    let actor = graph.add(move || DrainContinuation {
        handled: actor_handled.clone(),
        started: actor_started.clone(),
        release: actor_release.clone(),
    });
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();
    handle.wait_started().await.unwrap();
    actor.send("hold").await.unwrap();
    started.notified().await;
    actor.send("trigger").await.unwrap();
    handle.shutdown();
    release.notify_one();
    handle.shutdown_and_wait().await.unwrap();
    assert_eq!(&*handled.lock().await, &["hold", "trigger"]);
}

#[tokio::test]
async fn external_shutdown_drops_a_continuation_queued_by_an_in_flight_handler() {
    let handled = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let actor_handled = handled.clone();
    let actor_started = started.clone();
    let actor_release = release.clone();
    let actor = graph.add(move || DrainContinuation {
        handled: actor_handled.clone(),
        started: actor_started.clone(),
        release: actor_release.clone(),
    });
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .build()
        .unwrap()
        .spawn();

    actor.send("hold-and-continue").await.unwrap();
    started.notified().await;
    actor.send("mailbox").await.unwrap();
    handle.shutdown();
    release.notify_one();
    handle.shutdown_and_wait().await.unwrap();

    assert_eq!(&*handled.lock().await, &["hold-and-continue", "mailbox"]);
}

#[derive(Clone)]
struct StopsOnStart {
    started: Arc<Notify>,
    release: Arc<Notify>,
    events: Arc<Mutex<Vec<&'static str>>>,
    policy: DrainPolicy,
}

impl Actor for StopsOnStart {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut ActorContext<Self::Msg>) -> ActorResult {
        ctx.continue_with("continuation");
        self.started.notify_one();
        self.release.notified().await;
        Ok(Stop)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut ActorContext<Self::Msg>,
    ) -> ActorResult {
        self.events.lock().await.push(message);
        Ok(Continue)
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext<Self::Msg>) -> Result<(), BoxError> {
        self.events.lock().await.push("stopped");
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        self.policy
    }
}

#[tokio::test]
async fn stop_from_on_start_drops_mailbox_and_continuations_then_runs_on_stop() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut graph = GraphBuilder::new();
    let actor = graph.add({
        let started = started.clone();
        let release = release.clone();
        let events = events.clone();
        move || StopsOnStart {
            started: started.clone(),
            release: release.clone(),
            events: events.clone(),
            policy: DrainPolicy::Discard,
        }
    });
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .restart(RestartPolicy::Never)
        .build()
        .unwrap()
        .spawn();

    started.notified().await;
    actor.send("mailbox").await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while events.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(&*events.lock().await, &["stopped"]);
    handle.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn stop_from_on_start_with_drain_handles_the_queued_mailbox_only() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut graph = GraphBuilder::new();
    let actor = graph.add({
        let started = started.clone();
        let release = release.clone();
        let events = events.clone();
        move || StopsOnStart {
            started: started.clone(),
            release: release.clone(),
            events: events.clone(),
            policy: DrainPolicy::Drain,
        }
    });
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .restart(RestartPolicy::Never)
        .build()
        .unwrap()
        .spawn();

    started.notified().await;
    actor.send("mailbox").await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while events.lock().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(&*events.lock().await, &["mailbox", "stopped"]);
    handle.shutdown_and_wait().await.unwrap();
}

#[derive(Clone)]
struct PromptRaw;

impl RawActor for PromptRaw {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<Self::Msg>) -> ActorResult {
        Ok(Continue)
    }
}

#[tokio::test]
async fn prompt_raw_actor_delivers_readiness_before_completion() {
    let mut graph = GraphBuilder::new();
    graph.add(|| PromptRaw);
    let handle = Runtime::builder()
        .graph(graph.build().unwrap())
        .restart(RestartPolicy::Never)
        .build()
        .unwrap()
        .spawn();
    tokio::time::timeout(Duration::from_secs(1), handle.wait_started())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        handle.snapshot().children[0].last_exit,
        Some(ExitStatusView::Completed)
    ));
    handle.shutdown_and_wait().await.unwrap();
}
