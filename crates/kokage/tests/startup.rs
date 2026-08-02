mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "host")]
use kokage::raw::ActorRunError;
use kokage::{
    ActorSpec, BoxError, DynamicScopeRef, DynamicTree, MailboxShutdown, RestartPolicy, SendError,
    SendErrorKind, Shutdown, SupervisorError, TaskSpec,
    observe::ExitStatus,
    prelude::*,
    raw::{RawActor, RawContext},
};
use tokio::sync::{Mutex, Notify, mpsc, watch};

#[derive(Clone)]
struct Probe {
    name: &'static str,
    order: Arc<Mutex<Vec<&'static str>>>,
    release: Option<Arc<Notify>>,
}

impl Actor for Probe {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.order.lock().await.push(self.name);
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if self.name == "first" {
            ctx.continue_with("continue");
        }
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.order.lock().await.push(message);
        Ok(())
    }
}

#[derive(Clone)]
struct AddsChildOnStart {
    handle_rx: watch::Receiver<Option<DynamicScopeRef>>,
    added_started: Arc<Notify>,
}

impl Actor for AddsChildOnStart {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let handle = {
            let ready = self
                .handle_rx
                .wait_for(Option::is_some)
                .await
                .expect("test handle sender remains open");
            ready
                .as_ref()
                .expect("runtime handle was installed")
                .clone()
        };
        let added_started = Arc::clone(&self.added_started);
        handle
            .add_task_spec(TaskSpec::new("added-from-on-start", move |ctx| {
                let added_started = Arc::clone(&added_started);
                async move {
                    added_started.notify_one();
                    ctx.shutdown_token().cancelled().await;
                    Ok(())
                }
            }))
            .await?;
        Ok(())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn actor_on_start_can_await_add_task_on_its_own_dynamic_supervisor() {
    let (handle_tx, handle_rx) = watch::channel::<Option<DynamicScopeRef>>(None);
    let added_started = Arc::new(Notify::new());
    let handle = DynamicTree::new().spawn().expect("dynamic runtime builds");
    handle_tx
        .send(Some(handle.scope()))
        .expect("startup actor retains handle receiver");
    support::dynamic_root(&handle)
        .add_actor_spec(ActorSpec::new("starter", {
            let added_started = Arc::clone(&added_started);
            move || AddsChildOnStart {
                handle_rx: handle_rx.clone(),
                added_started: Arc::clone(&added_started),
            }
        }))
        .await
        .expect("startup actor added");

    tokio::time::timeout(Duration::from_secs(1), added_started.notified())
        .await
        .expect("self-scope add_task should not deadlock actor startup");
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn actors_gate_sequential_start_on_on_start_and_run_continuations_first() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let first_order = order.clone();
    let first_release = release.clone();
    let first = graph.add_actor_spec(ActorSpec::new("Probe", move || Probe {
        name: "first",
        order: first_order.clone(),
        release: Some(first_release.clone()),
    }));
    let second_order = order.clone();
    graph.add_actor_spec(ActorSpec::new("Probe-2", move || Probe {
        name: "second",
        order: second_order.clone(),
        release: None,
    }));

    let handle = graph.spawn().unwrap();

    first.send("mailbox").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(&*order.lock().await, &["first"]);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
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
    handle.shutdown().await.unwrap();
}

#[derive(Clone)]
struct FailsOnStart;

impl Actor for FailsOnStart {
    type Msg = ();

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        Err(std::io::Error::other("actor init failed").into())
    }

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn failed_actor_start_disarms_readiness_without_panicking() {
    let mut graph = Tree::new();
    graph.add_actor_spec(ActorSpec::new("FailsOnStart", || FailsOnStart));
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
            .await
            .unwrap(),
        Err(SupervisorError::StartupAborted(_))
    ));
    let child = handle
        .scope()
        .snapshot()
        .children
        .into_iter()
        .next()
        .unwrap();
    assert!(
        child
            .state
            .last_exit()
            .is_some_and(|exit| exit.failure_message().is_some())
    );
    handle.shutdown().await.unwrap();
}

#[derive(Clone)]
struct DrainContinuation {
    handled: Arc<Mutex<Vec<&'static str>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for DrainContinuation {
    type Msg = &'static str;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
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
        Ok(())
    }
}

#[tokio::test]
async fn drain_drops_continuations_queued_by_drained_messages() {
    let handled = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let actor_handled = handled.clone();
    let actor_started = started.clone();
    let actor_release = release.clone();
    let actor = graph.add_actor_spec(ActorSpec::new("DrainContinuation", move || {
        DrainContinuation {
            handled: actor_handled.clone(),
            started: actor_started.clone(),
            release: actor_release.clone(),
        }
    }));
    let handle = graph.spawn().unwrap();
    handle.scope().wait_started().await.unwrap();
    actor.send("hold").await.unwrap();
    started.notified().await;
    actor.send("trigger").await.unwrap();
    handle.scope().request_shutdown();
    release.notify_one();
    handle.shutdown().await.unwrap();
    assert_eq!(&*handled.lock().await, &["hold", "trigger"]);
}

#[tokio::test]
async fn external_shutdown_drops_a_continuation_queued_by_an_in_flight_handler() {
    let handled = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let actor_handled = handled.clone();
    let actor_started = started.clone();
    let actor_release = release.clone();
    let actor = graph.add_actor_spec(ActorSpec::new("DrainContinuation", move || {
        DrainContinuation {
            handled: actor_handled.clone(),
            started: actor_started.clone(),
            release: actor_release.clone(),
        }
    }));
    let handle = graph.spawn().unwrap();

    actor.send("hold-and-continue").await.unwrap();
    started.notified().await;
    actor.send("mailbox").await.unwrap();
    handle.scope().request_shutdown();
    release.notify_one();
    handle.shutdown().await.unwrap();

    assert_eq!(&*handled.lock().await, &["hold-and-continue", "mailbox"]);
}

/// One `handle` call as the probe saw it: the message, drain phase, and
/// graph shutdown state.
type HandleCalls = Arc<Mutex<Vec<(&'static str, bool, bool)>>>;

/// Records, for every handled message, which phase the provided loop called
/// `handle` from and whether the graph was shutting down at the time.
#[derive(Clone)]
struct DrainPhaseProbe {
    observed: HandleCalls,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for DrainPhaseProbe {
    type Msg = &'static str;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.lock().await.push((
            message,
            ctx.is_draining(),
            ctx.shutdown_token().is_cancelled(),
        ));
        match message {
            "hold" => {
                self.started.notify_one();
                self.release.notified().await;
                // Returning only once the request is visible keeps the next
                // queued message on the drain path rather than the ordinary
                // one, which is what this test is about.
                while !ctx.shutdown_token().is_cancelled() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            "stop" => {
                self.started.notify_one();
                self.release.notified().await;
                ctx.stop();
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn drain_phase_probe_graph(
    observed: &HandleCalls,
    started: &Arc<Notify>,
    release: &Arc<Notify>,
) -> (Tree, ActorRef<&'static str>) {
    let mut graph = Tree::new();
    let observed = observed.clone();
    let started = started.clone();
    let release = release.clone();
    let actor = graph.add_actor_spec(ActorSpec::new("DrainPhaseProbe", move || DrainPhaseProbe {
        observed: observed.clone(),
        started: started.clone(),
        release: release.clone(),
    }));
    (graph, actor)
}

#[tokio::test]
async fn is_draining_separates_the_drain_phase_from_ordinary_handling() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (graph, actor) = drain_phase_probe_graph(&observed, &started, &release);
    let handle = graph.spawn().unwrap();
    handle.scope().wait_started().await.unwrap();

    actor.send("hold").await.unwrap();
    started.notified().await;
    actor.send("queued").await.unwrap();
    handle.scope().request_shutdown();
    release.notify_one();
    handle.shutdown().await.unwrap();

    assert_eq!(
        &*observed.lock().await,
        &[("hold", false, false), ("queued", true, true)]
    );
}

#[tokio::test]
async fn is_draining_after_a_self_stop_that_never_shuts_the_graph_down() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (graph, actor) = drain_phase_probe_graph(&observed, &started, &release);
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();
    handle.scope().wait_started().await.unwrap();

    actor.send("stop").await.unwrap();
    started.notified().await;
    actor.send("queued").await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while observed.lock().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // The drain runs because the actor asked to stop, not because the graph
    // is going away: the shutdown token is still live for the drained message.
    assert_eq!(
        &*observed.lock().await,
        &[("stop", false, false), ("queued", true, false)]
    );
    handle.shutdown().await.unwrap();
}

#[derive(Clone)]
struct OverlappingStopProbe {
    observed: mpsc::UnboundedSender<bool>,
}

impl Actor for OverlappingStopProbe {
    type Msg = &'static str;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            "hold" => {
                self.observed.send(ctx.is_draining()).unwrap();
                ctx.shutdown_token().cancelled().await;
                self.observed.send(ctx.is_draining()).unwrap();
                ctx.stop();
                self.observed.send(ctx.is_draining()).unwrap();
            }
            "queued" => {
                self.observed.send(ctx.is_draining()).unwrap();
                ctx.stop();
                self.observed.send(ctx.is_draining()).unwrap();
            }
            other => panic!("unexpected message: {other}"),
        }
        Ok(())
    }
}

#[tokio::test]
async fn is_draining_changes_only_after_the_stopping_callback_returns() {
    let (observed, mut statuses) = mpsc::unbounded_channel();
    let mut graph = Tree::new();
    let actor = graph.add_actor_spec(ActorSpec::new("OverlappingStopProbe", move || {
        OverlappingStopProbe {
            observed: observed.clone(),
        }
    }));
    let handle = graph.spawn().unwrap();
    handle.scope().wait_started().await.unwrap();

    actor.send("hold").await.unwrap();
    assert_eq!(statuses.recv().await, Some(false));
    actor.send("queued").await.unwrap();
    handle.scope().request_shutdown();

    assert_eq!(statuses.recv().await, Some(false));
    assert_eq!(statuses.recv().await, Some(false));
    assert_eq!(statuses.recv().await, Some(true));
    assert_eq!(statuses.recv().await, Some(true));
    handle.shutdown().await.unwrap();
}

#[derive(Clone)]
struct StopsOnStart {
    started: Arc<Notify>,
    release: Arc<Notify>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for StopsOnStart {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        assert!(!ctx.is_draining());
        ctx.continue_with("continuation");
        self.started.notify_one();
        self.release.notified().await;
        ctx.stop();
        assert!(!ctx.is_draining());
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.events.lock().await.push(message);
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        self.events.lock().await.push("stopped");
        Ok(())
    }
}

#[tokio::test]
async fn on_start_context_stop_drops_mailbox_and_continuations_then_runs_on_stop() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut graph = Tree::new();
    let actor = graph.add_actor_spec(
        ActorSpec::new("StopsOnStart", {
            let started = started.clone();
            let release = release.clone();
            let events = events.clone();
            move || StopsOnStart {
                started: started.clone(),
                release: release.clone(),
                events: events.clone(),
            }
        })
        .shutdown(Shutdown::graceful_for(Duration::from_secs(5)))
        .mailbox_shutdown(MailboxShutdown::Discard),
    );
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();

    started.notified().await;
    actor.send("mailbox").await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
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
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn on_start_context_stop_with_drain_handles_the_queued_mailbox_only() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut graph = Tree::new();
    let actor = graph.add_actor_spec(
        ActorSpec::new("StopsOnStart", {
            let started = started.clone();
            let release = release.clone();
            let events = events.clone();
            move || StopsOnStart {
                started: started.clone(),
                release: release.clone(),
                events: events.clone(),
            }
        })
        .shutdown(Shutdown::graceful_for(Duration::from_secs(5))),
    );
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();

    started.notified().await;
    actor.send("mailbox").await.unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
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
    handle.shutdown().await.unwrap();
}

#[derive(Clone)]
struct PromptRaw;

impl RawActor for PromptRaw {
    type Msg = ();

    async fn run(&mut self, _ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

struct ExitCleanupProbe {
    self_ref: Arc<std::sync::Mutex<Option<ActorRef<()>>>>,
    intake_closed: Arc<AtomicBool>,
    dropped: Arc<Notify>,
}

impl RawActor for ExitCleanupProbe {
    type Msg = ();

    async fn run(&mut self, _ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

impl Drop for ExitCleanupProbe {
    fn drop(&mut self) {
        let intake_closed = matches!(
            self.self_ref
                .lock()
                .unwrap()
                .as_ref()
                .expect("the actor ref is installed before spawning")
                .try_send(()),
            Err(SendError {
                kind: SendErrorKind::NotRunning,
                ..
            })
        );
        self.intake_closed.store(intake_closed, Ordering::SeqCst);
        self.dropped.notify_one();
    }
}

#[tokio::test]
async fn raw_actor_exit_closes_external_intake_before_dropping_the_actor() {
    let self_ref = Arc::new(std::sync::Mutex::new(None));
    let intake_closed = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let actor = graph.add_actor_spec(ActorSpec::new("ExitCleanupProbe", {
        let self_ref = Arc::clone(&self_ref);
        let intake_closed = Arc::clone(&intake_closed);
        let dropped = Arc::clone(&dropped);
        move || ExitCleanupProbe {
            self_ref: Arc::clone(&self_ref),
            intake_closed: Arc::clone(&intake_closed),
            dropped: Arc::clone(&dropped),
        }
    }));
    *self_ref.lock().unwrap() = Some(actor);

    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), dropped.notified())
        .await
        .expect("the raw actor is dropped after returning");

    assert!(intake_closed.load(Ordering::SeqCst));
    handle.shutdown().await.unwrap();
}

struct ReceivesOneRaw {
    runs: Arc<AtomicUsize>,
    received: mpsc::UnboundedSender<(usize, &'static str)>,
}

impl RawActor for ReceivesOneRaw {
    type Msg = &'static str;

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        let run = self.runs.fetch_add(1, Ordering::SeqCst);
        ctx.mark_ready();
        let message = ctx.recv().await.expect("outer raw run remains active");
        self.received.send((run, message)).unwrap();
        Ok(())
    }
}

struct ReenteringRawActor {
    inner: ReceivesOneRaw,
    between_runs: mpsc::UnboundedSender<()>,
    resume: Arc<Notify>,
    finished: Arc<Notify>,
}

impl RawActor for ReenteringRawActor {
    type Msg = &'static str;

    fn manual_readiness(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        self.inner.run(ctx).await?;
        self.between_runs.send(()).unwrap();
        self.resume.notified().await;
        self.inner.run(ctx).await?;
        self.finished.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn raw_actor_decorator_can_reenter_with_the_same_open_context() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (between_runs, mut between_runs_rx) = mpsc::unbounded_channel();
    let resume = Arc::new(Notify::new());
    let (received, mut received_rx) = mpsc::unbounded_channel();
    let finished = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let actor = graph.add_actor_spec(ActorSpec::new("ReenteringRawActor", {
        let runs = Arc::clone(&runs);
        let between_runs = between_runs.clone();
        let resume = Arc::clone(&resume);
        let received = received.clone();
        let finished = Arc::clone(&finished);
        move || ReenteringRawActor {
            inner: ReceivesOneRaw {
                runs: Arc::clone(&runs),
                received: received.clone(),
            },
            between_runs: between_runs.clone(),
            resume: Arc::clone(&resume),
            finished: Arc::clone(&finished),
        }
    }));
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
        .await
        .unwrap()
        .unwrap();
    actor.send("first").await.unwrap();
    between_runs_rx.recv().await.expect("the first run returns");
    assert_eq!(received_rx.recv().await, Some((0, "first")));
    actor
        .send("between")
        .await
        .expect("the outer raw run keeps external intake open");
    resume.notify_one();
    assert_eq!(received_rx.recv().await, Some((1, "between")));
    tokio::time::timeout(Duration::from_secs(1), finished.notified())
        .await
        .expect("the second run returns");
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn prompt_raw_actor_delivers_readiness_before_completion() {
    let mut graph = Tree::new();
    graph.add_actor_spec(ActorSpec::new("PromptRaw", || PromptRaw));
    let handle = graph
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle.scope().wait_started())
        .await
        .unwrap()
        .unwrap();
    assert!(
        handle.scope().snapshot().children[0]
            .state
            .last_exit()
            .is_some_and(|exit| exit.is_completed())
    );
    handle.shutdown().await.unwrap();
}

struct BoundedRawReadiness {
    attempts: Arc<AtomicUsize>,
}

impl RawActor for BoundedRawReadiness {
    type Msg = ();

    fn manual_readiness(&self) -> Option<Duration> {
        Some(Duration::from_millis(10))
    }

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            std::future::pending::<()>().await;
        }
        ctx.mark_ready();
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn raw_manual_readiness_timeout_restarts_then_accepts_mark_ready() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut graph = Tree::new();
    graph.add_actor_spec(ActorSpec::new("BoundedRawReadiness", {
        let attempts = Arc::clone(&attempts);
        move || BoundedRawReadiness {
            attempts: Arc::clone(&attempts),
        }
    }));

    let handle = graph.spawn().unwrap();
    handle.scope().wait_started().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let snapshot = handle.scope().snapshot();
    let previous_exit = snapshot
        .child("BoundedRawReadiness")
        .and_then(|child| child.state.last_exit())
        .expect("replacement retains the readiness timeout");
    assert_eq!(
        previous_exit.readiness_timeout(),
        Some(Duration::from_millis(10))
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
#[cfg(feature = "host")]
async fn ordinary_task_propagating_actor_timeout_remains_an_ordinary_failure() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let task = tree.add_task_spec(
        TaskSpec::new("host-wrapper", {
            let attempts = Arc::clone(&attempts);
            move |ctx| {
                let attempts = Arc::clone(&attempts);
                async move {
                    ctx.mark_ready();
                    ActorSpec::new("NestedHost", move || BoundedRawReadiness {
                        attempts: Arc::clone(&attempts),
                    })
                    .into_host()
                    .run_once(std::future::pending::<()>(), Shutdown::abort())
                    .await?;
                    Ok(())
                }
            }
        })
        .manual_readiness(Duration::from_secs(1))
        .restart(RestartPolicy::never()),
    );

    let running = tree.spawn().unwrap();
    task.wait_started()
        .await
        .expect("the wrapper task reports its own readiness");
    let exit = task
        .wait()
        .await
        .expect("the wrapper task exit is retained");

    assert_eq!(exit.readiness_timeout(), None);
    assert!(matches!(
        exit,
        ExitStatus::Failed { message, cancelled: false }
            if message.contains("actor `NestedHost` did not report readiness")
    ));
    running.shutdown().await.unwrap();
}

struct SlowShutdownRawReadiness {
    started: mpsc::UnboundedSender<()>,
}

impl RawActor for SlowShutdownRawReadiness {
    type Msg = ();

    fn manual_readiness(&self) -> Option<Duration> {
        Some(Duration::from_millis(10))
    }

    async fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> ExitResult {
        let _ = self.started.send(());
        ctx.shutdown_token().cancelled().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn raw_actor_shutdown_disarms_a_short_manual_readiness_deadline() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let mut graph = Tree::new();
    graph.add_actor_spec(
        ActorSpec::new("SlowShutdownRawReadiness", move || {
            SlowShutdownRawReadiness {
                started: started_tx.clone(),
            }
        })
        .shutdown(Shutdown::graceful_for(Duration::from_millis(50))),
    );

    let running = graph.spawn().unwrap();
    let scope = running.scope();
    started_rx.recv().await.expect("actor starts");
    running.shutdown().await.unwrap();

    let snapshot = scope.snapshot();
    let exit = snapshot
        .child("SlowShutdownRawReadiness")
        .and_then(|child| child.state.last_exit())
        .expect("shutdown records the pre-ready actor exit");
    assert!(matches!(exit, ExitStatus::Completed { cancelled: true }));
}

#[tokio::test(start_paused = true)]
#[cfg(feature = "host")]
async fn directly_hosted_raw_manual_readiness_timeout_is_typed() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let host = ActorSpec::new("BoundedRawReadiness", {
        let attempts = Arc::clone(&attempts);
        move || BoundedRawReadiness {
            attempts: Arc::clone(&attempts),
        }
    })
    .into_host();

    let error = host
        .run_once(std::future::pending::<()>(), Shutdown::abort())
        .await
        .expect_err("manual readiness expires");
    assert!(matches!(
        error,
        ActorRunError::ReadinessTimedOut {
            actor_id, timeout, ..
        }
            if actor_id == "BoundedRawReadiness" && timeout == Duration::from_millis(10)
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// Overrides nothing: exercises the [`Actor`] trait's default drain policy.
#[derive(Clone)]
struct DefaultPolicy {
    handled: Arc<Mutex<Vec<&'static str>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for DefaultPolicy {
    type Msg = &'static str;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.handled.lock().await.push(message);
        if message == "hold" {
            self.started.notify_one();
            self.release.notified().await;
            while !ctx.shutdown_token().is_cancelled() {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}

#[test]
fn the_shutdown_default_drains() {
    assert_eq!(
        Shutdown::default(),
        Shutdown::graceful_for(std::time::Duration::from_secs(5))
    );
}

/// A handler that never configures shutdown finishes the mailbox its
/// incarnation already accepted. Flipping this default back to `Discard` is a
/// silent message-loss change, so it is pinned here rather than left to the
/// tests that set a policy explicitly.
#[tokio::test]
async fn an_actor_that_sets_no_policy_drains_its_queued_mailbox() {
    let handled = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut graph = Tree::new();
    let actor_handled = handled.clone();
    let actor_started = started.clone();
    let actor_release = release.clone();
    let actor = graph.add_actor_spec(ActorSpec::new("DefaultPolicy", move || DefaultPolicy {
        handled: actor_handled.clone(),
        started: actor_started.clone(),
        release: actor_release.clone(),
    }));
    let handle = graph.spawn().unwrap();
    handle.scope().wait_started().await.unwrap();

    // Park the handler so the next sends land in the mailbox rather than being
    // consumed by the ordinary receive loop.
    actor.send("hold").await.unwrap();
    started.notified().await;
    actor.send("queued-first").await.unwrap();
    actor.send("queued-second").await.unwrap();

    handle.scope().request_shutdown();
    release.notify_one();
    handle.shutdown().await.unwrap();

    assert_eq!(
        &*handled.lock().await,
        &["hold", "queued-first", "queued-second"],
        "the default policy dropped messages the mailbox had already accepted"
    );
}
