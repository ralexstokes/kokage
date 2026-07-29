use std::{
    convert::Infallible,
    future::pending,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorResult, ActorStatus, DrainPolicy, GraphBuilder, MessageContext, OrderedTree,
    StartContext, StopContext, TaskHandle,
};
use tokio::sync::{Notify, mpsc};

const WAIT: Duration = Duration::from_secs(3);

async fn wait_for<T>(receiver: &mut mpsc::UnboundedReceiver<T>, phase: &str) -> T {
    tokio::time::timeout(WAIT, receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {phase}"))
        .unwrap_or_else(|| panic!("channel closed while waiting for {phase}"))
}

enum ReadyMsg {
    ScopeStarted(Result<(), kokage_supervisor::SupervisorError>),
}

struct ReadyReporter {
    report: mpsc::UnboundedSender<bool>,
}

impl Actor for ReadyReporter {
    type Msg = ReadyMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        ctx.spawn_scope_wait(
            &scope,
            |handle| async move { handle.wait_started().await },
            ReadyMsg::ScopeStarted,
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let ReadyMsg::ScopeStarted(result) = message;
        self.report.send(result.is_ok()).expect("receiver open");
        Ok(())
    }
}

#[tokio::test]
async fn scope_wait_maps_completion_through_the_actor_mailbox() {
    let (report, mut reports) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (slot, actor) = graph.slot("reporter");
    graph.define(slot, move || ReadyReporter {
        report: report.clone(),
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    assert!(wait_for(&mut reports, "mapped scope-start result").await);
    assert_eq!(actor.stats().messages_accepted, 1);
    assert_eq!(actor.stats().messages_received, 1);

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

struct DropSignal(mpsc::UnboundedSender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

enum PendingMsg {
    Completed,
}

struct PendingWait {
    started: mpsc::UnboundedSender<()>,
    dropped: mpsc::UnboundedSender<()>,
    drain_policy: DrainPolicy,
}

impl Actor for PendingWait {
    type Msg = PendingMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        let started = self.started.clone();
        let dropped = self.dropped.clone();
        ctx.spawn_scope_wait(
            &scope,
            move |_handle| async move {
                let _drop_signal = DropSignal(dropped);
                started.send(()).expect("receiver open");
                pending::<()>().await;
            },
            |()| PendingMsg::Completed,
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        _message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        panic!("a pending scope wait must not complete")
    }

    fn drain_policy(&self) -> DrainPolicy {
        self.drain_policy
    }
}

async fn assert_pending_scope_wait_is_cancelled(drain_policy: DrainPolicy) {
    let (started, mut starts) = mpsc::unbounded_channel();
    let (dropped, mut drops) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("pending");
    graph.define(slot, move || PendingWait {
        started: started.clone(),
        dropped: dropped.clone(),
        drain_policy,
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    wait_for(&mut starts, "scope wait to start").await;
    tokio::time::timeout(WAIT, runtime.shutdown_and_wait())
        .await
        .expect("pending wait is not drained")
        .expect("clean shutdown");
    wait_for(&mut drops, "scope wait cancellation").await;
}

#[tokio::test]
async fn pending_scope_wait_is_cancelled_and_exempt_from_drain() {
    assert_pending_scope_wait_is_cancelled(DrainPolicy::Drain).await;
}

#[tokio::test]
async fn pending_scope_wait_is_cancelled_before_discard_shutdown() {
    assert_pending_scope_wait_is_cancelled(DrainPolicy::Discard).await;
}

enum CancelMsg {
    Start,
    Completed,
}

struct CancellableWait {
    started: mpsc::UnboundedSender<()>,
    dropped: mpsc::UnboundedSender<()>,
    handles: mpsc::UnboundedSender<TaskHandle>,
    completions: mpsc::UnboundedSender<()>,
}

impl Actor for CancellableWait {
    type Msg = CancelMsg;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            CancelMsg::Start => {
                let scope = ctx.supervisor();
                let started = self.started.clone();
                let dropped = self.dropped.clone();
                let handle = ctx.spawn_scope_wait(
                    &scope,
                    move |_handle| async move {
                        let _drop_signal = DropSignal(dropped);
                        started.send(()).expect("receiver open");
                        pending::<()>().await;
                    },
                    |()| CancelMsg::Completed,
                );
                self.handles.send(handle).expect("handle receiver open");
            }
            CancelMsg::Completed => {
                self.completions.send(()).expect("receiver open");
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn message_context_scope_wait_can_be_cancelled_and_is_accounted() {
    let (started, mut starts) = mpsc::unbounded_channel();
    let (dropped, mut drops) = mpsc::unbounded_channel();
    let (handles, mut handle_rx) = mpsc::unbounded_channel();
    let (completions, mut completion_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (slot, actor) = graph.slot("cancellable-wait");
    graph.define(slot, move || CancellableWait {
        started: started.clone(),
        dropped: dropped.clone(),
        handles: handles.clone(),
        completions: completions.clone(),
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    actor.send(CancelMsg::Start).await.expect("actor is live");
    let handle = wait_for(&mut handle_rx, "scope-wait cancellation handle").await;
    wait_for(&mut starts, "message-context scope wait to start").await;
    assert_eq!(actor.stats().outstanding_scope_waits, 1);

    handle.abort();
    wait_for(&mut drops, "explicit scope-wait cancellation").await;
    tokio::time::timeout(WAIT, async {
        while actor.stats().outstanding_scope_waits != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled scope wait is reaped");
    assert!(handle.is_finished());
    assert!(completion_rx.try_recv().is_err());

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

enum BackpressureMsg {
    Block,
    Filler,
    ScopeReady,
}

struct BackpressureWait {
    wait_gate: Arc<Notify>,
    wait_started: mpsc::UnboundedSender<()>,
    mapped: mpsc::UnboundedSender<()>,
    handler_started: mpsc::UnboundedSender<()>,
    handler_release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
    handles: mpsc::UnboundedSender<TaskHandle>,
}

impl Actor for BackpressureWait {
    type Msg = BackpressureMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        let wait_gate = Arc::clone(&self.wait_gate);
        let wait_started = self.wait_started.clone();
        let mapped = self.mapped.clone();
        let handle = ctx.spawn_scope_wait(
            &scope,
            move |_handle| async move {
                wait_started.send(()).expect("receiver open");
                wait_gate.notified().await;
            },
            move |()| {
                mapped.send(()).expect("receiver open");
                BackpressureMsg::ScopeReady
            },
        );
        self.handles.send(handle).expect("handle receiver open");
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            BackpressureMsg::Block => {
                self.handler_started.send(()).expect("receiver open");
                self.handler_release.notified().await;
            }
            BackpressureMsg::Filler => self.observed.send("filler").expect("receiver open"),
            BackpressureMsg::ScopeReady => self.observed.send("scope").expect("receiver open"),
        }
        Ok(())
    }
}

#[tokio::test]
async fn scope_wait_completion_obeys_full_fifo_mailbox_backpressure() {
    let wait_gate = Arc::new(Notify::new());
    let handler_release = Arc::new(Notify::new());
    let (wait_started, mut wait_starts) = mpsc::unbounded_channel();
    let (mapped, mut mapped_rx) = mpsc::unbounded_channel();
    let (handler_started, mut handler_starts) = mpsc::unbounded_channel();
    let (observed, mut observed_rx) = mpsc::unbounded_channel();
    let (handles, mut handle_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (slot, actor) = graph.slot("backpressured-wait");
    graph.define(slot, {
        let wait_gate = Arc::clone(&wait_gate);
        let handler_release = Arc::clone(&handler_release);
        move || BackpressureWait {
            wait_gate: Arc::clone(&wait_gate),
            wait_started: wait_started.clone(),
            mapped: mapped.clone(),
            handler_started: handler_started.clone(),
            handler_release: Arc::clone(&handler_release),
            observed: observed.clone(),
            handles: handles.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    let _wait_handle = wait_for(&mut handle_rx, "scope-wait handle").await;
    wait_for(&mut wait_starts, "scope wait to start").await;
    actor
        .send(BackpressureMsg::Block)
        .await
        .expect("block message accepted");
    wait_for(&mut handler_starts, "blocking handler").await;
    actor
        .send(BackpressureMsg::Filler)
        .await
        .expect("filler occupies mailbox");
    wait_gate.notify_one();
    wait_for(&mut mapped_rx, "scope-wait mapper").await;

    let stats = actor.stats();
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.messages_accepted, 2);
    assert_eq!(stats.outstanding_scope_waits, 1);
    assert!(observed_rx.try_recv().is_err());

    handler_release.notify_one();
    assert_eq!(wait_for(&mut observed_rx, "FIFO filler").await, "filler");
    assert_eq!(
        wait_for(&mut observed_rx, "scope completion").await,
        "scope"
    );
    tokio::time::timeout(WAIT, async {
        while actor.stats().outstanding_scope_waits != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed scope wait is reaped");
    assert_eq!(actor.stats().messages_accepted, 3);

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn handle_abort_wins_before_a_blocked_scope_wait_message_is_accepted() {
    let wait_gate = Arc::new(Notify::new());
    let handler_release = Arc::new(Notify::new());
    let (wait_started, mut wait_starts) = mpsc::unbounded_channel();
    let (mapped, mut mapped_rx) = mpsc::unbounded_channel();
    let (handler_started, mut handler_starts) = mpsc::unbounded_channel();
    let (observed, mut observed_rx) = mpsc::unbounded_channel();
    let (handles, mut handle_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (slot, actor) = graph.slot("cancelled-backpressured-wait");
    graph.define(slot, {
        let wait_gate = Arc::clone(&wait_gate);
        let handler_release = Arc::clone(&handler_release);
        move || BackpressureWait {
            wait_gate: Arc::clone(&wait_gate),
            wait_started: wait_started.clone(),
            mapped: mapped.clone(),
            handler_started: handler_started.clone(),
            handler_release: Arc::clone(&handler_release),
            observed: observed.clone(),
            handles: handles.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    let wait_handle = wait_for(&mut handle_rx, "scope-wait handle").await;
    wait_for(&mut wait_starts, "scope wait to start").await;
    actor
        .send(BackpressureMsg::Block)
        .await
        .expect("block message accepted");
    wait_for(&mut handler_starts, "blocking handler").await;
    actor
        .send(BackpressureMsg::Filler)
        .await
        .expect("filler occupies mailbox");
    wait_gate.notify_one();
    wait_for(&mut mapped_rx, "scope-wait mapper").await;
    assert_eq!(actor.stats().mailbox_depth, 1);

    wait_handle.abort();
    tokio::time::timeout(WAIT, async {
        while !wait_handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("blocked scope-wait send is cancelled and reaped");
    handler_release.notify_one();

    assert_eq!(wait_for(&mut observed_rx, "FIFO filler").await, "filler");
    tokio::time::timeout(WAIT, async {
        while actor.stats().outstanding_scope_waits != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled scope wait is reaped after the handler returns");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observed_rx.recv())
            .await
            .is_err(),
        "a mapped message was accepted after its handle won cancellation"
    );
    assert_eq!(actor.stats().messages_accepted, 2);

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

enum RestartMsg {
    Crash,
    OldWaitCompleted,
}

struct RestartProbe {
    incarnation: usize,
    starts: mpsc::UnboundedSender<usize>,
    wait_started: mpsc::UnboundedSender<()>,
    wait_dropped: mpsc::UnboundedSender<()>,
    wait_gate: Arc<Notify>,
    stale_completions: mpsc::UnboundedSender<()>,
}

impl Actor for RestartProbe {
    type Msg = RestartMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.starts
            .send(self.incarnation)
            .expect("start receiver open");
        if self.incarnation == 0 {
            let scope = ctx.supervisor();
            let wait_started = self.wait_started.clone();
            let wait_dropped = self.wait_dropped.clone();
            let wait_gate = Arc::clone(&self.wait_gate);
            ctx.spawn_scope_wait(
                &scope,
                move |_handle| async move {
                    let _drop_signal = DropSignal(wait_dropped);
                    wait_started.send(()).expect("wait receiver open");
                    wait_gate.notified().await;
                },
                |()| RestartMsg::OldWaitCompleted,
            );
        }
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            RestartMsg::Crash => Err(io::Error::other("scripted restart").into()),
            RestartMsg::OldWaitCompleted => {
                self.stale_completions
                    .send(())
                    .expect("stale-completion receiver open");
                Ok(())
            }
        }
    }
}

#[tokio::test]
async fn restart_cancels_pending_scope_wait_without_delivering_to_the_next_incarnation() {
    let incarnations = Arc::new(AtomicUsize::new(0));
    let (starts, mut start_reports) = mpsc::unbounded_channel();
    let (wait_started, mut wait_starts) = mpsc::unbounded_channel();
    let (wait_dropped, mut wait_drops) = mpsc::unbounded_channel();
    let (stale_completions, mut stale_reports) = mpsc::unbounded_channel();
    let wait_gate = Arc::new(Notify::new());
    let mut graph = GraphBuilder::new();
    let (slot, actor) = graph.slot("restart-probe");
    graph.define(slot, {
        let incarnations = Arc::clone(&incarnations);
        let starts = starts.clone();
        let wait_started = wait_started.clone();
        let wait_dropped = wait_dropped.clone();
        let wait_gate = Arc::clone(&wait_gate);
        let stale_completions = stale_completions.clone();
        move || RestartProbe {
            incarnation: incarnations.fetch_add(1, Ordering::SeqCst),
            starts: starts.clone(),
            wait_started: wait_started.clone(),
            wait_dropped: wait_dropped.clone(),
            wait_gate: Arc::clone(&wait_gate),
            stale_completions: stale_completions.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    assert_eq!(wait_for(&mut start_reports, "first incarnation").await, 0);
    wait_for(&mut wait_starts, "first incarnation scope wait").await;
    actor
        .send(RestartMsg::Crash)
        .await
        .expect("first incarnation receives crash");
    assert_eq!(wait_for(&mut start_reports, "second incarnation").await, 1);
    wait_for(&mut wait_drops, "old incarnation scope wait cancellation").await;

    wait_gate.notify_waiters();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stale_reports.recv())
            .await
            .is_err(),
        "the old incarnation's mapped completion reached the new incarnation"
    );

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

enum CompletionRaceMsg {
    Crash,
    Filler,
    OldWaitCompleted,
}

struct CompletionRaceProbe {
    incarnation: usize,
    starts: mpsc::UnboundedSender<usize>,
    wait_started: mpsc::UnboundedSender<()>,
    wait_gate: Arc<Notify>,
    mapped: Arc<Notify>,
    stale_completions: mpsc::UnboundedSender<()>,
}

impl Actor for CompletionRaceProbe {
    type Msg = CompletionRaceMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.starts
            .send(self.incarnation)
            .expect("start receiver open");
        if self.incarnation == 0 {
            let scope = ctx.supervisor();
            let myself = ctx.myself();
            let wait_started = self.wait_started.clone();
            let wait_gate = Arc::clone(&self.wait_gate);
            let mapped = Arc::clone(&self.mapped);
            ctx.spawn_scope_wait(
                &scope,
                move |_handle| async move {
                    wait_started.send(()).expect("wait receiver open");
                    wait_gate.notified().await;
                    myself
                        .send(CompletionRaceMsg::Crash)
                        .await
                        .expect("crash message accepted");
                    myself
                        .send(CompletionRaceMsg::Filler)
                        .await
                        .expect("filler occupies old mailbox");
                },
                move |()| {
                    mapped.notify_one();
                    CompletionRaceMsg::OldWaitCompleted
                },
            );
        }
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            CompletionRaceMsg::Crash => {
                self.mapped.notified().await;
                Err(io::Error::other("restart immediately after wait completion").into())
            }
            CompletionRaceMsg::Filler => panic!("old mailbox filler must be discarded on restart"),
            CompletionRaceMsg::OldWaitCompleted => {
                self.stale_completions
                    .send(())
                    .expect("stale-completion receiver open");
                Ok(())
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_racing_restart_cannot_follow_the_ref_into_the_next_incarnation() {
    let incarnations = Arc::new(AtomicUsize::new(0));
    let wait_gate = Arc::new(Notify::new());
    let mapped = Arc::new(Notify::new());
    let (starts, mut start_reports) = mpsc::unbounded_channel();
    let (wait_started, mut wait_starts) = mpsc::unbounded_channel();
    let (stale_completions, mut stale_reports) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (slot, actor) = graph.slot("completion-race");
    graph.define(slot, {
        let incarnations = Arc::clone(&incarnations);
        let wait_gate = Arc::clone(&wait_gate);
        let mapped = Arc::clone(&mapped);
        move || CompletionRaceProbe {
            incarnation: incarnations.fetch_add(1, Ordering::SeqCst),
            starts: starts.clone(),
            wait_started: wait_started.clone(),
            wait_gate: Arc::clone(&wait_gate),
            mapped: Arc::clone(&mapped),
            stale_completions: stale_completions.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    assert_eq!(wait_for(&mut start_reports, "first incarnation").await, 0);
    wait_for(&mut wait_starts, "racing scope wait").await;
    assert_eq!(actor.stats().outstanding_scope_waits, 1);
    wait_gate.notify_one();
    assert_eq!(
        wait_for(&mut start_reports, "restarted incarnation").await,
        1
    );
    assert_eq!(actor.stats().outstanding_scope_waits, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stale_reports.recv())
            .await
            .is_err(),
        "the old completion followed the stable ref into the new incarnation"
    );

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Clone, Copy)]
enum PanicSite {
    Future,
    Mapper,
}

struct PanicOnce {
    panic_site: Option<PanicSite>,
}

impl Actor for PanicOnce {
    type Msg = Infallible;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        match self.panic_site {
            Some(PanicSite::Future) => {
                ctx.spawn_scope_wait(
                    &scope,
                    |_handle| async {
                        panic!("scripted scope-wait future panic");
                        #[allow(unreachable_code)]
                        ()
                    },
                    |()| -> Infallible { unreachable!("panicking future cannot complete") },
                );
            }
            Some(PanicSite::Mapper) => {
                ctx.spawn_scope_wait(
                    &scope,
                    |_handle| async {},
                    |()| -> Infallible { panic!("scripted scope-wait mapper panic") },
                );
            }
            None => {}
        }
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {}
    }
}

async fn assert_scope_wait_panic_is_supervised(panic_site: PanicSite, phase: &str) {
    let incarnations = Arc::new(AtomicUsize::new(0));
    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("panic-once");
    graph.define(slot, {
        let incarnations = incarnations.clone();
        move || PanicOnce {
            panic_site: (incarnations.fetch_add(1, Ordering::SeqCst) == 0).then_some(panic_site),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = runtime.handle().snapshot();
            let child = snapshot.child("panic-once").expect("child exists");
            if child.generation >= 1 {
                assert!(
                    child
                        .state
                        .last_exit()
                        .is_some_and(|exit| exit.is_panicked())
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{phase} triggers a supervised restart"));
    assert!(incarnations.load(Ordering::SeqCst) >= 2);

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn scope_wait_mapper_panic_fails_the_actor_and_is_supervised() {
    assert_scope_wait_panic_is_supervised(PanicSite::Mapper, "mapper panic").await;
}

#[tokio::test]
async fn scope_wait_future_panic_fails_the_actor_and_is_supervised() {
    assert_scope_wait_panic_is_supervised(PanicSite::Future, "wait-future panic").await;
}

struct CompletedPanicDuringDrain {
    stopped: mpsc::UnboundedSender<()>,
}

impl Actor for CompletedPanicDuringDrain {
    type Msg = Infallible;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        let handle = ctx.spawn_scope_wait(
            &scope,
            |_handle| async {
                panic!("completed scope-wait panic must be discarded at stop");
                #[allow(unreachable_code)]
                ()
            },
            |()| -> Infallible { unreachable!("panicking wait cannot map a message") },
        );
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        ctx.stop();
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {}
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> ActorResult {
        self.stopped.send(()).expect("receiver open");
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_discards_an_unobserved_completed_scope_wait_panic() {
    let (stopped, mut stops) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    let (slot, _) = graph.slot("completed-panic-drain");
    graph.define(slot, move || CompletedPanicDuringDrain {
        stopped: stopped.clone(),
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    wait_for(&mut stops, "clean on_stop after completed wait panic").await;
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

enum MixedDrainMsg {
    Begin,
    Filler,
    OffloadDone,
    ScopeDone,
}

struct MixedDrain {
    scope_started: mpsc::UnboundedSender<()>,
    scope_release: Arc<Notify>,
    scope_mapped: mpsc::UnboundedSender<()>,
    begin_started: mpsc::UnboundedSender<()>,
    allow_stop: Arc<Notify>,
    offload_started: mpsc::UnboundedSender<()>,
    offload_release: Arc<Notify>,
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for MixedDrain {
    type Msg = MixedDrainMsg;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        let scope = ctx.supervisor();
        let scope_started = self.scope_started.clone();
        let scope_release = Arc::clone(&self.scope_release);
        let scope_mapped = self.scope_mapped.clone();
        ctx.spawn_scope_wait(
            &scope,
            move |_handle| async move {
                scope_started.send(()).expect("receiver open");
                scope_release.notified().await;
            },
            move |()| {
                scope_mapped.send(()).expect("receiver open");
                MixedDrainMsg::ScopeDone
            },
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            MixedDrainMsg::Begin => {
                let offload_started = self.offload_started.clone();
                let offload_release = Arc::clone(&self.offload_release);
                ctx.offload(
                    WAIT,
                    async move {
                        offload_started.send(()).expect("receiver open");
                        offload_release.notified().await;
                    },
                    |_| MixedDrainMsg::OffloadDone,
                );
                self.begin_started.send(()).expect("receiver open");
                self.allow_stop.notified().await;
                ctx.stop();
            }
            MixedDrainMsg::Filler => {
                assert_eq!(ctx.status(), ActorStatus::Draining);
                self.observed.send("filler").expect("receiver open");
            }
            MixedDrainMsg::OffloadDone => {
                assert_eq!(ctx.status(), ActorStatus::Draining);
                self.observed.send("offload").expect("receiver open");
            }
            MixedDrainMsg::ScopeDone => {
                panic!("blocked scope-wait delivery must be cancelled before drain")
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> ActorResult {
        self.observed.send("stop").expect("receiver open");
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}

#[tokio::test]
async fn drain_aborts_scope_waits_but_still_waits_for_offloads() {
    let (scope_started, mut scope_starts) = mpsc::unbounded_channel();
    let scope_release = Arc::new(Notify::new());
    let (scope_mapped, mut scope_maps) = mpsc::unbounded_channel();
    let (begin_started, mut begin_starts) = mpsc::unbounded_channel();
    let allow_stop = Arc::new(Notify::new());
    let (offload_started, mut offload_starts) = mpsc::unbounded_channel();
    let offload_release = Arc::new(Notify::new());
    let (observed, mut observed_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();
    graph.mailbox_capacity(1);
    let (slot, actor) = graph.slot("mixed-drain");
    graph.define(slot, {
        let offload_release = Arc::clone(&offload_release);
        let scope_release = Arc::clone(&scope_release);
        let allow_stop = Arc::clone(&allow_stop);
        move || MixedDrain {
            scope_started: scope_started.clone(),
            scope_release: Arc::clone(&scope_release),
            scope_mapped: scope_mapped.clone(),
            begin_started: begin_started.clone(),
            allow_stop: Arc::clone(&allow_stop),
            offload_started: offload_started.clone(),
            offload_release: Arc::clone(&offload_release),
            observed: observed.clone(),
        }
    });
    let runtime = OrderedTree::graph(graph.build().expect("graph builds"))
        .spawn()
        .expect("tree builds");

    wait_for(&mut scope_starts, "mixed-drain scope wait").await;
    actor
        .send(MixedDrainMsg::Begin)
        .await
        .expect("begin drain message accepted");
    wait_for(&mut begin_starts, "mixed-drain handler").await;
    wait_for(&mut offload_starts, "mixed-drain offload").await;
    actor
        .send(MixedDrainMsg::Filler)
        .await
        .expect("filler occupies mailbox before stop");
    scope_release.notify_one();
    wait_for(&mut scope_maps, "mixed-drain scope mapper").await;
    assert_eq!(actor.stats().mailbox_depth, 1);
    assert_eq!(actor.stats().outstanding_scope_waits, 1);

    allow_stop.notify_one();
    assert_eq!(wait_for(&mut observed_rx, "drained filler").await, "filler");
    assert_eq!(actor.stats().outstanding_scope_waits, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observed_rx.recv())
            .await
            .is_err(),
        "drain accepted a cancelled scope result or finished before its offload"
    );
    assert_eq!(actor.stats().messages_accepted, 2);

    offload_release.notify_one();
    assert_eq!(
        wait_for(&mut observed_rx, "drained offload").await,
        "offload"
    );
    assert_eq!(wait_for(&mut observed_rx, "on_stop").await, "stop");
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}
