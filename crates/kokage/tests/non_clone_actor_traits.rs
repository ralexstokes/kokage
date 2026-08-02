use std::{
    cell::Cell,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorFactory, ActorSpec, Context, ExitResult, Reply, RestartPolicy, ScopeRef, Tree,
    observe::SupervisorSnapshotReceiver,
    raw::{RawActor, RawContext},
};
use tokio::sync::mpsc;
#[cfg(feature = "host")]
use {
    kokage::{Shutdown, raw::DEFAULT_SHUTDOWN_BOUND},
    std::future::pending,
};

fn restart_observer(handle: &ScopeRef, id: &str) -> (SupervisorSnapshotReceiver, u64) {
    let snapshots = handle.subscribe_snapshots();
    let child = handle
        .snapshot()
        .child(id)
        .expect("child exists")
        .generation;
    (snapshots, child)
}

async fn await_restart(mut snapshots: SupervisorSnapshotReceiver, id: &str, baseline: u64) {
    snapshots
        .wait_for_child(id, |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .expect("runtime remains live");
}

struct SendOnlyState {
    _not_sync: Cell<()>,
}

struct HandlerWithNonCloneState {
    _state: SendOnlyState,
}

impl Actor for HandlerWithNonCloneState {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct HandlerWithSendOnlyMessage;

impl Actor for HandlerWithSendOnlyMessage {
    type Msg = Cell<usize>;

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        let value = ctx.run_blocking(move |_| message.get()).await?;
        ctx.continue_with(Cell::new(value));
        Ok(())
    }
}

struct RawWithNonCloneState {
    _state: SendOnlyState,
}

impl RawActor for RawWithNonCloneState {
    type Msg = ();

    async fn run(&mut self, _ctx: RawContext<()>) -> ExitResult {
        Ok(())
    }
}

fn assert_actor<T: Actor>() {}
fn assert_raw_actor<T: RawActor>() {}

#[test]
fn actor_traits_accept_non_clone_send_only_state() {
    assert_actor::<HandlerWithNonCloneState>();
    assert_raw_actor::<HandlerWithNonCloneState>();
    assert_raw_actor::<RawWithNonCloneState>();
}

#[test]
fn mutable_handler_context_accepts_send_only_messages() {
    assert_actor::<HandlerWithSendOnlyMessage>();
}

enum ProbeMsg {
    Increment(Reply<(usize, usize)>),
    Crash,
}

struct NonCloneHandler {
    _guard: Mutex<()>,
    incarnation: usize,
    local: usize,
}

impl Actor for NonCloneHandler {
    type Msg = ProbeMsg;

    async fn handle(&mut self, message: ProbeMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ProbeMsg::Increment(reply) => {
                self.local += 1;
                reply.send((self.incarnation, self.local));
                Ok(())
            }
            ProbeMsg::Crash => Err(io::Error::other("restart probe").into()),
        }
    }
}

struct NonCloneHandlerFactory {
    constructions: Arc<AtomicUsize>,
}

impl ActorFactory for NonCloneHandlerFactory {
    type Actor = NonCloneHandler;

    fn build(&self) -> Self::Actor {
        NonCloneHandler {
            _guard: Mutex::new(()),
            incarnation: self.constructions.fetch_add(1, Ordering::SeqCst),
            local: 0,
        }
    }
}

#[tokio::test]
async fn non_clone_actor_factory_constructs_fresh_state_per_incarnation() {
    let constructions = Arc::new(AtomicUsize::new(0));
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new(
        "handler",
        NonCloneHandlerFactory {
            constructions: constructions.clone(),
        },
    ));
    let handle = builder
        .default_child_restart(RestartPolicy::on_failure())
        .spawn()
        .expect("runtime builds");

    assert_eq!(
        actor_ref
            .call(ProbeMsg::Increment, Duration::from_secs(1))
            .await
            .expect("first incarnation replies"),
        (0, 1)
    );
    let (lifecycle, baseline) = restart_observer(&handle.scope(), "handler");
    actor_ref
        .send(ProbeMsg::Crash)
        .await
        .expect("crash accepted");
    tokio::time::timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "handler", baseline),
    )
    .await
    .expect("restart observed");
    assert_eq!(
        actor_ref
            .call(ProbeMsg::Increment, Duration::from_secs(1))
            .await
            .expect("replacement replies"),
        (1, 1)
    );
    assert_eq!(constructions.load(Ordering::SeqCst), 2);

    handle.shutdown().await.expect("clean shutdown");
}

struct NonCloneRaw {
    _guard: Mutex<()>,
    incarnation: usize,
    observed: mpsc::UnboundedSender<(usize, usize)>,
}

impl RawActor for NonCloneRaw {
    type Msg = bool;

    async fn run(&mut self, mut ctx: RawContext<bool>) -> ExitResult {
        let mut local = 0;
        while let Some(crash) = ctx.recv().await {
            if crash {
                return Err(io::Error::other("restart probe").into());
            }
            local += 1;
            self.observed
                .send((self.incarnation, local))
                .expect("observer alive");
        }
        Ok(())
    }
}

#[tokio::test]
async fn non_clone_raw_actor_factory_is_reused_for_restart() {
    let constructions = Arc::new(AtomicUsize::new(0));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new("raw", {
        let constructions = constructions.clone();
        move || NonCloneRaw {
            _guard: Mutex::new(()),
            incarnation: constructions.fetch_add(1, Ordering::SeqCst),
            observed: observed_tx.clone(),
        }
    }));
    let handle = builder
        .default_child_restart(RestartPolicy::on_failure())
        .spawn()
        .expect("runtime builds");

    actor_ref.send(false).await.expect("first message accepted");
    assert_eq!(observed_rx.recv().await, Some((0, 1)));
    let (lifecycle, baseline) = restart_observer(&handle.scope(), "raw");
    actor_ref.send(true).await.expect("crash accepted");
    tokio::time::timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "raw", baseline),
    )
    .await
    .expect("restart observed");
    actor_ref
        .send(false)
        .await
        .expect("replacement message accepted");
    assert_eq!(observed_rx.recv().await, Some((1, 1)));
    assert_eq!(constructions.load(Ordering::SeqCst), 2);

    handle.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
#[cfg(feature = "host")]
async fn constructor_panic_uses_the_actor_panic_path() {
    struct PanickingFactory;

    impl ActorFactory for PanickingFactory {
        type Actor = RawWithNonCloneState;

        fn build(&self) -> Self::Actor {
            panic!("constructor panic")
        }
    }

    let actor = ActorSpec::new("panics", PanickingFactory).into_host();

    let joined = tokio::spawn(async move {
        actor
            .run_once(
                pending::<()>(),
                Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND),
            )
            .await
    })
    .await;
    assert!(joined.expect_err("constructor panic propagates").is_panic());
}

#[derive(Default)]
struct DefaultActor;

impl Actor for DefaultActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn default_constructor_path_is_an_actor_factory() {
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new("DefaultActor", DefaultActor::default));
    let handle = builder.spawn().expect("runtime builds");

    handle.scope().wait_started().await.expect("actor starts");
    actor_ref.send(()).await.expect("default actor is running");
    handle.shutdown().await.expect("clean shutdown");
}
