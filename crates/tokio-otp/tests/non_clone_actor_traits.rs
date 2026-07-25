use std::{
    cell::Cell,
    future::pending,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_otp::{
    Actor, ActorContext, ActorFactory, ActorResult, GraphBuilder, RawActor, Reply, RestartPolicy,
    Runtime, prelude::Continue,
};

struct HandlerWithNonCloneState {
    _state: Mutex<()>,
}

impl Actor for HandlerWithNonCloneState {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut ActorContext<()>) -> ActorResult {
        Ok(Continue)
    }
}

struct HandlerWithSendOnlyMessage;

impl Actor for HandlerWithSendOnlyMessage {
    type Msg = Cell<usize>;

    async fn handle(
        &mut self,
        message: Self::Msg,
        ctx: &mut ActorContext<Self::Msg>,
    ) -> ActorResult {
        let value = ctx.run_blocking(move |_| message.get()).await;
        ctx.continue_with(Cell::new(value));
        Ok(Continue)
    }
}

struct RawWithNonCloneState {
    _state: Mutex<()>,
}

impl RawActor for RawWithNonCloneState {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<()>) -> ActorResult {
        Ok(Continue)
    }
}

fn assert_actor<T: Actor>() {}
fn assert_raw_actor<T: RawActor>() {}

#[test]
fn actor_traits_accept_non_clone_state() {
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

    async fn handle(
        &mut self,
        message: ProbeMsg,
        _ctx: &mut ActorContext<ProbeMsg>,
    ) -> ActorResult {
        match message {
            ProbeMsg::Increment(reply) => {
                self.local += 1;
                reply.send((self.incarnation, self.local));
                Ok(Continue)
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
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor(
        "handler",
        NonCloneHandlerFactory {
            constructions: constructions.clone(),
        },
    );
    let handle = Runtime::builder()
        .graph(builder.build().expect("graph builds"))
        .restart(RestartPolicy::OnFailure)
        .build()
        .expect("runtime builds")
        .spawn();

    assert_eq!(
        actor_ref
            .call(Duration::from_secs(1), ProbeMsg::Increment)
            .await
            .expect("first incarnation replies"),
        (0, 1)
    );
    let restarted = handle
        .supervisor_handle()
        .monitor_restart("handler")
        .expect("restart monitor exists");
    actor_ref
        .send(ProbeMsg::Crash)
        .await
        .expect("crash accepted");
    tokio::time::timeout(Duration::from_secs(1), restarted)
        .await
        .expect("restart observed")
        .expect("restart succeeds");
    assert_eq!(
        actor_ref
            .call(Duration::from_secs(1), ProbeMsg::Increment)
            .await
            .expect("replacement replies"),
        (1, 1)
    );
    assert_eq!(constructions.load(Ordering::SeqCst), 2);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

struct NonCloneRaw {
    _guard: Mutex<()>,
    incarnation: usize,
    observed: mpsc::UnboundedSender<(usize, usize)>,
}

impl RawActor for NonCloneRaw {
    type Msg = bool;

    async fn run(&mut self, mut ctx: ActorContext<bool>) -> ActorResult {
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
        Ok(Continue)
    }
}

#[tokio::test]
async fn non_clone_raw_actor_factory_is_reused_for_restart() {
    let constructions = Arc::new(AtomicUsize::new(0));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor("raw", {
        let constructions = constructions.clone();
        move || NonCloneRaw {
            _guard: Mutex::new(()),
            incarnation: constructions.fetch_add(1, Ordering::SeqCst),
            observed: observed_tx.clone(),
        }
    });
    let handle = Runtime::builder()
        .graph(builder.build().expect("graph builds"))
        .restart(RestartPolicy::OnFailure)
        .build()
        .expect("runtime builds")
        .spawn();

    actor_ref.send(false).await.expect("first message accepted");
    assert_eq!(observed_rx.recv().await, Some((0, 1)));
    let restarted = handle
        .supervisor_handle()
        .monitor_restart("raw")
        .expect("restart monitor exists");
    actor_ref.send(true).await.expect("crash accepted");
    tokio::time::timeout(Duration::from_secs(1), restarted)
        .await
        .expect("restart observed")
        .expect("restart succeeds");
    actor_ref
        .send(false)
        .await
        .expect("replacement message accepted");
    assert_eq!(observed_rx.recv().await, Some((1, 1)));
    assert_eq!(constructions.load(Ordering::SeqCst), 2);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn constructor_panic_uses_the_actor_panic_path() {
    struct PanickingFactory;

    impl ActorFactory for PanickingFactory {
        type Actor = RawWithNonCloneState;

        fn build(&self) -> Self::Actor {
            panic!("constructor panic")
        }
    }

    let mut builder = GraphBuilder::new();
    builder.actor("panics", PanickingFactory);
    let graph = builder.build().expect("registration does not construct");
    let actor = graph.actors()[0].clone();

    let joined =
        tokio::spawn(async move { actor.run_until(pending::<()>(), RestartPolicy::Never).await })
            .await;
    assert!(joined.expect_err("constructor panic propagates").is_panic());
}

#[derive(Default)]
struct DefaultActor;

impl Actor for DefaultActor {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut ActorContext<()>) -> ActorResult {
        Ok(Continue)
    }
}

#[tokio::test]
async fn default_constructor_path_is_an_actor_factory() {
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.add(DefaultActor::default);
    let handle = Runtime::builder()
        .graph(builder.build().expect("graph builds"))
        .build()
        .expect("runtime builds")
        .spawn();

    handle.wait_started().await.expect("actor starts");
    actor_ref.send(()).await.expect("default actor is running");
    handle.shutdown_and_wait().await.expect("clean shutdown");
}
