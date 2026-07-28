use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio_otp::{
    Actor, ActorFactory, ActorResult, GraphBuilder, LifecycleWatch, MessageContext, Reply,
    RestartPolicy, Runtime, RuntimeHandle, StartContext,
};

fn restart_observer(handle: &RuntimeHandle, id: &str) -> (LifecycleWatch, u64) {
    let lifecycle = handle.watch_lifecycle();
    let child = handle
        .snapshot()
        .child(id)
        .expect("child exists")
        .generation;
    (lifecycle, child)
}

async fn await_restart(mut lifecycle: LifecycleWatch, id: &str, baseline: u64) {
    lifecycle
        .started_after(&[], id, baseline)
        .await
        .expect("runtime remains live");
}

enum ProbeMsg {
    Increment(Reply<(usize, usize)>),
    Crash,
}

#[derive(tokio_otp::ActorFactory)]
struct DerivedActor {
    starts: Arc<AtomicUsize>,
    #[factory(default)]
    _non_clone: Mutex<()>,
    #[factory(default)]
    incarnation: usize,
    #[factory(default)]
    local: usize,
}

impl Actor for DerivedActor {
    type Msg = ProbeMsg;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.incarnation = self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
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

fn assert_factory<F: ActorFactory<Actor = DerivedActor> + Clone>() {}

#[tokio::test]
async fn derive_clones_durable_configuration_and_defaults_each_incarnation() {
    assert_factory::<DerivedActorFactory>();

    let starts = Arc::new(AtomicUsize::new(0));
    let mut builder = GraphBuilder::new();
    let (actor_ref_slot, actor_ref) = builder.slot("derived");
    builder.define(
        actor_ref_slot,
        DerivedActorFactory {
            starts: starts.clone(),
        },
    );
    let handle = Runtime::builder()
        .graph(builder.build().expect("graph builds"))
        .default_restart(RestartPolicy::OnFailure)
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
    assert_eq!(
        actor_ref
            .call(Duration::from_secs(1), ProbeMsg::Increment)
            .await
            .expect("first incarnation replies again"),
        (0, 2)
    );

    let (lifecycle, baseline) = restart_observer(&handle, "derived");
    actor_ref
        .send(ProbeMsg::Crash)
        .await
        .expect("crash accepted");
    tokio::time::timeout(
        Duration::from_secs(1),
        await_restart(lifecycle, "derived", baseline),
    )
    .await
    .expect("restart observed");

    assert_eq!(
        actor_ref
            .call(Duration::from_secs(1), ProbeMsg::Increment)
            .await
            .expect("replacement replies"),
        (1, 1)
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
