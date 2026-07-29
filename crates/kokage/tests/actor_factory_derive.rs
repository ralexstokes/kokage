mod support;

use support::TreeBuilder;

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{
    Actor, ActorFactory, ActorResult, ActorSpec, Context, Reply, Restart, RuntimeHandle,
    observe::SupervisorSnapshotReceiver,
};

fn restart_observer(handle: &RuntimeHandle, id: &str) -> (SupervisorSnapshotReceiver, u64) {
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

enum ProbeMsg {
    Increment(Reply<(usize, usize)>),
    Crash,
}

#[derive(kokage::ActorFactory)]
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

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ActorResult {
        self.incarnation = self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ActorResult {
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
    let mut builder = TreeBuilder::new();
    let actor_ref = builder.actor(ActorSpec::new(
        "derived",
        DerivedActorFactory {
            starts: starts.clone(),
        },
    ));
    let handle = builder
        .build()
        .default_restart(Restart::on_failure())
        .spawn()
        .expect("runtime builds");

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

    let (lifecycle, baseline) = restart_observer(&handle.handle(), "derived");
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
