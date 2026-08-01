#![cfg(feature = "derive")]

use std::{
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
    #[factory(default = Self::INITIAL_LOCAL)]
    local: usize,
}

impl DerivedActor {
    const INITIAL_LOCAL: usize = 10;
}

impl Actor for DerivedActor {
    type Msg = ProbeMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.incarnation = self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
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
    let mut builder = Tree::new();
    let actor_ref = builder.add_actor_spec(ActorSpec::new(
        "derived",
        DerivedActorFactory {
            starts: starts.clone(),
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
        (0, 11)
    );
    assert_eq!(
        actor_ref
            .call(ProbeMsg::Increment, Duration::from_secs(1))
            .await
            .expect("first incarnation replies again"),
        (0, 12)
    );

    let (lifecycle, baseline) = restart_observer(&handle.scope(), "derived");
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
            .call(ProbeMsg::Increment, Duration::from_secs(1))
            .await
            .expect("replacement replies"),
        (1, 11)
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    handle.shutdown().await.expect("clean shutdown");
}
