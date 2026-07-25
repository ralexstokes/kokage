use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio_otp::{
    Actor, ActorContext, ActorFactory, ActorResult, GraphBuilder, Reply, RestartPolicy, Runtime,
    prelude::Continue,
};

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

    async fn on_start(&mut self, _ctx: &mut ActorContext<Self::Msg>) -> ActorResult {
        self.incarnation = self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut ActorContext<Self::Msg>,
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

fn assert_factory<F: ActorFactory<Actor = DerivedActor> + Clone>() {}

#[tokio::test]
async fn derive_clones_durable_configuration_and_defaults_each_incarnation() {
    assert_factory::<DerivedActorFactory>();

    let starts = Arc::new(AtomicUsize::new(0));
    let mut builder = GraphBuilder::new();
    let actor_ref = builder.actor(
        "derived",
        DerivedActorFactory {
            starts: starts.clone(),
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
    assert_eq!(
        actor_ref
            .call(Duration::from_secs(1), ProbeMsg::Increment)
            .await
            .expect("first incarnation replies again"),
        (0, 2)
    );

    let restarted = handle
        .supervisor_handle()
        .monitor_restart("derived")
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
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
