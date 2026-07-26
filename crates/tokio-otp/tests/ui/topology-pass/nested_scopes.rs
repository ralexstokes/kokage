//! A nested topology: an ordered root, a nested scope, a leader-owned scope,
//! and a dynamic marker scope all wire from one factories literal.

use tokio_otp::{
    ActorContext, ActorResult, DynamicScope, RestartPolicy, Runtime, Strategy, Topology,
    TopologyBuildError,
};

struct Worker;

impl tokio_otp::RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(tokio_otp::prelude::Continue)
    }
}

#[derive(Topology)]
#[topology(strategy = Strategy::OneForAll)]
struct Workers {
    parse: Worker,
    render: Worker,
}

#[derive(Topology)]
#[topology(leader_strategy = Strategy::OneForAll)]
struct Pool {
    #[topology(leader)]
    manager: Worker,
    #[topology(dynamic)]
    sessions: DynamicScope,
}

#[derive(Topology)]
#[topology(strategy = Strategy::OneForOne, restart = RestartPolicy::Always)]
struct App {
    #[topology(restart = RestartPolicy::Never, label = "collector")]
    ingest: Worker,
    #[topology(scope)]
    workers: Workers,
    #[topology(scope)]
    pool: Pool,
}

fn main() -> Result<(), TopologyBuildError> {
    let (runtime, refs) = App::runtime_with_refs(|_refs| AppFactories {
        ingest: || Worker,
        workers: WorkersFactories {
            parse: || Worker,
            render: || Worker,
        },
        pool: PoolFactories {
            manager: || Worker,
            sessions: Runtime::dynamic().restart(RestartPolicy::Never),
        },
    })?;

    let _ = (runtime, refs.ingest, refs.workers.parse, refs.pool.manager);
    Ok(())
}
