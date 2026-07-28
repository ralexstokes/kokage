//! A nested declaration: an ordered root, two nested scopes, and a dynamic
//! marker scope all wire from one factories literal.

use tokio_otp::{
    ActorContext, ActorResult, DynamicScope, RestartPolicy, Runtime, Strategy, Supervision,
    SupervisionBuildError,
};

struct Worker;

impl tokio_otp::RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForAll)]
struct Workers {
    parse: Worker,
    render: Worker,
}

#[derive(Supervision)]
#[supervision(strategy = Strategy::RestForOne)]
struct Pool {
    manager: Worker,
    #[supervision(dynamic)]
    sessions: DynamicScope,
}

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForOne, restart = RestartPolicy::Always)]
struct App {
    #[supervision(restart = RestartPolicy::Never, label = "collector")]
    ingest: Worker,
    #[supervision(scope)]
    workers: Workers,
    #[supervision(scope)]
    pool: Pool,
}

fn main() -> Result<(), SupervisionBuildError> {
    let (runtime, refs) = App::runtime(|_refs| AppFactories {
        ingest: || Worker,
        workers: WorkersFactories {
            parse: || Worker,
            render: || Worker,
        },
        pool: PoolFactories {
            manager: || Worker,
            sessions: Runtime::dynamic().default_restart(RestartPolicy::Never),
        },
    })?;

    let _ = (runtime, refs.ingest, refs.workers.parse, refs.pool.manager);
    Ok(())
}
