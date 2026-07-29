//! A nested declaration: an ordered root, two nested scopes, and a dynamic
//! marker scope all wire from one factories literal.

use tokio_otp::{
    ActorContext, ActorResult, DynamicScope, DynamicTree, GraphBuildError, RestartPolicy, Strategy,
    Supervision,
};

struct Worker;

impl tokio_otp::host::RawActor for Worker {
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

fn main() -> Result<(), GraphBuildError> {
    let (tree, refs) = App::tree(|_refs| AppFactories {
        ingest: || Worker,
        workers: WorkersFactories {
            parse: || Worker,
            render: || Worker,
        },
        pool: PoolFactories {
            manager: || Worker,
            sessions: DynamicTree::new().default_restart(RestartPolicy::Never),
        },
    })?;

    let _ = (tree, refs.ingest, refs.workers.parse, refs.pool.manager);
    Ok(())
}
