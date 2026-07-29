//! The derive uses the same `restart_config` spelling as the tree and actor
//! specification builder APIs, at both scope and actor-field levels.

use kokage::{ActorResult, GraphBuildError, Supervision, host::ActorContext};

struct Worker;

impl kokage::host::RawActor for Worker {
    type Msg = ();

    async fn run(&mut self, _: ActorContext<()>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
#[supervision(
    restart_config = kokage::RestartConfig::new(4, std::time::Duration::from_secs(2))
)]
struct App {
    #[supervision(
        restart_config = kokage::RestartConfig::new(2, std::time::Duration::from_secs(1))
    )]
    worker: Worker,
}

fn main() -> Result<(), GraphBuildError> {
    let (tree, refs) = App::tree(|_refs| AppFactories { worker: || Worker })?;
    let _ = (tree, refs.worker);
    Ok(())
}
