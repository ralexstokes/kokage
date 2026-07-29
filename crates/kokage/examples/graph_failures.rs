use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kokage::{host::RawActor, prelude::*};
use tokio::time::{sleep, timeout};

#[derive(Clone)]
struct FailsOnce {
    runs: Arc<AtomicUsize>,
}

impl RawActor for FailsOnce {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(io::Error::other("boom").into());
        }
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[derive(Clone)]
struct Healthy {
    runs: Arc<AtomicUsize>,
}

impl RawActor for Healthy {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        self.runs.fetch_add(1, Ordering::SeqCst);
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

async fn demonstrate(strategy: Strategy) -> Result<(usize, usize), Box<dyn Error>> {
    let failing_runs = Arc::new(AtomicUsize::new(0));
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("healthy");
    builder.define(actor_slot, {
        let healthy_runs = healthy_runs.clone();
        move || Healthy {
            runs: healthy_runs.clone(),
        }
    });
    let (actor_slot, _) = builder.slot("failing");
    builder.define(actor_slot, {
        let failing_runs = failing_runs.clone();
        move || FailsOnce {
            runs: failing_runs.clone(),
        }
    });

    let handle = OrderedTree::graph(builder.build()?)
        .strategy(strategy)
        .default_restart(RestartPolicy::Always)
        .spawn()?;

    timeout(Duration::from_secs(1), async {
        loop {
            let failed_restarted = failing_runs.load(Ordering::SeqCst) >= 2;
            let healthy_restarted = healthy_runs.load(Ordering::SeqCst) >= 2;
            if failed_restarted && (strategy == Strategy::OneForOne || healthy_restarted) {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;

    sleep(Duration::from_millis(20)).await;
    let counts = (
        failing_runs.load(Ordering::SeqCst),
        healthy_runs.load(Ordering::SeqCst),
    );
    handle.shutdown_and_wait().await?;
    Ok(counts)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let one_for_one = demonstrate(Strategy::OneForOne).await?;
    assert_eq!(one_for_one.1, 1);
    println!(
        "OneForOne isolated the failure: failing runs={}, healthy runs={}",
        one_for_one.0, one_for_one.1
    );

    let one_for_all = demonstrate(Strategy::OneForAll).await?;
    assert!(one_for_all.1 >= 2);
    println!(
        "OneForAll shared fate: failing runs={}, healthy runs={}",
        one_for_all.0, one_for_all.1
    );

    Ok(())
}
