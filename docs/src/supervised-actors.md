# Supervised actors

Every actor declaration placed in a tree becomes its own supervised child.
That gives each logical actor independent restart policy, mailbox binding,
lifecycle events, and statistics.

```rust
# use std::time::Duration;

# use kokage::{ActorSpec, OrderedTree, Restart, Shutdown};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let worker = ActorSpec::new("worker", || Worker)
    .restart(Restart::always())
    .shutdown(Shutdown::drain_for(Duration::from_secs(5)));
let (worker, worker_ref) = worker.actor_ref();

let runtime = OrderedTree::new()
    .actor(worker)
    .spawn()?;
# let _ = worker_ref;
runtime.shutdown_and_wait().await?;
# Ok(())
# }
```

A restart invokes the retained factory, binds a fresh incarnation mailbox, and
reconnects the same `ActorRef`. Messages accepted by the failed incarnation
are lost; delivery is at-most-once.

Here is that recovery boundary in a complete print-shop example:

```rust,no_run
use std::{
    io,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use kokage::{ActorSpec, OrderedTree, Restart, host::BoxError};
use kokage::prelude::*;

struct FrontDesk(ActorRef<String>);

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.0.send(order).await?;
        Ok(())
    }
}

struct Press {
    runs: Arc<AtomicUsize>,
    run: usize,
}

impl Actor for Press {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        if self.run == 0 && order.contains("origami") {
            return Err::<_, BoxError>(Box::new(io::Error::other("paper jam")));
        }
        println!("printed {order}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runs = Arc::new(AtomicUsize::new(0));
    let press_actor = ActorSpec::new("press", move || Press {
        runs: runs.clone(),
        run: 0,
    });
    let (press_actor, press) = press_actor.actor_ref();
    let orders_actor = ActorSpec::new("front-desk", move || FrontDesk(press.clone()));
    let (orders_actor, orders) = orders_actor.actor_ref();

    let runtime = OrderedTree::new()
        .default_restart(Restart::on_failure().limit(5, Duration::from_secs(60)))
        .actor(press_actor)
        .actor(orders_actor)
        .spawn()?;
    let handle = runtime.handle();

    orders.send("business cards x100".into()).await?;
    let baseline = handle.snapshot().child("press").expect("press exists").generation;
    let mut snapshots = handle.subscribe_snapshots();
    orders.send("origami cranes x1000".into()).await?;
    snapshots
        .wait_for_child("press", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await?;
    orders.send("flyers x500".into()).await?;

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

Subscribing before the crashing send is deliberate. A restarted actor gets a
fresh mailbox, so work accepted behind the crashing message is dropped with
the old incarnation. Capturing the generation and receiver first, then waiting
for a later running generation, creates a recovery boundary without a
lost-wakeup window.

## Explicit topology

`OrderedTree` is the static construction front door. Nest trees to create
restart boundaries:

```rust
# use kokage::{ActorSpec, OrderedTree, Strategy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let venues = OrderedTree::new()
    .strategy(Strategy::OneForAll)
    .actor(ActorSpec::new("feed", || Worker))
    .actor(ActorSpec::new("gateway", || Worker));

let tree = OrderedTree::new()
    .actor(ActorSpec::new("router", || Worker))
    .subtree("venues", venues);
# let _ = tree;
```

The id `feed` is local to `venues`. Another sibling scope may also contain a
`feed`; duplicate ids inside the same scope are rejected at spawn.

Scope configuration such as strategy, restart budget, and mailbox-capacity
default is applied where it is declared. Per-actor settings stay on
`ActorSpec`.

`Actor::on_start` is the actor's readiness boundary. An ordered scope does not
start the next declared child until that hook succeeds, and outside code can
await `RuntimeHandle::wait_started`. The readiness latch resets on restart.

Finite work stays on the same handle. `wait_completed(["importer"])` waits for
a successful terminal exit with no pending restart.
`shutdown_on_completion(["importer"])` arms shutdown at that boundary; obtain
the handle before spawning to avoid racing a fast child, and retain the guard.

Use `OrderedTree::task` to mix an arbitrary non-actor `host::ChildSpec` into the
same sequence. Policy configured on the child spec is preserved, while unset
restart and shutdown settings inherit the tree defaults. Task children appear
in snapshots and lifecycle watches but not actor stats.

## Cycles remain typed

Use `ActorSlot` when factories form a cycle. Mint every ref, define the slots,
then place the returned specs in whichever scopes express the intended failure
relationships. Wiring does not choose topology.

## Dynamic membership

A `DynamicTree` is an initially empty one-for-one scope. Its handle accepts
`ActorSpec` values and nested trees at runtime. Static and dynamic scopes
share runtime handles, snapshots, lifecycle streams, and shutdown behavior;
only their membership capability differs.
