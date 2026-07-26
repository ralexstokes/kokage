# Supervised Actors

`tokio-otp` decomposes a typed actor graph so each actor becomes its own
supervised child. Existing `ActorRef<M>` handles keep following the same
long-lived mailbox bindings, so a sender can wait while a failed actor is
restarted and then deliver to the new generation.

Each child is rebuilt by its `ActorFactory` for the initial run and every
restart. Closures in the example below implement that trait automatically;
`#[derive(ActorFactory)]` generates a named factory from the actor when wiring
is large enough to benefit from an explicit type. See
[Incarnation-local state](actor-graphs.md#incarnation-local-state) for the
durable-factory versus local-actor state boundary.

```rust,no_run
use std::{io, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use tokio_otp::prelude::*;

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut ActorContext<String>) -> ActorResult {
        self.press.send(order).await?;
        Ok(Continue)
    }
}

struct Press {
    runs: Arc<AtomicUsize>,
    run: usize,
}

impl Actor for Press {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut ActorContext<String>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(&mut self, order: String, _ctx: &mut ActorContext<String>) -> ActorResult {
        if self.run == 0 && order.contains("origami") {
            return Err::<_, BoxError>(Box::new(io::Error::other("paper jam")));
        }
        println!("printed {order}");
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = GraphBuilder::new();
    let (press_slot, press_ref) = builder.slot::<String>("press");
    let orders = builder.actor("front-desk", {
        let press_ref = press_ref.clone();
        move || FrontDesk { press: press_ref.clone() }
    });
    let runs = Arc::new(AtomicUsize::new(0));
    builder.define(press_slot, move || Press { runs: runs.clone(), run: 0 });
    let graph = builder.build()?;

    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .restart(RestartPolicy::OnFailure)
        .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60)))
        .build()?;
    let handle = runtime.spawn();

    orders.send("business cards x100".into()).await?;
    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle.snapshot().child("press").unwrap().generation;
    orders.send("origami cranes x1000".into()).await?;
    lifecycle
        .wait_started("press", baseline)
        .await
        .expect("press restart is observed");
    orders.send("flyers x500".into()).await?;

    handle.shutdown_and_wait().await?;
    Ok(())
}
```

`Runtime::builder()` is the front door for the common case: it turns every
graph actor into its own supervised child and packages
the result into a `Runtime` with a supervisor and dynamic actor support.
The builder lowers through an inspectable `SupervisionTree`; the next chapter
shows when and how to work with that declaration directly.

Nested actor graphs stay on that path too. Build each graph independently so
typed refs can cross graph boundaries, then attach a configured nested runtime
builder with `subtree`:

```rust,ignore
let runtime = Runtime::builder()
    .graph(core_graph)
    .strategy(Strategy::OneForOne)
    .subtree(
        "venues",
        Runtime::builder()
            .graph(venue_graph)
            .strategy(Strategy::OneForOne),
    )
    .build()?;
```

Subtrees are added before the containing graph's actors, so sequential startup
waits for nested readiness first. `RuntimeHandle::actor_stats()` recursively
includes both graphs. `handle.subtree("venues")` returns a scoped runtime handle
that retains the venue graph's dynamic actor factory, stats, and actor-aware
control methods. Its `supervisor_handle()` exposes lower-level supervisor
control when needed.

Actor children use `on_start` as their readiness boundary. Ordered runtimes do
not spawn the next declared actor until that boundary is crossed; snapshots
remain `Starting`, and `ChildStarted` events and lifecycle `Started`
transitions are delayed until `on_start` succeeds. Code outside the tree can
await `RuntimeHandle::wait_started`; readiness is latched for a completed
generation and resets on restart.

The lifecycle watch before sending `origami cranes x1000` is deliberate.
A worker gets a fresh mailbox on restart; anything queued behind the crashing
`origami` order would be dropped with the old mailbox. `send` waits while the
actor is unbound, but it cannot recover messages already accepted by the
failed run. Waiting for `Started` with a generation above the captured
baseline preserves the old one-shot recovery boundary without a separate
monitor type.

Per-actor policies — say a tighter restart budget for the press alone — stay
on the same builder. Overrides are keyed by the actor's typed ref, so a typo'd
name is unrepresentable:

```rust,ignore
let runtime = Runtime::builder()
    .graph(graph)
    .strategy(Strategy::OneForOne)
    .restart(RestartPolicy::OnFailure)
    .actor_restart_intensity(&press_ref, RestartIntensity::new(5, Duration::from_secs(60)))
    .build()?;
```

Use `RuntimeBuilder::child` to mix arbitrary non-actor `ChildSpec`s into the
same supervisor. Use `RuntimeBuilder::subtree` for nested actor-aware or
graph-less runtime builders.

There are no string lookups anywhere on this path: every ref you need is
minted at wiring time (or returned by `add_actor` for runtime-added actors)
and travels by clone or by message.

Use `Strategy::OneForAll` when a group of actor children should share fate,
or configure them as a runtime subtree for a scoped restart boundary.
