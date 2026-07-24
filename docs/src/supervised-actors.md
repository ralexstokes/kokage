# Supervised Actors

`tokio-otp` decomposes a typed actor graph so each actor becomes its own
supervised child. Existing `ActorRef<M>` handles keep following the same
long-lived mailbox bindings, so a sender can wait while a failed actor is
restarted and then deliver to the new generation.

Each child is rebuilt by its `ActorFactory` for the initial run and every
restart. Closures in the example below implement that trait automatically;
named spec structs can hold the same durable configuration when the wiring is
large enough to benefit from an explicit type.

```rust,no_run
use std::{io, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use tokio_otp::prelude::*;

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &ActorContext<String>) -> ActorResult {
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

    async fn on_start(&mut self, _ctx: &ActorContext<String>) -> ActorResult {
        self.run = self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(Continue)
    }

    async fn handle(&mut self, order: String, _ctx: &ActorContext<String>) -> ActorResult {
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
    let restart = handle.supervisor_handle().monitor_restart("press")?;
    orders.send("origami cranes x1000".into()).await?;
    restart.await?;
    orders.send("flyers x500".into()).await?;

    handle.shutdown_and_wait().await?;
    Ok(())
}
```

`Runtime::builder()` is the front door for the common case: it turns every
graph actor into its own supervised child with uniform policies and packages
the result into a `Runtime` with a supervisor and dynamic actor support.

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
            .strategy(Strategy::OneForOne)
            .start_mode(StartMode::Sequential),
    )
    .build()?;
```

Subtrees are added before the containing graph's actors, so sequential startup
waits for nested readiness first. `RuntimeHandle::actor_stats()` recursively
includes both graphs. `handle.subtree("venues")` returns a scoped runtime handle
that retains the venue graph's dynamic actor factory and stats. Lower-level
supervisor control remains available explicitly through
`handle.supervisor_handle().supervisor("venues")`.

Actor children use `on_start` as their readiness boundary. Even with the
default concurrent start mode, snapshots remain `Starting`, `ChildStarted`
events are delayed, and restart monitors remain pending until `on_start`
succeeds. Select `StartMode::Sequential` to additionally prevent the next
actor from spawning until that boundary is crossed. Code outside the tree can
await `RuntimeHandle::wait_started`; readiness is latched for a completed
generation and resets on restart.

The restart monitor before sending `origami cranes x1000` is still deliberate.
A worker gets a fresh mailbox on restart; anything queued behind the crashing
`origami` order would be dropped with the old mailbox. `send` waits while the
actor is unbound, but it cannot recover messages already accepted by the
failed run.

When you need per-actor policies — say a tighter restart budget for the press
alone — drop down to `SupervisedActors`, which the builder uses under the
hood. Overrides are keyed by the actor's typed ref, so a typo'd name is
unrepresentable:

```rust,ignore
let runtime = SupervisedActors::new(graph)
    .restart(RestartPolicy::OnFailure)
    .actor_restart_intensity(&press_ref, RestartIntensity::new(5, Duration::from_secs(60)))
    .build_runtime(SupervisorBuilder::new().strategy(Strategy::OneForOne))?;
```

`SupervisedActors::new(graph)` adapts the graph's runnable actors, then you
choose the integration level:

- `build_runtime(builder)` returns a `Runtime` with a supervisor and dynamic
  actor support.
- `build_supervisor(builder)` returns a plain `Supervisor`.
- `build()` returns `Vec<ChildSpec>` for integrating actor children into a
  larger supervisor manually.

There are no string lookups anywhere on this path: every ref you need is
minted at wiring time (or returned by `add_actor` for runtime-added actors)
and travels by clone or by message.

Use `Strategy::OneForAll` when a group of actor children should share fate,
or configure them as a runtime subtree for a scoped restart boundary. Use a raw
nested supervisor when the subtree contains arbitrary non-actor children.
