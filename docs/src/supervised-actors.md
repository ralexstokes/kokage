# Supervised Actors

`kokage` decomposes a typed actor graph so each actor becomes its own
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

use kokage::{RestartConfig, host::BoxError};
use kokage::prelude::*;

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.press.send(order).await?;
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
    let mut builder = GraphBuilder::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let press = builder.actor(ActorSpec::new("press", move || Press {
        runs: runs.clone(),
        run: 0,
    }));
    let orders = builder.actor(ActorSpec::new("front-desk", move || FrontDesk {
        press: press.clone(),
    }));

    let runtime = OrderedTree::graph(builder.build()?)
        .restart_config(RestartConfig::new(5, Duration::from_secs(60)))
        .spawn()?;
    let handle = runtime.handle();

    orders.send("business cards x100".into()).await?;
    let baseline = handle
        .snapshot()
        .child("press")
        .expect("the declared press actor is present")
        .generation;
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

`OrderedTree` is the static composition front door. `OrderedTree::graph`
handles the common flat case above by consuming one graph and turning every
actor into a direct child of one ordered scope. Call `handle()` before spawn
when wiring needs the root handle.

For nested scopes, build each graph independently so typed refs can cross graph
boundaries, then compose them directly as a tree:

```rust,ignore
let tree = OrderedTree::graph(core_graph)
    .subtree("venues", OrderedTree::graph(venue_graph));
let runtime = tree.spawn()?;
```

The tree's declaration order is its startup order and reverses for shutdown.
`RuntimeHandle::actor_stats()` recursively includes both graphs.
`runtime.handle().subtree("venues")` returns a scoped actor-aware runtime handle with
the same observation, completion, and shutdown operations. When that scope is
dynamic, the scoped handle's `dynamic()` method returns its
membership-mutation capability.

Actor children use `on_start` as their readiness boundary. Ordered runtimes do
not spawn the next declared actor until that boundary is crossed; snapshots
remain `Starting`, and `ChildStarted` events and lifecycle `Started`
transitions are delayed until `on_start` succeeds. Code outside the tree can
await `RuntimeHandle::wait_started`; readiness is latched for a completed
generation and resets on restart.

Finite actor work stays on that same handle. `wait_completed(["importer"])`
waits until the named child has exited successfully without a pending restart.
`shutdown_on_completion(["importer"])` arms a background reduction that shuts
the scope down at the same boundary; take the runtime's handle before spawning
to avoid racing a fast child, and retain the returned guard.

Subscribing to snapshots before sending `origami cranes x1000` is deliberate.
A worker gets a fresh mailbox on restart; anything queued behind the crashing
`origami` order would be dropped with the old mailbox. `send` waits while the
actor is unbound, but it cannot recover messages already accepted by the
failed run. Capturing the baseline and creating the receiver before the send,
then waiting for a later running generation, gives a recovery boundary without
a lost-wakeup window or a separate monitor type.

Per-actor policies — say a tighter restart budget for the press alone — belong
on that actor's `ActorSpec`. Scope methods set inherited defaults, while an
`ActorSpec` is the explicit override:

```rust,ignore
let press = ActorSpec::new("press", PressFactory::new())
    .restart_config(RestartConfig::new(5, Duration::from_secs(60)));
let press_ref = press.actor_ref();
let orders = ActorSpec::new("front-desk", move || FrontDesk {
    press: press_ref.clone(),
});

let tree = OrderedTree::new()
    .actor(orders)
    .actor(press);
let runtime = tree.spawn()?;
```

Use `OrderedTree::task` to mix an arbitrary non-actor `host::ChildSpec` into an
ordered scope. Restart and shutdown policies set on the `ChildSpec` are
preserved; unset policies inherit the tree's `default_*` values. Readiness and
restart configuration on the spec are preserved too. Use `OrderedTree::subtree` for
recursive actor-aware or graph-less scopes. A dynamic
`DynamicRuntimeHandle::add_child` adds the same task shape at runtime; task children
appear in snapshots and lifecycle watches but not actor stats.

There are no string lookups anywhere on this path: every ref you need is
minted at wiring time (or returned by `add_actor` for runtime-added actors)
and travels by clone or by message.

Use `Strategy::OneForAll` when a group of actor children should share fate,
or configure them as a runtime subtree for a scoped restart boundary.

## Derived Wiring, Explicit Topology

`#[derive(Supervision)]` is intentionally limited to cyclic graph wiring and
typed refs. Derive it on an actor declaration, return the generated factory
bundle from `wire`, then build the supervision tree explicitly:

```rust,ignore
use kokage::{GraphBuilder, OrderedTree, RestartPolicy, Strategy, Supervision};

#[derive(Supervision)]
struct App {
    ingest: Ingest,
    parser: Parser,
    renderer: Renderer,
}

let mut graph = GraphBuilder::new();
let refs = App::wire(&mut graph, |refs| AppFactories {
    ingest: IngestFactory::new(refs.parser.clone()),
    parser: ParserFactory::new(refs.renderer.clone()),
    renderer: RendererFactory::new(refs.ingest.clone()),
});
let mut nodes = graph.build()?.into_nodes_by_label();
let ingest = nodes.remove("ingest").expect("ingest node");
let parser = nodes.remove("parser").expect("parser node");
let renderer = nodes.remove("renderer").expect("renderer node");

let tree = OrderedTree::new()
    .strategy(Strategy::OneForAll)
    .default_restart(RestartPolicy::Never)
    .actor(ingest)
    .subtree(
        "workers",
        OrderedTree::new()
            .actor(parser)
            .actor(renderer),
    );
let runtime = tree.spawn()?;
```

The macro generates `AppRefs`, generic `AppFactories`, and `App::wire`; it no
longer generates `Slots` or `Scopes` types. The wiring closure remains because
every ref must exist before cyclic factories can capture it.

Only `#[supervision(label = "...")]` remains as a field attribute. Mailbox
configuration belongs on the explicit graph declaration: derived fields use
the graph defaults, while an actor that needs individual settings can be left
out of the derived declaration and wired with an `ActorSlot` alongside it.
Restart/shutdown policy, ordering, nested scopes, and dynamic membership belong
on `OrderedTree` and `DynamicTree`; there is no `DynamicScope` marker or
type-name detection.
