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
    let (press_slot, press_ref) =
        builder.slot::<String>("press");
    let (orders_slot, orders) =
        builder.slot("front-desk");
    builder.define(orders_slot, {
        let press_ref = press_ref.clone();
        move || FrontDesk { press: press_ref.clone() }
    });
    let runs = Arc::new(AtomicUsize::new(0));
    builder.define(press_slot, move || Press { runs: runs.clone(), run: 0 });
    let graph = builder.build()?;

    let runtime = SupervisionTree::graph(&graph)
        .strategy(Strategy::OneForOne)
        .default_restart(RestartPolicy::OnFailure)
        .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60)))
        .build()?;
    let handle = runtime.spawn();

    orders.send("business cards x100".into()).await?;
    let mut lifecycle = handle.watch_lifecycle();
    let baseline = handle.snapshot().child("press").unwrap().generation;
    orders.send("origami cranes x1000".into()).await?;
    lifecycle
        .started_after("press", baseline)
        .await
        .expect("press restart is observed");
    orders.send("flyers x500".into()).await?;

    handle.shutdown_and_wait().await?;
    Ok(())
}
```

`SupervisionTree` is the composition front door. `SupervisionTree::graph`
handles the common flat case above by turning every actor in one graph into a
direct child of one ordered scope. Call `reserve()` on the tree when a root
handle is needed before build.

For nested scopes, build each graph independently so typed refs can cross graph
boundaries, then compose them directly as a tree:

```rust,ignore
let tree = SupervisionTree::graph(&core_graph)
    .strategy(Strategy::OneForOne)
    .subtree(
        "venues",
        SupervisionTree::graph(&venue_graph)
            .strategy(Strategy::OneForOne),
    );
let runtime = tree.build()?;
```

The tree's declaration order is its startup order and reverses for shutdown.
`RuntimeHandle::actor_stats()` recursively includes both graphs.
`handle.subtree("venues")` returns a scoped actor-aware runtime handle with
the same observation, completion, shutdown, and dynamic-insertion operations.

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

The lifecycle watch before sending `origami cranes x1000` is deliberate.
A worker gets a fresh mailbox on restart; anything queued behind the crashing
`origami` order would be dropped with the old mailbox. `send` waits while the
actor is unbound, but it cannot recover messages already accepted by the
failed run. Waiting for `Started` with a generation above the captured
baseline gives a one-shot recovery boundary without a separate monitor type.

Per-actor policies — say a tighter restart budget for the press alone — belong
on that actor's `ActorSpec`. Scope methods set inherited defaults, while an
`ActorSpec` is the explicit override:

```rust,ignore
let tree = SupervisionTree::new()
    .strategy(Strategy::OneForOne)
    .default_restart(RestartPolicy::OnFailure)
    .actor(graph.actor_for(&orders)?)
    .actor(
        ActorSpec::new(graph.actor_for(&press_ref)?)
            .restart_intensity(RestartIntensity::new(5, Duration::from_secs(60))),
    );
let runtime = tree.build()?;
```

Use `SupervisionTree::task` to mix an arbitrary non-actor `ChildSpec` into an
ordered scope. Its explicit restart and shutdown arguments are authoritative
for both the tree outline and the running child, so set those policies on the
`task` call rather than on the `ChildSpec`; readiness and restart-intensity
settings on the spec are preserved. Use `SupervisionTree::subtree` for
recursive actor-aware or graph-less scopes. A dynamic
`RuntimeHandle::add_child` adds the same task shape at runtime; task children
appear in snapshots and lifecycle watches but not actor stats.

There are no string lookups anywhere on this path: every ref you need is
minted at wiring time (or returned by `add_actor` for runtime-added actors)
and travels by clone or by message.

Use `Strategy::OneForAll` when a group of actor children should share fate,
or configure them as a runtime subtree for a scoped restart boundary.

## Declaring a Tree with the Derive

When the shape is static, `#[derive(Supervision)]` can declare the graph and
its `SupervisionTree` at once: struct nesting is scope nesting.

```rust,ignore
use tokio_otp::{DynamicScope, RestartPolicy, Strategy, Supervision};

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForAll)]
struct Workers {
    parse: Parser,
    render: Renderer,
}

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForOne)]
struct App {
    #[supervision(restart = RestartPolicy::Never)]
    ingest: Ingest,
    #[supervision(scope)]
    workers: Workers,
    #[supervision(dynamic)]
    sessions: DynamicScope,
}

// Reserved before wiring, so an actor factory can capture the mount.
let sessions = SupervisionTree::dynamic()
    .default_restart(RestartPolicy::Never)
    .reserve();
let mount = sessions.handle();

let (tree, refs) = App::tree(|_refs| AppFactories {
    ingest: move || Ingest::new(mount.clone()),
    workers: WorkersFactories {
        parse: || Parser::new(),
        render: || Renderer::new(),
    },
    sessions,
})?;
let handle = tree.build()?.spawn();
```

All three actors join **one** graph, so refs cross scope boundaries freely and
cyclic wiring keeps working exactly as it does for a graph alone. Only
supervision placement is hierarchical. Actor labels are qualified by the scope
path, so the graph above contains `ingest`, `workers.parse`, and
`workers.render`.

Supervisor child ids stay local to their scope: `parse` is named `parse` inside
the `workers` supervisor, giving the path `root.workers.parse` — the label with
`root.` in front, not a repeated scope name. Snapshot and lifecycle lookups take
the local id (`workers_handle.snapshot().child("parse")`) while `actor_stats`
reports the qualified label (`workers.parse`).

Because one graph means one `mailbox_capacity`, set a per-actor override with
`ActorOptions::mailbox_capacity` where a scope previously had its own graph.

Field order is semantic here in a way it is not for a graph alone: an ordered
scope starts children in declaration order, and `Strategy::RestForOne` restarts
the ones that follow. Reordering fields changes restart behaviour.

Two field attributes select what a field is:

- `#[supervision(scope)]` — a nested derived struct, becoming a named child
  scope.
- `#[supervision(dynamic)]` — an empty scope whose membership is written at
  runtime. The field type is the `DynamicScope` marker, which is never
  constructed; its wiring entry is a `ReservedSupervisionTree<true>`. Supplying the
  reserved tree is what makes the scope's mount handle available *before* wiring, so
  an actor can hold it as a durable factory field instead of looking the scope
  up after spawn. Policy comes from the tree
  (`SupervisionTree::dynamic().default_restart(..)`), not from attributes.

Per-actor `restart`, `shutdown`, and `restart_intensity` overrides go on the
field; scope-wide defaults and `strategy` go on the struct. `App::tree` returns
the non-`Clone` `ReservedSupervisionTree` declaration — paired, like every
generated constructor, with the refs bundle — without building it. The
reservation carries the pre-spawn identities for dynamic fields, so the mount
handles supplied during wiring bind to the runtime eventually built from that
exact declaration. It is also useful for asserting shape through `outline()`.

The derive generates `tree` and `tree_with`. The latter takes a `GraphConfig`
for graph-wide name and mailbox capacity without exposing mutable actor slots
to generated composition.

Use `GraphBuilder::slot(id)` plus `define` when graph actors are created in a
loop or need hand-written wiring; choose `slot_with(id, ActorOptions)` for
non-default mailbox behavior. Compose the resulting graph with
`SupervisionTree`; reserve the tree first when wiring needs its pre-spawn
handle.
