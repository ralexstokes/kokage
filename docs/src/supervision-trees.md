# Inspectable supervision trees

[`OrderedTree`] and [`DynamicTree`] are the primary composition APIs. They are
single-use, identity-owning declarations for ordered scopes, dynamic scopes,
actors, arbitrary supervised tasks, and actor-owned scopes. Their public
signatures do not expose a scope-kind generic.

For the common flat shape, `OrderedTree::graph(graph)` consumes a graph and
places every actor in one ordered scope. `DynamicTree::new()` creates one empty
dynamic scope. Both use the same `default_restart`, `default_shutdown`, and
`restart_intensity` vocabulary.

```rust,ignore
let tree = OrderedTree::graph(graph)
    .strategy(Strategy::RestForOne)
    .default_restart(RestartPolicy::OnFailure);

println!("{:#?}", tree.outline());
let handle = tree.spawn()?;
```

The actor refs minted by `GraphBuilder::slot` continue to follow those actors
across their respective restarts. `RuntimeHandle::actor_stats()` also recurses
through the tree, and the same local child id may be reused in a different
scope.

## The recursive shape

There are two root kinds:

- `OrderedTree::new()` constructs a declared, readiness-gated child sequence.
- `DynamicTree::new()` constructs an empty leaf whose membership is added and
  removed at runtime.

Ordered trees expose the fluent `actor`, `task`, `subtree`,
`actor_with_scope`, and `strategy` methods. Dynamic trees do not expose those
methods, so declared children and group strategies are compile-time errors.

Child order is behavior for an ordered scope. It determines readiness-gated
startup order, reverse-order shutdown, and the suffix restarted by
`Strategy::RestForOne`. A dynamic scope has no declared children. Its outline
still records the defaults future members inherit.

Restart and shutdown defaults apply to every direct child edge, including an
edge that wraps a nested supervisor. The nested scope still controls the
defaults of its own children. For `actor_with_scope`, the generated `leader`
and `children` edges both inherit the enclosing scope's defaults unless the
leader carries an explicit override.

## Graph ownership and actor placement

A `Graph` establishes typed mailbox wiring. It is not cloneable: moving it into
`OrderedTree::graph` establishes one runtime owner for every runnable binding.
Typed refs minted by `GraphBuilder::slot` remain valid because they own the
stable mailbox identities independently.

For a custom shape, clone individual [`RunnableActor`] values out of the graph
and place them at different levels:

```rust,no_run
use tokio_otp::{ActorSpec, OrderedTree, prelude::*};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphBuilder::new();
    let (ingest_slot, ingest) = graph.slot("ingest");
    let (parse_slot, parse) = graph.slot("parse");
    graph.define(ingest_slot, || Worker);
    graph.define(parse_slot, || Worker);
    let graph = graph.build()?;

    let tree = OrderedTree::new()
        .default_restart(RestartPolicy::OnFailure)
        .actor(
            ActorSpec::new(graph.actor_for(&ingest)?)
                .restart(RestartPolicy::Never),
        )
        .subtree(
            "workers",
            OrderedTree::new()
                .strategy(Strategy::OneForAll)
                .actor(graph.actor_for(&parse)?),
        );

    let handle = tree.spawn()?;
    drop(handle);
    Ok(())
}
```

Graph validation and post-build ref lookup are intentionally separate error
domains: `GraphBuilder::build` returns `GraphBuildError`, while
`Graph::actor_for` returns `GraphLookupError`. The example uses
`Box<dyn Error>` so `?` can carry both. Production code can instead define an
application error enum with one transparent variant for each type.

A runnable binding may occur only once in the complete recursive tree. Reusing
the same `RunnableActor` clone in two nodes is rejected while the tree is
lowered. `RunnableActor` remains cloneable so applications can select actors
from a graph while composing a custom shape; cloning does not create a second
runtime identity.

Advanced code can place clones of one binding into separate trees because
each tree is lowered independently. That does not create another runnable
identity: if both trees run concurrently, the second actor exits with
`ActorRunError::AlreadyRunning`. Prefer one composed tree; retain a runnable
clone only for custom placement or hand-driving where ownership is coordinated.

The actor refs minted by `GraphBuilder::slot` continue to follow those actors
across their respective restarts. `RuntimeHandle::actor_stats()` also recurses
through the tree, and the same local child id may be reused in a different
scope.

An [`ActorSpec`] is a complete actor child declaration. Its runnable payload
provides the id, while optional `restart`, `shutdown`, and `restart_intensity`
values override the enclosing scope's defaults. `child_id` overrides the local
supervisor id when an actor label is already qualified by its scope path. Bare
runnable actors convert to `ActorSpec`, so `.actor(runnable)` is the concise
spelling when no override is needed.

## Identity exists before spawn

Every tree owns its one stable runtime identity from construction. Call
`handle()` whenever wiring code needs the scope's [`RuntimeHandle`] before the
tree is spawned:

```rust,ignore
let sessions_tree = DynamicTree::new()
    .default_restart(RestartPolicy::OnFailure);
let sessions = sessions_tree.handle();

let mut graph = GraphBuilder::new();
let (router_slot, router) = graph.slot("router");
graph.define(router_slot, move || Router::new(sessions.clone()));
let graph = graph.build()?;

let app_tree = OrderedTree::new()
    // Moving the nested tree transfers its identity into the root.
    .subtree("sessions", sessions_tree)
    .actor(graph.actor_for(&router)?);
let app_handle = app_tree.handle();
let handle = app_tree.spawn()?;
# drop((app_handle, handle));
```

Trees deliberately do not implement `Clone`: one identity can bind to one
runtime. Before binding, control operations report
`ControlError::Unavailable`, while projected snapshots and subscriptions are
already usable. `spawn()` consumes the tree and returns its `RuntimeHandle`.
Moving a tree into `subtree` or `actor_with_scope` transfers its identity into
the parent.

Dropping an unspawned tree, failing to lower or spawn it, or having a dynamic
insertion rejected makes all handles issued from that tree terminal. There is
no intermediate `Runtime` object and no `into_supervisor` escape hatch.

## Inspect the declaration

`outline()` removes executable factories and returns a
[`SupervisionOutline`]. It is `Clone + Debug + Eq + PartialEq`; enabling the
`serde` feature also gives it `Serialize` and `Deserialize`.

An outline retains:

- each scope's immutable [`ScopeKind`], strategy, inherited actor policies,
  and restart-intensity default;
- children in semantic order;
- resolved actor policies;
- nested scopes and actor-owned scopes recursively.

That makes outlines useful for assertions, configuration export, and
rendering a topology before spawn. A [`SupervisorSnapshot`] is the runtime
companion: it reports current memberships, generations, states, and exits.

```rust,ignore
let tree = OrderedTree::new()
    .default_restart(RestartPolicy::Always)
    .actor(
        ActorSpec::new(graph.actor_for(&ingest)?)
            .restart(RestartPolicy::Never),
    )
    .actor(graph.actor_for(&parse)?);
let outline = tree.outline();

assert_eq!(outline.child_ids(), ["ingest", "parse"]);
let ChildOutline::Actor { restart, .. } = outline.child("ingest").unwrap()
else {
    unreachable!()
};
assert_eq!(*restart, RestartPolicy::Never);
```

`Debug` for an executable tree delegates to its outline, so logging a tree
never tries to print actor factories.

## Actor-owned scopes

Use `actor_with_scope` when one actor owns a set of workers:

```rust,ignore
let sessions = OrderedTree::new().actor_with_scope(
    "session-runtime",
    session_actor,
    DynamicTree::new(),
    Strategy::RestForOne,
);
```

The node lowers to an ordered `[leader, children]` pair. With `RestForOne`, a
leader failure recycles the owned scope, while a failure inside that scope
leaves the leader running. Use `Strategy::OneForAll` when either side failing
must recycle both, or `Strategy::OneForOne` when they should restart
independently.

Inside the leader, every actor stage's `children()` method returns a
`RestrictedScope` for the child scope. See [Scope handles inside actors] for
startup ordering and dynamic-membership reconciliation.

[`OrderedTree`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.OrderedTree.html
[`DynamicTree`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.DynamicTree.html
[`RuntimeHandle`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.RuntimeHandle.html
[`RunnableActor`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.RunnableActor.html
[`ActorSpec`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorSpec.html
[`SupervisionOutline`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.SupervisionOutline.html
[`ScopeKind`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.ScopeKind.html
[`SupervisorSnapshot`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.SupervisorSnapshot.html
[Scope handles inside actors]: dynamic-actors.md#scope-handles-inside-actors
