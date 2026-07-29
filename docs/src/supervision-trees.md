# Inspectable supervision trees

[`OrderedTree`] and [`DynamicTree`] are the primary composition APIs. They are
single-use, identity-owning declarations for ordered scopes, dynamic scopes,
actors, arbitrary supervised tasks, and actor-owned scopes. Their public
signatures do not expose a scope-kind generic.

For the common flat shape, `OrderedTree::graph(graph)` consumes a graph and
places every actor in one ordered scope. `DynamicTree::new()` creates one empty
dynamic scope. Both use the same `default_restart`, `default_shutdown`, and
`restart_config` vocabulary.

```rust,ignore
let tree = OrderedTree::graph(graph)
    .strategy(Strategy::RestForOne);

println!("{:#?}", tree.outline());
let runtime = tree.spawn()?;
```

The actor refs returned by `GraphBuilder::actor` (or minted from an
`ActorSlot` for a cycle) continue to follow those actors across their
respective restarts.
`RuntimeHandle::actor_stats()` also recurses
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

A `Graph` establishes typed mailbox wiring and owns one linear [`ActorNode`]
for each declaration. It is not cloneable: moving it into
`OrderedTree::graph` establishes one runtime owner for every actor binding.
Typed refs returned by `GraphBuilder::actor` remain valid because they own the
stable mailbox identities independently.

For a custom shape, consume the graph with `Graph::into_nodes` and move its
nodes to the desired levels. Nodes retain the configuration supplied by their
`ActorSpec` before graph registration:

```rust,no_run
use kokage::{ActorSpec, OrderedTree, prelude::*};

struct Worker;

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphBuilder::new();
    let _ingest = graph.actor(
        ActorSpec::new("ingest", || Worker)
            .restart(RestartPolicy::Never),
    );
    let _parse = graph.actor(ActorSpec::new("parse", || Worker));
    let mut nodes = graph.build()?.into_nodes().into_iter();
    let ingest = nodes.next().expect("ingest node");
    let parse = nodes.next().expect("parse node");

    let tree = OrderedTree::new()
        .actor(ingest)
        .subtree(
            "workers",
            OrderedTree::new()
                .strategy(Strategy::OneForAll)
                .actor(parse),
        );

    let runtime = tree.spawn()?;
    drop(runtime);
    Ok(())
}
```

`GraphBuilder::build` validates the graph before returning it. A graph's nodes
are yielded once, in declaration order. Because `ActorNode` is not cloneable,
moving a node into a tree makes duplicate placement unrepresentable. Advanced
custom hosts can explicitly leave the tree-placement API with
`ActorNode::into_runnable`; ordinary tree composition should keep the linear
node.

The actor refs returned during graph registration continue to follow those
actors across their respective restarts. `RuntimeHandle::actor_stats()` also recurses
through the tree, and the same local child id may be reused in a different
scope.

An [`ActorSpec`] is a complete actor child declaration. It owns the actor id,
factory, mailbox settings, and optional `restart`, `shutdown`, and
`restart_config` overrides. It is itself linear and can be consumed directly
by `GraphBuilder::actor`, `OrderedTree::actor`, or dynamic insertion. A graph
turns each declaration into an `ActorNode`; `ActorNode::child_id` can override
the local supervisor id when an actor label is already qualified by its scope
path.

## Identity exists before spawn

Every tree owns its one stable runtime identity from construction. Call
`handle()` whenever wiring code needs the scope's handle before the tree is
spawned. An `OrderedTree` returns a [`RuntimeHandle`], while a `DynamicTree`
returns a `DynamicRuntimeHandle` that exposes membership directly:

```rust,ignore
let sessions_tree = DynamicTree::new();
let sessions = sessions_tree.handle();

let router = ActorSpec::new("router", move || Router::new(sessions.clone()));
let router_ref = router.actor_ref();

let app_tree = OrderedTree::new()
    // Moving the nested tree transfers its identity into the root.
    .subtree("sessions", sessions_tree)
    .actor(router);
let app_handle = app_tree.handle();
let runtime = app_tree.spawn()?;
let handle = runtime.handle();
# drop((router_ref, app_handle, handle, runtime));
```

Trees deliberately do not implement `Clone`: one identity can bind to one
runtime. Before binding, control operations report
`ControlError::Unavailable`, while projected snapshots and subscriptions are
already usable. `spawn()` consumes the tree and returns its owning `Runtime`;
`Runtime::handle()` clones a non-owning `RuntimeHandle`.
Moving a tree into `subtree` or `actor_with_scope` transfers its identity into
the parent.

Dropping an unspawned tree, failing to lower or spawn it, or having a dynamic
insertion rejected makes all handles issued from that tree terminal. Dropping
any handles leaves a spawned runtime alive; dropping its `Runtime` owner is the
one implicit graceful-shutdown path. `let _ = tree.spawn()?;` therefore starts
shutdown at the end of that statement.

## Inspect the declaration

`outline()` removes executable factories and returns a
[`observe::SupervisionOutline`]. It is `Clone + Debug + Eq + PartialEq`; enabling the
`serde` feature also gives it `Serialize` and `Deserialize`.

An outline retains:

- each scope's immutable [`ScopeKind`], strategy, inherited actor policies,
  and restart configuration;
- children in semantic order;
- resolved actor policies;
- nested scopes and actor-owned scopes recursively.

That makes outlines useful for assertions, configuration export, and
rendering a topology before spawn. An [`observe::SupervisorSnapshot`] is the runtime
companion: it reports current memberships, generations, states, and exits.

```rust,ignore
let tree = OrderedTree::new()
    .default_restart(RestartPolicy::Always)
    .actor(
        ActorSpec::new("ingest", || Worker)
            .restart(RestartPolicy::Never),
    )
    .actor(ActorSpec::new("parse", || Worker));
let outline = tree.outline();

assert_eq!(outline.child_ids(), ["ingest", "parse"]);
let observe::ChildOutline::Actor { restart, .. } = outline.child("ingest").unwrap()
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

[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`DynamicTree`]: https://stokes.io/kokage/api/kokage/struct.DynamicTree.html
[`RuntimeHandle`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html
[`DynamicRuntimeHandle`]: https://stokes.io/kokage/api/kokage/struct.DynamicRuntimeHandle.html
[`ActorNode`]: https://stokes.io/kokage/api/kokage/struct.ActorNode.html
[`host::RunnableActor`]: https://stokes.io/kokage/api/kokage/host/struct.RunnableActor.html
[`ActorSpec`]: https://stokes.io/kokage/api/kokage/struct.ActorSpec.html
[`observe::SupervisionOutline`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisionOutline.html
[`ScopeKind`]: https://stokes.io/kokage/api/kokage_supervisor/enum.ScopeKind.html
[`observe::SupervisorSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisorSnapshot.html
[Scope handles inside actors]: dynamic-actors.md#scope-handles-inside-actors
