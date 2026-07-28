# Inspectable supervision trees

[`SupervisionTree`] is the primary composition API. It is the recursive model
for ordered scopes, dynamic scopes, actors, arbitrary supervised tasks, and
actor-owned scopes. Build a tree directly whenever the application has more
than one scope or needs per-child policy.

For the common flat shape, `SupervisionTree::graph(&graph)` places every actor
in one ordered scope. `SupervisionTree::dynamic()` creates one empty dynamic
scope. Both use the same `default_restart`, `default_shutdown`, and
`restart_intensity` vocabulary.

```rust,ignore
let tree = SupervisionTree::graph(&graph)
    .strategy(Strategy::RestForOne)
    .default_restart(RestartPolicy::OnFailure);

println!("{:#?}", tree.outline());
let runtime = tree.build()?;
```

## The recursive shape

A tree is an opaque scope declaration with two root kinds:

- `SupervisionTree::new()` constructs a declared, readiness-gated ordered
  child sequence.
- `SupervisionTree::dynamic()` constructs an empty leaf whose membership is
  added and removed at runtime.

Ordered trees expose the fluent `actor`, `task`, `subtree`,
`actor_with_scope`, and `strategy` methods. Their internal child nodes are not
publicly constructible. Dynamic trees do not expose those methods at all, so
declared children and group strategies are compile-time errors rather than
late build failures.

Child order is behavior for an ordered scope. It determines readiness-gated
startup order, reverse-order shutdown, and the suffix restarted by
`Strategy::RestForOne`. A dynamic scope has no declared children. Its outline
is still useful because it records the defaults future members inherit.

Restart and shutdown defaults apply to every direct child edge, including an
edge that wraps a nested supervisor. The nested scope still controls the
defaults of its own children. For `actor_with_scope`, the generated `leader`
and `children` edges both inherit the enclosing scope's defaults unless the
leader carries an explicit override.

## Place actors instead of whole graphs

A `Graph` establishes typed mailbox wiring, but it does not require all of its
actors to be siblings. A hand-built tree can place individual runnable actors
at different levels:

```rust,no_run
use tokio_otp::{ActorSpec, SupervisionTree, prelude::*};

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

    let tree = SupervisionTree::new()
        .default_restart(RestartPolicy::OnFailure)
        .actor(
            ActorSpec::new(graph.actor_for(&ingest)?)
                .restart(RestartPolicy::Never),
        )
        .subtree(
            "workers",
            SupervisionTree::new()
                .strategy(Strategy::OneForAll)
                .actor(graph.actor_for(&parse)?),
        );

    let runtime = tree.build()?;
    drop(runtime);
    Ok(())
}
```

The actor refs minted by `GraphBuilder::slot` continue to follow those actors
across their respective restarts. `RuntimeHandle::actor_stats()` also recurses
through the tree, and the same local child id may be reused in a different
scope.

An [`ActorSpec`] is a complete actor child declaration. Its runnable payload
provides the id, while optional `restart`, `shutdown`, and `restart_intensity`
values override the enclosing scope's defaults. `child_id` overrides the
local supervisor id when an actor label is already qualified by its scope
path. Bare runnable actors convert to `ActorSpec`, so `.actor(runnable)` is the
concise spelling when no override is needed.

For the flat case, `SupervisionTree::graph(&graph)` creates an ordered scope
containing every graph actor. For a runtime-written leaf, configure
`SupervisionTree::dynamic()` with `default_restart`, `default_shutdown`, and
`restart_intensity`; runtime-added actors can still override those inherited
policies with `DynamicActorOptions`. There is no separate graph-derived
defaults path for dynamic composition.

## Reserve a pre-spawn identity

A plain `SupervisionTree` is cloneable declaration data. Call `reserve()` when
code needs the scope's stable `RuntimeHandle` before build or spawn:

```rust,ignore
let sessions_tree = SupervisionTree::dynamic()
    .default_restart(RestartPolicy::OnFailure)
    .reserve();
let sessions = sessions_tree.handle();

let mut graph = GraphBuilder::new();
let (router_slot, router) = graph.slot("router");
graph.define(router_slot, move || Router::new(sessions.clone()));
let graph = graph.build()?;

let app_tree = SupervisionTree::new()
    .reserve()
    // This transfers the nested reservation into the root declaration.
    .reserved_subtree("sessions", sessions_tree)
    .actor(graph.actor_for(&router)?);
let app_handle = app_tree.handle();
let runtime = app_tree.build()?;
# drop((app_handle, runtime));
```

`reserve()` consumes the cloneable tree and returns a
[`ReservedSupervisionTree`], which intentionally does not implement `Clone`.
One reserved identity can therefore bind to exactly one eventual runtime.
The reserved form retains the fluent configuration and composition methods;
`reserved_subtree` and `actor_with_reserved_scope` transfer nested
reservations into their eventual parent.

Before binding, control operations report `ControlError::Unavailable`, while
the handle's projected snapshot and subscriptions are already usable.
`build()` consumes the reserved declaration and preserves that exact identity
in the returned runtime. Dropping the declaration, failing its build, or
dropping the built runtime before spawn makes retained handles terminal.

## Inspect the declaration

`SupervisionTree::outline()` removes executable factories and returns a
[`SupervisionOutline`]. It is `Clone + Debug + Eq + PartialEq`; enabling the
`serde` feature also gives it `Serialize` and `Deserialize`.

An outline retains:

- each scope's immutable [`ScopeKind`], strategy, inherited actor policies,
  and restart-intensity default;
- children in semantic order;
- resolved actor policies, regardless of whether they came from an actor or
  its enclosing scope;
- nested scopes and actor-owned scopes recursively.

That makes outlines useful for assertions, configuration export, and
rendering a declared topology before spawn. A [`SupervisorSnapshot`] is the
runtime companion: it reports current memberships, generations, states, and
exits. They answer different questions, so compare them by path and policy
rather than expecting the structures to be identical.

```rust,ignore
let tree = SupervisionTree::new()
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

Because `Debug` for an executable tree delegates to its outline, logging the
tree never tries to print actor factories. The reserved form has the same
outline and debug projection.

## Actor-owned scopes

Use `actor_with_scope` when one actor owns a set of workers. The restart
relationship is explicit at the call site:

```rust,ignore
let sessions = SupervisionTree::new().actor_with_scope(
    "session-runtime",
    session_actor,
    SupervisionTree::dynamic(),
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

[`SupervisionTree`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.SupervisionTree.html
[`ReservedSupervisionTree`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ReservedSupervisionTree.html
[`ActorSpec`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorSpec.html
[`SupervisionOutline`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.SupervisionOutline.html
[`ScopeKind`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.ScopeKind.html
[`SupervisorSnapshot`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.SupervisorSnapshot.html
[Scope handles inside actors]: dynamic-actors.md#scope-handles-inside-actors
