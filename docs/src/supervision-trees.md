# Inspectable supervision trees

`Runtime::builder()` is concise when every actor in a graph belongs at one
scope level. The builder is still describing a tree, though: nested scopes
live in `subtree` calls, actor policy lives in overrides, and child order is
spread across method calls. [`RuntimeBuilder::into_tree`] exposes the same
configuration as a [`SupervisionTree`] before anything runs.

```rust,ignore
let tree = Runtime::builder()
    .graph(graph)
    .strategy(Strategy::RestForOne)
    .restart(RestartPolicy::OnFailure)
    .subtree("storage", storage_runtime)
    .into_tree();

println!("{:#?}", tree.outline()?);
let runtime = tree.build()?;
```

`RuntimeBuilder::build()` is itself `into_tree().build()`, so inspection and
execution cannot drift onto separate lowering paths. The same conversion is
available on `DynamicRuntimeBuilder`, where it produces an empty dynamic
scope.

## The recursive shape

A tree has two scope nodes and three child-node shapes:

- `SupervisionTree::Ordered` is a declared, readiness-gated child sequence.
  `SupervisionTree::new()` constructs an empty ordered scope.
- `SupervisionTree::Dynamic` is an empty leaf whose membership is added and
  removed at runtime. `SupervisionTree::dynamic()` constructs one.
- `SupervisionTree::Actor` carries an actor declaration and its policy
  overrides.
- `SupervisionTree::Child` carries an arbitrary non-actor `ChildSpec`.
- `SupervisionTree::ActorWithScope` carries an actor leader and the scope it
  owns.

The root passed to `build` must be a scope. The fluent `actor`, `task`,
`subtree`, and `actor_with_scope` methods append child nodes beneath it.

Child order is behavior for an ordered scope. It determines
readiness-gated startup order, reverse-order shutdown, and the suffix restarted
by `Strategy::RestForOne`. A dynamic scope has no declared children; attempts
to give it a group strategy or append children are rejected when the tree is
built. Its outline is still useful because it records the policies future
members inherit.

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
        Ok(Continue)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphBuilder::new();
    let _ingest = graph.actor("ingest", || Worker);
    let _parse = graph.actor("parse", || Worker);
    let graph = graph.build()?;

    let tree = SupervisionTree::new()
        .actor(
            ActorSpec::new(graph.actors()[0].clone())
                .restart(RestartPolicy::Never),
        )
        .subtree(
            "workers",
            SupervisionTree::new()
                .strategy(Strategy::OneForAll)
                .actor(graph.actors()[1].clone()),
        );

    let runtime = tree.build()?;
    drop(runtime);
    Ok(())
}
```

The actor refs minted while building the graph continue to follow those
actors across their respective restarts. `RuntimeHandle::actor_stats()` also
recurses through the tree, and the same local actor label may be reused in a
different scope because child ids are sibling-scoped.

An [`ActorSpec`] is a complete actor child declaration: its runnable payload
provides the id, and optional `restart`, `shutdown`, and `restart_intensity`
values override the enclosing scope's defaults. `child_id` overrides the id
itself, for a nested scope whose actor labels are already qualified by the
scope path. Bare runnable actors convert to `ActorSpec`, so `.actor(runnable)`
remains the concise spelling when no override is needed.

When a hand-built dynamic scope will receive actors at runtime, call
`dynamic_defaults(&graph)` on that scope to adopt the graph's actor execution
settings. Ordered scopes have fixed membership and reject those additions.

## Inspect the declaration

`SupervisionTree::outline()` removes executable factories and returns a
[`SupervisionOutline`]. It is `Clone + Debug + Eq + PartialEq`; enabling the
`serde` feature also gives it `Serialize` and `Deserialize`.

An outline retains:

- each scope's immutable [`ScopeKind`], strategy, inherited actor policies,
  and restart-intensity default;
- children in semantic order;
- resolved actor policies, regardless of whether they came from the actor or
  its enclosing scope;
- nested scopes and actor-owned scopes recursively.

That makes outlines useful for snapshots in tests, configuration export, and
rendering a declared topology before spawn. A [`SupervisorSnapshot`] is the
runtime companion: it reports current memberships, generations, states, and
exits. They answer different questions, so compare them by path and policy
rather than expecting the two structures to be identical.

```rust,ignore
let outline = Runtime::builder()
    .graph(graph)
    .restart(RestartPolicy::Always)
    .actor_restart(&ingest_ref, RestartPolicy::Never)
    .into_tree()
    .outline()?;

assert_eq!(outline.child_ids(), ["ingest", "parse"]);
let ChildOutline::Actor { restart, .. } = outline.child("ingest").unwrap()
else {
    unreachable!()
};
assert_eq!(*restart, RestartPolicy::Never);
```

Because `Debug` for an executable tree delegates to its outline, logging the
tree never tries to print actor factories.

## Actor-owned scopes

Use `actor_with_scope` when one actor owns a set of workers:

```rust,ignore
let sessions = SupervisionTree::new().actor_with_scope(
    "session-runtime",
    session_actor,
    SupervisionTree::dynamic(),
);
```

The node lowers to an ordered `[leader, children]` pair. Its default
`RestForOne` relationship means a leader failure recycles the owned scope,
while a failure inside that scope leaves the leader running. Use
`actor_with_scope_strategy(..., Strategy::OneForAll)` when either side failing
must recycle both.

Inside the leader, `MessageContext::children()` returns the child scope's
pre-spawn `RuntimeHandle`; `StartContext::children()` returns the narrower
`RestrictedScope`, as does `StopContext::children()`.
See [Scope handles inside actors] for the startup ordering and
reconciliation rules of dynamic membership.

[`RuntimeBuilder::into_tree`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.RuntimeBuilder.html#method.into_tree
[`SupervisionTree`]: https://stokes.io/tokio-otp/api/tokio_otp/enum.SupervisionTree.html
[`ActorSpec`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorSpec.html
[`SupervisionOutline`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.SupervisionOutline.html
[`ScopeKind`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.ScopeKind.html
[`SupervisorSnapshot`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.SupervisorSnapshot.html
[Scope handles inside actors]: dynamic-actors.md#scope-handles-inside-actors
