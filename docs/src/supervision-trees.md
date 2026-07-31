# Inspectable supervision trees

`OrderedTree` and `DynamicTree` are single-use, identity-owning
declarations. Actors are placed directly in them, so typed wiring and failure
topology are expressed together.

## Recursive composition

```rust
# use kokage::prelude::*;
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let mut workers = OrderedTree::new().strategy(Strategy::OneForAll);
workers.add_actor(ActorSpec::new("parse", || Worker));
workers.add_actor(ActorSpec::new("index", || Worker));

let mut tree = OrderedTree::new().strategy(Strategy::OneForOne);
tree.add_actor(ActorSpec::new("ingest", || Worker));
tree.add_subtree("workers", workers);
# let _ = tree;
```

A scope owns one namespace. Actor and subtree ids must be non-empty and unique
among that scope's direct children. Sibling scopes have independent namespaces,
so reusing `worker` beneath each is legal.

Child order is behavior in an ordered scope. Startup waits for each declared
child's readiness before starting the next, shutdown visits children in reverse
order, and `Strategy::RestForOne` restarts the failed child plus the suffix
declared after it. A dynamic scope has no declared sequence.

Restart and shutdown defaults apply to every direct child edge, including an
edge that wraps a subtree. The nested scope controls the defaults of its own
children. Mailbox-capacity defaults apply only to actors directly in the scope;
subtrees start from the standard default unless configured themselves.

Tree lowering happens at `spawn` and at dynamic `add_subtree`. Invalid
mailbox defaults, invalid ids, and duplicates are rejected before startup is
scheduled.

## Identity exists before spawn

Each tree has a stable reference as soon as it is created. This lets a factory
capture a future scope without a global cell:

```rust
# use kokage::prelude::*;
# struct Router(kokage::ScopeRef);
# impl kokage::Actor for Router { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let sessions = DynamicTree::new();
let sessions_ref = sessions.scope();
let router = ActorSpec::new("router", move || Router(sessions_ref.clone()));

let mut app = OrderedTree::new();
// Moving the nested tree transfers its identity into the root. Declaring
// it first also makes it ready before the dependent router starts.
app.add_subtree("sessions", sessions);
app.add_actor(router);
let app_ref = app.scope();
# let _ = app_ref;
```

Moving a tree into `add_subtree` transfers ownership, while previously issued
references continue to address the same identity.

Use `SubtreeSpec` when the nested scope's edge needs policies distinct from its
siblings. `restart` and `shutdown` configure how the parent restarts or stops
the whole subtree; the tree's `default_restart` and `default_shutdown` still
apply inside it:

```rust
# use kokage::{SubtreeSpec, prelude::*};
let workers = OrderedTree::new();
let mut app = OrderedTree::new();
app.add_subtree(
    "workers",
    SubtreeSpec::from(workers)
        .restart(Restart::never())
        .shutdown(Shutdown::abort()),
);
# let _ = app;
```

Trees deliberately do not implement `Clone`: one identity binds to one
runtime. Before binding, control operations return `ControlError::Unavailable`,
while projected snapshots and subscriptions are already usable. `spawn()`
consumes the tree and returns its owning runtime. Dropping an unspawned tree or
failing to spawn it makes issued references terminal.

Dropping a non-owning reference does not stop a runtime. Dropping the owning
`RunningTree` requests graceful shutdown, so `let _ = tree.spawn()?;` is a footgun:
the temporary owner is dropped at the end of the statement.

## Inspect the declaration

`outline()` returns an `observe::SupervisionOutline` without spawning:

```rust
# use kokage::prelude::*;
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let mut tree = OrderedTree::new().strategy(Strategy::RestForOne);
tree.add_actor(ActorSpec::new("ingest", || Worker));
tree.add_actor(ActorSpec::new("parse", || Worker));
let outline = tree.outline();
assert_eq!(outline.strategy, Strategy::RestForOne);
assert_eq!(outline.child_ids(), ["ingest", "parse"]);
```

Use the outline for configuration tests and topology review. Runtime snapshots
and lifecycle watches cover live state.

## Leader-owned scopes

Represent an actor and its workers with an explicit nested tree:

```rust
# use kokage::prelude::*;
# struct Leader;
# impl kokage::Actor for Leader { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let mut session_runtime = OrderedTree::new().strategy(Strategy::OneForAll);
session_runtime.add_actor(ActorSpec::new("leader", || Leader));
session_runtime.add_subtree("children", DynamicTree::new());

let mut sessions = OrderedTree::new();
sessions.add_subtree("session-runtime", session_runtime);
# let _ = sessions;
```

Inside the leader, resolve the declared scope explicitly with
`ctx.scope().subtree("children")`. The lookup works during `on_start`, before
the child scope has started, and returns a `ScopeRef`. Do not await that
scope's readiness from the callback that must return before startup can
continue. Check `kind()` when the tree shape is not already known;
membership operations return `ControlError::NotDynamic` on an ordered scope.

The containing strategy states the fate-sharing relationship. For example,
`OneForAll` restarts the leader when a restartable worker failure exhausts
the inner scope, while `RestForOne` respects declaration order.

[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`DynamicTree`]: https://stokes.io/kokage/api/kokage/struct.DynamicTree.html
[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`ActorSpec`]: https://stokes.io/kokage/api/kokage/struct.ActorSpec.html
[`observe::SupervisionOutline`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisionOutline.html
[`ScopeKind`]: https://stokes.io/kokage/api/kokage/observe/enum.ScopeKind.html
[`observe::SupervisorSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisorSnapshot.html
