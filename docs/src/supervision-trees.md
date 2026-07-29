# Inspectable supervision trees

`OrderedTree` and `DynamicTree` are single-use, identity-owning
declarations. Actors are placed directly in them, so typed wiring and failure
topology are expressed together.

## Recursive composition

```rust
# use kokage::{ActorSpec, OrderedTree, Strategy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let tree = OrderedTree::new()
    .strategy(Strategy::OneForOne)
    .actor(ActorSpec::new("ingest", || Worker))
    .subtree(
        "workers",
        OrderedTree::new()
            .strategy(Strategy::OneForAll)
            .actor(ActorSpec::new("parse", || Worker))
            .actor(ActorSpec::new("index", || Worker)),
    );
# let _ = tree;
```

A scope owns one namespace. Actor and subtree ids must be non-empty and unique
among that scope's direct children. Sibling scopes have independent namespaces,
so reusing `worker` beneath each is legal.

Tree lowering happens at `spawn` and at dynamic `add_subtree`. Invalid
mailbox defaults, invalid ids, and duplicates are rejected before startup is
scheduled.

## Identity exists before spawn

Each tree has a stable handle as soon as it is created. This lets a factory
capture a future scope without a global cell:

```rust
# use kokage::{ActorSpec, DynamicTree, OrderedTree};
# struct Router(kokage::DynamicRuntimeHandle);
# impl kokage::Actor for Router { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let sessions = DynamicTree::new();
let sessions_handle = sessions.handle();
let router = ActorSpec::new("router", move || Router(sessions_handle.clone()));

let app = OrderedTree::new()
    .actor(router)
    .subtree("sessions", sessions);
let handle = app.handle();
# let _ = handle;
```

Moving a tree into `subtree` transfers ownership, while previously issued
handles continue to address the same identity.

## Inspect the declaration

`outline()` returns an `observe::SupervisionOutline` without spawning:

```rust
# use kokage::{ActorSpec, OrderedTree, Strategy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let tree = OrderedTree::new()
    .strategy(Strategy::RestForOne)
    .actor(ActorSpec::new("ingest", || Worker))
    .actor(ActorSpec::new("parse", || Worker));
let outline = tree.outline();
assert_eq!(outline.strategy, Strategy::RestForOne);
assert_eq!(outline.child_ids(), ["ingest", "parse"]);
```

Use the outline for configuration tests and topology review. Runtime snapshots
and lifecycle watches cover live state.

## Leader-owned scopes

Represent an actor and its workers with an explicit nested tree:

```rust
# use kokage::{ActorSpec, DynamicTree, OrderedTree, Strategy};
# struct Leader;
# impl kokage::Actor for Leader { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let sessions = OrderedTree::new().subtree(
    "session-runtime",
    OrderedTree::new()
        .strategy(Strategy::OneForAll)
        .actor(ActorSpec::new("leader", || Leader))
        .subtree("children", DynamicTree::new()),
);
# let _ = sessions;
```

Inside the leader, resolve the declared scope explicitly with
`ctx.supervisor().subtree("children")`. The lookup works during
`on_start`, before the child scope has started, and returns a
`RestrictedScope`. Call `dynamic()` on it to insert actor specs without
exposing lifecycle waits that could deadlock an actor callback.

The containing strategy states the fate-sharing relationship. For example,
`OneForAll` restarts the leader when a restartable worker failure exhausts
the inner scope, while `RestForOne` respects declaration order.
