# Dynamic Trees

Everything so far was declared up front: the tree's children were known
before `spawn`. Plenty of real systems aren't like that — a session per
connected user, a worker per submitted job, a press per walk-in client. A
[`DynamicTree`] is a supervision scope whose membership changes *at runtime*.

## Adding and removing children at runtime

```rust
use kokage::prelude::*;

struct Press {
    client: &'static str,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("[{}] printed: {job}", self.client);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running_tree = DynamicTree::new().spawn()?;
    let scope = running_tree.scope();

    // A big client walks in: give them a dedicated press.
    let acme = scope
        .add_actor("acme-press", || Press { client: "acme" })
        .await?;
    acme.send("letterhead x1000".to_owned()).await?;

    // Contract over: the press drains its queue and leaves the tree.
    scope.remove(&acme).await?;

    running_tree.shutdown().await?;
    Ok(())
}
```

A dynamic tree starts empty (spawning an empty one is perfectly legal) and
always supervises `OneForOne` — group strategies need a stable group, so
they belong to ordered scopes. Membership is managed through the
[`DynamicScopeRef`]:

- `scope.add_actor(id, factory).await?` — returns the typed `ActorRef`.
- `scope.add_task(id, task).await?` — supervised tasks work too.
- `scope.spawn_once(id, task).await?` — finite, non-restarting work that removes
  its membership on completion.
- `scope.add_subtree(id, tree).await?` — insert a whole *ordered or dynamic*
  subtree, and get back the new scope's `ScopeRef`.
- `scope.remove(&child).await?` — stop and remove the exact actor, task, or
  subtree membership represented by a returned handle. A stale handle cannot
  remove a same-id replacement.
- `scope.remove_named(id).await?` — the id-based escape hatch for an
  external registry or operator command.

The adjacent `add_actor_spec`, `add_task_spec`, and `spawn_once_spec` forms
accept explicitly configured declarations. `OneShotTaskSpec` preserves a
consuming factory while configuring shutdown, readiness, or whether the
terminal membership remains visible. `add_subtree` accepts a `SubtreeSpec`
directly when the subtree edge needs policy overrides.

These operations exist only on `DynamicScopeRef`, so an ordered scope cannot
be mutated accidentally. They return [`ControlError`] for operational errors:
`UnknownChildId`, `ChildRemovalInProgress` if you re-add an id whose removal
hasn't finished, or `Rejected` wrapping the same validation a static `spawn`
performs. Ids must be unique within the scope at any moment — a removed id may
be reused afterwards. When traversal returns an ordinary `ScopeRef`, call
`scope.dynamic()` to request the mutation capability after checking its kind.
To retain a nested dynamic subtree's mutation capability, call
`dynamic_tree.scope()` before moving the declaration into `add_subtree`, as in
the static-skeleton example below.

## Mixing static structure with dynamic membership

The usual shape is not "everything dynamic" but a **static skeleton with
dynamic compartments**. Declare the dynamic tree, take its scope *before
spawning*, and nest it in the ordered tree; the handle becomes live once the
tree spawns:

```rust
# use kokage::prelude::*;
# struct Session;
# impl Actor for Session {
#     type Msg = ();
#     async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let dynamic_tree = DynamicTree::new();
let sessions = dynamic_tree.scope(); // usable for wiring before spawn

let mut shop = Tree::new();
// ... static children: front desk, press room ...
shop.add_subtree("sessions", dynamic_tree);
let running_tree = shop.spawn()?;
// `spawn` returns while children are still starting; wait for the tree to
// come up before the dynamic scope can accept members.
running_tree.scope().wait_started().await?;

// Later, as clients arrive:
let session = sessions
    .add_actor("session-1", || Session)
    .await?;
# let _ = session;
# running_tree.shutdown().await?;
# Ok(())
# }
```

Actors inside the static skeleton can do the same from within — a front-desk
actor holding the `sessions` scope (it is cheaply cloneable) can spawn a
session actor per request, hand out its `ActorRef`, and remove it when the
client leaves.

## One-shot work: run to completion, then clean up

`spawn_once` gives dynamic trees straightforward batch semantics. Finished
work leaves the scope; the returned `TaskRef` remains tied to that exact
membership and preserves its completion even when the task exits quickly. Its
factory is `FnOnce`, so it can consume owned inputs:

```rust
# use kokage::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let batch = DynamicTree::new();
let scope = batch.scope();
let running_tree = batch.spawn()?;

let job = scope
    .spawn_once("job-42", |_ctx| async move { Ok(()) })
    .await?;

let exit = job.wait().await?;
assert!(exit.is_completed());
scope.request_shutdown();
running_tree.wait().await?;
# Ok(())
# }
```

Use `scope.spawn_once_spec(OneShotTaskSpec::new(...))` when the same consuming
factory needs a custom shutdown policy, readiness gate, or retained terminal
membership. Restart settings are deliberately unavailable because the factory
cannot create a second incarnation. Select `.retain_when_done()` when
scope-level snapshot observers must discover the terminal state without an
existing `TaskRef`. A retained membership keeps its child id occupied until
`scope.remove(&job).await?` removes it or the scope shuts down; the
`TaskRef` retains the terminal outcome whether or not the membership is kept.

`TaskRef::wait` skips exits followed by the task's restart policy and returns
the terminal `ExitStatus`. Use `tokio::try_join!` or a task set when several
jobs must finish, then request the enclosing scope's shutdown explicitly.

## What restarts cannot restore

One sharp edge deserves a box around it: **runtime-added children are not
part of any declaration.** If a *subtree* fails hard enough that its parent
restarts it, the replacement scope is rebuilt from its static declaration —
which for a dynamic scope means *empty*. Children added through a `DynamicScopeRef`
are gone, and the refs you held for them are terminal.

If dynamic membership must survive that, own the roster somewhere: keep a
directory actor (see the repository's
[`directory.rs`](https://github.com/ralexstokes/kokage/blob/main/crates/kokage/examples/directory.rs)
example) or other userland record of what should exist, watch the scope, and
re-add on restart. The library deliberately does not guess this for you —
replaying stale membership after a wipeout is a policy decision, not a
mechanism.

[`DynamicTree`]: https://stokes.io/kokage/api/kokage/struct.DynamicTree.html
[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`DynamicScopeRef`]: https://stokes.io/kokage/api/kokage/struct.DynamicScopeRef.html
[`ControlError`]: https://stokes.io/kokage/api/kokage/enum.ControlError.html
[`TaskRef`]: https://stokes.io/kokage/api/kokage/struct.TaskRef.html
