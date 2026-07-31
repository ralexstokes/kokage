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
    let runtime = DynamicTree::new().spawn()?;
    let scope = runtime.scope();

    // A big client walks in: give them a dedicated press.
    let acme = scope
        .add_actor("acme-press", || Press { client: "acme" })
        .await?;
    acme.send("letterhead x1000".to_owned()).await?;

    // Contract over: the press drains its queue and leaves the tree.
    scope.remove_child("acme-press").await?;

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

A dynamic tree starts empty (spawning an empty one is perfectly legal) and
always supervises `OneForOne` — group strategies need a stable group, so
they belong to ordered scopes. Membership is managed through the
[`ScopeRef`]:

- `scope.add_actor(id, factory).await?` — returns the typed `ActorRef`.
- `scope.add_task(id, task).await?` — supervised tasks work too.
- `scope.add_subtree(id, tree).await?` — insert a whole *ordered or dynamic*
  subtree, and get back the new scope's `ScopeRef`.
- `scope.remove_child(id).await?` — stop (honoring the child's shutdown
  policy, so a draining child finishes its queue) and remove.

The adjacent `add_actor_spec` and `add_task_spec` forms accept explicitly
configured declarations. `add_subtree` accepts a `SubtreeSpec` directly when
the subtree edge needs policy overrides.

All four are `async` and return [`ControlError`] on misuse: `NotDynamic` if
the scope is ordered, `UnknownChildId`, `ChildRemovalInProgress` if you
re-add an id whose removal hasn't finished, or `Rejected` wrapping the same
validation a static `spawn` performs. Ids must be unique within the scope at
any moment — a removed id may be reused afterwards.

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
let runtime = shop.spawn()?;
// `spawn` returns while children are still starting; wait for the tree to
// come up before the dynamic scope can accept members.
runtime.wait_started().await?;

// Later, as clients arrive:
let session = sessions
    .add_actor("session-1", || Session)
    .await?;
# let _ = session;
# runtime.shutdown_and_wait().await?;
# Ok(())
# }
```

Actors inside the static skeleton can do the same from within — a front-desk
actor holding the `sessions` scope (it is cheaply cloneable) can spawn a
session actor per request, hand out its `ActorRef`, and remove it when the
client leaves.

## Job scopes: run to completion, then clean up

Dynamic trees plus two direct scope operations give you batch semantics.
`temporary()` makes a finished child leave the scope, while a completion
operation waits for the work or shuts the scope down:

```rust
# use kokage::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let batch = DynamicTree::new();
let scope = batch.scope();
let runtime = batch.spawn()?;

// Arm shutdown first so no fast completion can slip past it.
let done = scope.shutdown_when_future_children_complete(["job-42"])?;

scope
    .add_task_spec(TaskSpec::new("job-42", |_ctx| async move { Ok(()) }).temporary())
    .await?;

done.finished().await;
runtime.wait().await?;
# Ok(())
# }
```

`wait_for_children` counts a child as done when its current run exited cleanly
with no restart pending (children under `RestartMode::Always` never qualify —
they are services, not jobs). The explicitly named
`wait_for_future_children` and `shutdown_when_future_children_complete`
variants accept ids that have not been inserted yet. Shutdown triggers return
a [`Guard`], so dropping the owner revokes the operation.

## What restarts cannot restore

One sharp edge deserves a box around it: **runtime-added children are not
part of any declaration.** If a *subtree* fails hard enough that its parent
restarts it, the replacement scope is rebuilt from its static declaration —
which for a dynamic scope means *empty*. Children added through a `ScopeRef`
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
[`ControlError`]: https://stokes.io/kokage/api/kokage/enum.ControlError.html
[`wait_for_children`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.wait_for_children
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
