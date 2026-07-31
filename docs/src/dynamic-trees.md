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
        .add_actor(ActorSpec::new("acme-press", || Press { client: "acme" }))
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

- `scope.add_actor(spec).await?` — returns the typed `ActorRef`.
- `scope.add_task(spec).await?` — supervised tasks work too.
- `scope.add_subtree(id, tree).await?` — insert a whole *ordered or dynamic*
  subtree, and get back the new scope's `ScopeRef`.
- `scope.remove_child(id).await?` — stop (honoring the child's shutdown
  policy, so a draining child finishes its queue) and remove.

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

let mut shop = OrderedTree::new();
// ... static children: front desk, press room ...
shop.add_subtree("sessions", dynamic_tree);
let runtime = shop.spawn()?;
// `spawn` returns while children are still starting; wait for the tree to
// come up before the dynamic scope can accept members.
runtime.scope().wait_started().await?;

// Later, as clients arrive:
let session = sessions
    .add_actor(ActorSpec::new("session-1", || Session))
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

Dynamic trees plus two tools from earlier chapters give you batch semantics.
`remove_when_done` makes a finished child leave the scope; a **completion
watch** waits for the work:

```rust
# use kokage::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let batch = DynamicTree::new();
let scope = batch.scope();
let runtime = batch.spawn()?;

// Arm the watch first so no completion can slip past it.
let done = scope.completions(["job-42"]).allow_future_members();

scope
    .add_task(TaskSpec::new("job-42", |_ctx| async move { Ok(()) }).remove_when_done())
    .await?;

let outcome = done.wait().await?;
println!("batch finished: {outcome:?}");
# runtime.shutdown_and_wait().await?;
# Ok(())
# }
```

[`completions`] counts a child as done when its current run exited cleanly
with no restart pending (children under `Restart::always()` never qualify —
they are services, not jobs). `allow_future_members` lets the watch name
children that haven't been added yet, which also closes the race between
adding a fast job and watching for it. For a scope that should *shut itself
down* when the batch drains, `scope.completions(ids).then_shutdown()` returns
a [`Guard`] that does exactly that.

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
[`completions`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.completions
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
