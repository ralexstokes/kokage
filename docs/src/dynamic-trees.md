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

    runtime.shutdown().await?;
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
runtime.scope().wait_started().await?;

// Later, as clients arrive:
let session = sessions
    .add_actor("session-1", || Session)
    .await?;
# let _ = session;
# runtime.shutdown().await?;
# Ok(())
# }
```

Actors inside the static skeleton can do the same from within — a front-desk
actor holding the `sessions` scope (it is cheaply cloneable) can spawn a
session actor per request, hand out its `ActorRef`, and remove it when the
client leaves.

## Job scopes: run to completion, then clean up

`TaskRef` gives dynamic trees straightforward batch semantics. `temporary()`
makes finished work leave the scope; the ref remains tied to that exact
membership and preserves its completion even when the task exits quickly:

```rust
# use kokage::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let batch = DynamicTree::new();
let scope = batch.scope();
let runtime = batch.spawn()?;

let job = scope
    .add_task_spec(TaskSpec::new("job-42", |_ctx| async move { Ok(()) }).temporary())
    .await?;

let exit = job.wait().await?;
assert!(exit.is_completed());
scope.shutdown();
runtime.wait().await?;
# Ok(())
# }
```

`TaskRef::wait` skips exits followed by the task's restart policy and returns
the terminal `ExitStatus`. Use `tokio::try_join!` or a task set when several
jobs must finish, then request the enclosing scope's shutdown explicitly.

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
[`TaskRef`]: https://stokes.io/kokage/api/kokage/struct.TaskRef.html
