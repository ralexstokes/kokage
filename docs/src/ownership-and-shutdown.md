# Ownership and Shutdown

You have been typing `runtime.shutdown_and_wait()` since chapter two. This
chapter makes the machinery underneath precise: who owns a running tree, how
stopping propagates, and how long anything is allowed to take on the way
down.

## The owner and the references

`spawn` returns a [`RunningTree`] — the unique **owner** of the whole
supervision tree. Ownership here is literal Rust ownership:

- Keep it alive for as long as the application should run.
- **Dropping it requests graceful shutdown.** The type is `#[must_use]`
  because `let _ = tree.spawn()?;` discards the owner on the spot and shuts
  the tree down immediately — a classic first-day surprise.
- `shutdown()` requests shutdown and returns; `shutdown_and_wait().await`
  requests and waits for completion; `wait().await` just waits (for a tree
  that ends by other means — its own failure escalation, or a `shutdown()`
  from elsewhere). The waiting forms return a [`SupervisorError`] when the
  tree ended badly: startup aborted, restart intensity exceeded, or a
  shutdown that timed out.

Everything else holds a [`ScopeRef`] — the cheap, cloneable, *non-owning*
reference to a supervision scope, parallel to what `ActorRef` is for one
actor. `runtime.scope()` gives the root's; `scope.subtree("press-room")`
navigates down; trees hand them out even before spawn. A `ScopeRef` never
keeps the tree alive, and dropping one means nothing. What it does carry is
*control capability*: observation (snapshots, lifecycle watches, stats — see
[Observability](observability.md)), dynamic membership on dynamic scopes,
and targeted shutdown — `scope.shutdown_and_wait()` on a subtree stops just
that compartment, whose parent then sees a completed child.

## Shutdown policies: how children stop

Shutdown flows down the tree in reverse declaration order, and each child
stops according to its [`Shutdown`] policy:

- `Shutdown::drain_for(grace)` — the default (with a 5-second grace): stop
  accepting new messages, finish what is already queued, then run `on_stop`.
- `Shutdown::discard_after_current(grace)` — finish only the message in
  flight; drop the rest of the queue.
- `Shutdown::abort()` — cancel the future outright. For a subtree, the abort
  cascades through everything inside.

The `grace` is a bound, not a hope: a child still busy when it expires is
aborted, and the abort is recorded distinctly (`Aborted { after_grace: true,
.. }` in observation types, a `shutdown_timed_out` status in logs) so a
too-slow drain is visible rather than mysterious. Tasks get an escalation
hook: their [`TaskContext::abort_token`] fires when the grace has expired.

Watch a drain do its job:

```rust
use std::time::Duration;

use kokage::prelude::*;

struct Press;

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        tokio::time::sleep(Duration::from_millis(5)).await; // each job takes a moment
        println!("printed: {job}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec = ActorSpec::new("press", || Press)
        .shutdown(Shutdown::drain_for(Duration::from_secs(5)));
    let mut tree = OrderedTree::new();
    let press = tree.add_actor(spec);
    let runtime = tree.spawn()?;

    for n in 1..=5 {
        press.send(format!("job {n}")).await?;
    }

    // All five jobs print before this returns.
    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

Set policies per child (`ActorSpec::shutdown`, `TaskSpec::shutdown`), per
scope default (`default_shutdown`), or on a subtree's edge
(`SubtreeSpec::shutdown`). Inside an actor, `ctx.status()` reports
`Draining` while a drain is in progress, and hand-written loops can check
their [`Context::shutdown_token`].

## Guards: scoped ownership for background operations

By now you have met [`Guard`] several times; here is the general rule it
encodes. Every background operation an actor or scope starts — watches,
intervals, delayed sends, offloads, lifecycle pumps, completion-triggered
shutdowns — returns a `Guard`, and **dropping the guard cancels the
operation**.

This is the same philosophy as `RunningTree` ownership, miniaturized:
background work is always owned by *something*, so nothing can outlive its
reason for existing by accident. Your options, in order of preference:

1. **Store it** where its lifetime belongs (actor state, a struct field) —
   cancellation becomes automatic and correct.
2. **`guard.cancel()`** to stop early; `is_finished()` / `finished().await`
   observe natural completion.
3. **`guard.detach()`** when fire-and-forget is genuinely intended — an
   explicit, greppable decision, not a default.

## The application edge

Something outside the tree decides when the program ends: a signal handler,
a management endpoint. Kokage's crate-owned [`CancellationToken`] bridges
external triggers to tree shutdown without leaking scheduler types into your
API:

```rust
use std::time::Duration;

use kokage::{CancellationToken, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("service", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let runtime = tree.spawn()?;

    let stop = CancellationToken::new();
    stop.cancel_when(async {
        // stand-in for ctrl-c / SIGTERM / an admin endpoint
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("shutdown requested");
    });

    stop.cancelled().await;
    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

Tokens form trees of their own (`child_token()` cancels downward, never
upward), so one root token can fan out to subsystems beyond kokage while the
`RunningTree` remains the single authority over the actor tree itself.

[`RunningTree`]: https://stokes.io/kokage/api/kokage/struct.RunningTree.html
[`SupervisorError`]: https://stokes.io/kokage/api/kokage/enum.SupervisorError.html
[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`Shutdown`]: https://stokes.io/kokage/api/kokage/enum.Shutdown.html
[`TaskContext::abort_token`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html#method.abort_token
[`Context::shutdown_token`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.shutdown_token
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
[`CancellationToken`]: https://stokes.io/kokage/api/kokage/struct.CancellationToken.html
