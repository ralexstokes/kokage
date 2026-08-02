# Ownership and Shutdown

You have been typing `running_tree.shutdown()` since chapter two. This
chapter makes the machinery underneath precise: who owns a running tree, how
stopping propagates, and how long anything is allowed to take on the way
down.

## The owner and the references

Both `Tree::spawn` and `DynamicTree::spawn` return a [`RunningTree`] — the
unique **owner** of the whole supervision tree. Each spawn method fixes the
root-scope type: `ScopeRef` for an ordered tree and `DynamicScopeRef` for a
dynamic tree.
Ownership here is literal Rust ownership:

- Keep it alive for as long as the application should run.
- **Dropping it requests graceful shutdown.** The type is `#[must_use]`
  because `let _ = tree.spawn()?;` discards the owner on the spot and shuts
  the tree down immediately — a classic first-day surprise.
- `shutdown().await` consumes the owner, requests shutdown, and waits for
  completion. `wait().await` also consumes the owner and just waits, for a
  tree that ends by its own failure escalation or a shutdown requested
  through a `ScopeRef`. Both return a [`SupervisorError`] when the tree ended
  badly: startup aborted, restart intensity exceeded, or a shutdown that
  timed out.

Everything else holds a [`ScopeRef`] — the cheap, cloneable, *non-owning*
reference to a supervision scope, parallel to what `ActorRef` is for one
actor. `running_tree.scope()` gives you a cloneable root ref for observation and
control. `scope.subtree("press-room")` navigates down, and trees hand refs out
even before spawn. A `ScopeRef` never keeps the tree alive, and dropping one
means nothing. What it does carry is *control capability*: observation
(snapshots, lifecycle event streams, stats — see
[Observability](observability.md)) and targeted shutdown. A [`DynamicScopeRef`]
additionally carries membership mutation. [`scope.shutdown_and_wait().await`]
on a subtree stops just that compartment, whose parent then sees a completed
child. [`scope.wait_stopped()`] only waits for a stop requested elsewhere,
while [`scope.request_shutdown()`] requests shutdown without waiting. Nested
scope references identify stable memberships rather than individual runtime
incarnations: `wait_stopped()` follows parent-driven restarts and returns only
when the scope identity is terminal, with the final incarnation's result.

An actor must not await either waiting method on its own enclosing scope: the
actor's exit is itself part of the condition being awaited. During shutdown,
that cycle lasts until the actor's shutdown policy aborts its callback, so the
actor can never observe the result. Moving the same wait into an offload avoids
blocking the callback, but still cannot deliver the result: the actor must exit
before the wait resolves, and its offloads are cancelled while it stops. Use
[`ctx.request_scope_shutdown()`] inside an actor and observe the scope's
completion from outside it. Waiting on a different scope is safe when that
scope can stop independently of the actor. The same non-waiting helper is
available on [`RawContext`] and [`StopContext`].

## Shutdown policies: how children stop

Shutdown flows down the tree in reverse declaration order. [`Shutdown`]
controls timing for every child, while actor-only [`MailboxShutdown`] decides
what happens to accepted messages:

- `Shutdown::graceful_for(grace)` — the default (with a 5-second grace):
  request cooperative shutdown and wait up to the bound.
- `Shutdown::abort()` — cancel the future outright. For a subtree, the abort
  cascades through everything inside.
- `MailboxShutdown::Drain` — the actor default: stop accepting messages,
  finish what is already queued, then run `on_stop`.
- `MailboxShutdown::Discard` — finish only the message in flight and drop the
  rest of the actor's queue.

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
        .shutdown(Shutdown::graceful_for(Duration::from_secs(5)))
        .mailbox_shutdown(MailboxShutdown::Drain);
    let mut tree = Tree::new();
    let press = tree.add_actor_spec(spec);
    let running_tree = tree.spawn()?;

    for n in 1..=5 {
        press.send(format!("job {n}")).await?;
    }

    // All five jobs print before this returns.
    running_tree.shutdown().await?;
    Ok(())
}
```

Set timing per child (`ActorSpec::shutdown`, `TaskSpec::shutdown`), per scope
default (`default_child_shutdown`), or on a subtree's edge
(`SubtreeSpec::shutdown`). Mailbox behavior is actor-only: set a scope's actor
default with `default_actor_mailbox_shutdown`, then override individual declarations
with `ActorSpec::mailbox_shutdown`. Inside an actor, `ctx.is_draining()` reports
whether queued work is being drained, and hand-written raw-actor loops can check their
[`raw::RawContext::shutdown_token`].

## Guards: scoped ownership for background operations

By now you have met [`Guard`] several times; here is the general rule it
encodes. An ordinary actor watch is owned by the two restart-stable actor
memberships, while an ordinary offload is owned by the current actor
incarnation. Those are the common lifetimes: a watch follows both actors across
restarts, while an offload is aborted instead of delivering stale work to a
replacement incarnation. Their `watch_scoped` and `offload_scoped` variants,
mailbox timers, lifecycle pumps, and other explicitly scoped background
operations return a `Guard`, and **dropping the guard cancels the operation**.

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
    let mut tree = Tree::new();
    tree.add_task("service", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let running_tree = tree.spawn()?;

    let stop = CancellationToken::new();
    stop.cancel_when(async {
            // stand-in for ctrl-c / SIGTERM / an admin endpoint
            tokio::time::sleep(Duration::from_millis(50)).await;
            println!("shutdown requested");
        })
        .detach();

    stop.cancelled().await;
    running_tree.shutdown().await?;
    Ok(())
}
```

Tokens form trees of their own (`child_token()` cancels downward, never
upward), so one root token can fan out to subsystems beyond kokage while the
`RunningTree` remains the single authority over the actor tree itself.

[`RunningTree`]: https://stokes.io/kokage/api/kokage/struct.RunningTree.html
[`SupervisorError`]: https://stokes.io/kokage/api/kokage/enum.SupervisorError.html
[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`DynamicScopeRef`]: https://stokes.io/kokage/api/kokage/struct.DynamicScopeRef.html
[`Shutdown`]: https://stokes.io/kokage/api/kokage/enum.Shutdown.html
[`MailboxShutdown`]: https://stokes.io/kokage/api/kokage/enum.MailboxShutdown.html
[`TaskContext::abort_token`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html#method.abort_token
[`raw::RawContext::shutdown_token`]: https://stokes.io/kokage/api/kokage/raw/struct.RawContext.html#method.shutdown_token
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
[`CancellationToken`]: https://stokes.io/kokage/api/kokage/struct.CancellationToken.html
[`scope.shutdown_and_wait().await`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.shutdown_and_wait
[`scope.wait_stopped()`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.wait_stopped
[`scope.request_shutdown()`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.request_shutdown
[`ctx.request_scope_shutdown()`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.request_scope_shutdown
[`RawContext`]: https://stokes.io/kokage/api/kokage/raw/struct.RawContext.html
[`StopContext`]: https://stokes.io/kokage/api/kokage/struct.StopContext.html
