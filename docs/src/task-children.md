# Task Children

Not everything is an actor. A cache warmer, a metrics flusher, a listener
loop accepting connections — some children are just *futures* that should be
supervised like everything else. `Tree::add_task` declares an arbitrary async
task with default configuration; [`TaskSpec`] is the adjacent explicit form
for readiness and policy overrides. Tasks have the same restart behavior,
shutdown timing, and observability as actors.

## Declaring a task

```rust
use std::time::Duration;

use kokage::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();

    tree.add_task_spec(
        TaskSpec::new("cache-warmer", |ctx| async move {
            println!("warming cache (generation {})", ctx.generation());
            // ... load things ...
            ctx.mark_ready();

            // Then hold the warm cache until shutdown.
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .manual_readiness(Duration::from_secs(10)),
    );

    tree.add_task("api", |ctx| async move {
        println!("api serving (cache is warm)");
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });

    let running_tree = tree.spawn()?;
    running_tree.scope().wait_started().await?;
    running_tree.shutdown().await?;
    Ok(())
}
```

The closure you give `add_task` (or the explicitly configured
[`TaskSpec::new`]) receives a [`TaskContext`] and returns a future ending in
[`ExitResult`] — the same contract as an actor: `Ok(())` is a clean
completion, `Err` is a failure the supervisor answers with the child's
restart policy.

Because the closure is a factory (`Fn`, not `FnOnce`), it is called again for
every restart — exactly like an actor factory. State captured by the closure
must therefore be cloned *inside* it:

```rust
# use std::sync::Arc;
# use kokage::prelude::*;
# let jobs: Arc<Vec<String>> = Arc::new(vec![]);
let spec = TaskSpec::new("indexer", move |ctx| {
    let jobs = Arc::clone(&jobs);     // per-incarnation clone
    async move {
        let _ = (&jobs, ctx);
        // ... use jobs ...
        Ok(())
    }
});
# let _ = spec;
```

## Cooperating with shutdown

A task learns about shutdown through its context:

- [`shutdown_token`] — a [`CancellationToken`] cancelled when the scope wants
  the task to stop. A well-behaved loop `select!`s on it (or awaits
  `cancelled()` as above) and returns `Ok(())`.
- [`abort_token`] — cancelled when the *grace period has expired* and the
  supervisor is about to abort the future outright. Use it for last-resort
  cleanup of tasks whose work can't be interrupted mid-await.

A task that ignores its tokens is not stuck forever: shutdown policies bound
the wait, and after the grace period the future is aborted (recorded as an
aborted exit, not a clean one).

## Startup ordering with readiness

In an ordered scope, children normally start one after another as soon as
each future is spawned. When a later child genuinely depends on an earlier
one having *finished doing something* — the API above needs the cache warm —
pair [`manual_readiness`] on the spec with [`mark_ready`] in the task: the
supervisor holds back the next declared child until the mark. The supplied
deadline bounds startup; missing it is a failure handled by the task's restart
policy.

## Supervised service policies

Tasks take the same per-child configuration as actors:

```rust
# use std::time::Duration;
# use kokage::{RestartPolicy, Shutdown, prelude::*};
let spec = TaskSpec::new("indexer", |ctx| async move {
    ctx.shutdown_token().cancelled().await;
    Ok(())
})
    .restart(RestartPolicy::on_failure())
    .shutdown(Shutdown::abort());
# let _ = spec;
```

`TaskSpec` takes a repeatable `Fn` factory because the supervisor may need to
create another incarnation. Finite dynamic work has a separate, narrower
declaration: `OneShotTaskSpec` takes a consuming `FnOnce` factory, never
restarts, and removes its membership by default. Its concise entry point is
`DynamicScopeRef::spawn_once`; see [Dynamic Trees](dynamic-trees.md).

`ctx.generation()` tells a task which incarnation it is (0 for the first
run), which is handy for logging and for warm-up work that only the first
incarnation should do.

[`TaskSpec`]: https://stokes.io/kokage/api/kokage/struct.TaskSpec.html
[`TaskSpec::new`]: https://stokes.io/kokage/api/kokage/struct.TaskSpec.html#method.new
[`TaskContext`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html
[`ExitResult`]: https://stokes.io/kokage/api/kokage/type.ExitResult.html
[`shutdown_token`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html#method.shutdown_token
[`abort_token`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html#method.abort_token
[`CancellationToken`]: https://stokes.io/kokage/api/kokage/struct.CancellationToken.html
[`manual_readiness`]: https://stokes.io/kokage/api/kokage/struct.TaskSpec.html#method.manual_readiness
[`mark_ready`]: https://stokes.io/kokage/api/kokage/struct.TaskContext.html#method.mark_ready
