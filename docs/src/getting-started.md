# Getting started

## Dependencies

The crates are not yet published to crates.io, so use git dependencies (or
path dependencies if you are working inside this repository). `kokage` is
the one dependency needed for the actor product. The opening chapters also
construct raw task supervisors, so they name `kokage-supervisor` directly:

```toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
kokage = { git = "https://github.com/ralexstokes/kokage" }
kokage-supervisor = { git = "https://github.com/ralexstokes/kokage" }
```

`kokage::prelude` covers the day-one actor product surface and core policies.
Raw supervisor construction and control types stay in `kokage-supervisor`; if
tasks are all you need, depend on that crate alone.

## Your first supervised task

Before the print shop opens for business, let's supervise the simplest thing
possible: a heartbeat task. A supervisor is built from one or more
[`ChildSpec`]s. Each child spec pairs an *async factory* — a closure the
supervisor calls every time it needs to (re)start the child — with restart and
shutdown policies.

```rust,no_run
use std::time::Duration;

use kokage_supervisor::{ChildSpec, Supervisor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = Supervisor::ordered()
        .child(ChildSpec::task("heartbeat", |ctx| async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = ctx.shutdown_token().cancelled() => {
                        println!("heartbeat asked to stop");
                        return Ok(());
                    }
                    _ = ticker.tick() => {
                        println!("beat (generation {})", ctx.generation());
                    }
                }
            }
        }))
        .build()?;

    let handle = supervisor.spawn();

    tokio::time::sleep(Duration::from_secs(2)).await;

    handle.shutdown_and_wait().await?;
    println!("supervisor stopped");
    Ok(())
}
```

A few things worth noticing:

- **The factory receives a [`ChildContext`]** (`ctx`). It carries the child's
  `id`, its `generation` (0 for the first spawn, incremented on every
  restart), and a `token` — a `CancellationToken` the supervisor cancels when
  the child should stop. Well-behaved children select on it.
- **The child returns `Result<(), BoxError>`.** Returning `Ok(())` is a clean
  exit; returning an `Err`, panicking, or being aborted counts as a failure.
  The restart policy decides what happens next.
- **`spawn()` returns a [`SupervisorHandle`].** This is your control surface:
  shut the tree down, add or remove children, subscribe to lifecycle events,
  or grab a state snapshot. To drive the supervisor in the foreground, follow
  `spawn()` with `handle.wait().await` — and note that dropping the last
  handle clone requests graceful shutdown, so fire-and-forget operation means
  keeping a handle alive.

Run it and you'll see the heartbeat tick until the shutdown request cancels
its token:

```text
beat (generation 0)
beat (generation 0)
beat (generation 0)
beat (generation 0)
heartbeat asked to stop
supervisor stopped
```

So far the child never fails, so the supervisor has nothing interesting to do.
Let's fix that.

[`ChildSpec`]: https://stokes.io/kokage/api/kokage/host/struct.ChildSpec.html
[`ChildContext`]: https://stokes.io/kokage/api/kokage_supervisor/struct.ChildContext.html
[`SupervisorHandle`]: https://stokes.io/kokage/api/kokage_supervisor/struct.SupervisorHandle.html
