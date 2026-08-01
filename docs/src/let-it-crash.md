# Let It Crash

Presses jam. Networks flake. Parsers meet input nobody imagined. The
let-it-crash philosophy says: don't write defensive recovery code at every
site — let the actor fail, and let its supervisor put a fresh one in its
place.

## Failing on purpose

An actor fails by returning an `Err` from `handle` (or `on_start`), or by
panicking — both are treated as failures. Here is a press whose *first*
incarnation jams on a particular job:

```rust
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kokage::prelude::*;

struct Press {
    incarnation: usize,
    incarnations: Arc<AtomicUsize>,
}

impl Actor for Press {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.incarnation = self.incarnations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if self.incarnation == 0 && job == "jam" {
            return Err(io::Error::other("paper jam").into());
        }
        println!("[run {}] printed: {job}", self.incarnation);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let incarnations = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let press = tree.add_actor("press", {
        let incarnations = incarnations.clone();
        move || Press { incarnation: 0, incarnations: incarnations.clone() }
    });
    let running_tree = tree.spawn()?;

    press.send("flyers x500".to_owned()).await?;

    // Jam the press, then wait until the supervisor has restarted it.
    let scope = running_tree.scope();
    let baseline = scope.snapshot().child("press").expect("declared").generation;
    let mut snapshots = scope.subscribe_snapshots();
    press.send("jam".to_owned()).await?;
    snapshots
        .wait_for_child("press", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await?;

    // The same ref now reaches the replacement.
    press.send("business cards x100".to_owned()).await?;

    running_tree.shutdown().await?;
    Ok(())
}
```

Note what the *callers* had to do about the failure: nothing. The `press` ref
made before the crash delivers to the replacement afterwards. (The snapshot
dance in the middle is only there to make the demo deterministic — we wait
until the restart has happened before sending more. Snapshots get a proper
treatment in [Observability](observability.md).)

## What a restart means

When an actor fails, its supervisor:

1. tears down the failed run — its state is dropped, and its **mailbox dies
   with it**;
2. calls your factory to build a fresh actor value;
3. binds a fresh mailbox and runs `on_start` on the new incarnation.

The mailbox loss is deliberate and worth internalizing: messages accepted
behind the "poison" message are dropped with the failed run. Keeping them
would redeliver the poison and turn one crash into a crash loop. Senders
using `send` simply ride through the window; if you need certainty that a
particular message was processed, that's a [reply protocol](request-reply.md),
not a delivery guarantee.

## Restart policies

How eagerly should the supervisor restart? [`RestartPolicy`] selects which
exits restart and carries a budget and retry backoff:

```rust
# use std::time::Duration;
# use kokage::{Backoff, RestartPolicy, prelude::*};
let policy = RestartPolicy::on_failure()    // restart on failure only (the default)
    .limit(3, Duration::from_secs(10))      // at most 3 restarts within any 10s window
    .backoff(Backoff::exponential(
        Duration::from_millis(50),          // first delay
        2,                                  // multiply each time
        Duration::from_secs(1),             // cap
    ));
# let _ = policy;
```

- `RestartPolicy::on_failure()` — restart after errors, panics, and aborts; a clean
  exit stays down. This is the default.
- `RestartPolicy::always()` — restart even after a clean exit; for children that
  should run forever.
- `RestartPolicy::never()` — run at most once; failure is recorded, not retried.

Every restartable policy carries a restart *budget* — by default 5 restarts
within 30 seconds — and an optional [`Backoff`] (`fixed`, `exponential`, or
`exponential_with_jitter`) spacing the attempts. Attach a policy to one actor
with `ActorSpec::restart(...)`, or set a scope-wide default with
`Tree::default_restart(...)`.

The constructors and builders cover normal configuration. `RestartPolicy` is
also a public enum, so generic configuration code can match or construct its
`Always`, `OnFailure`, and `Never` variants directly. Restartable variants
carry their budget, window, and backoff fields directly; `Never` carries no
meaningless tuning. The fluent constructors remain the concise common path,
while the variants are the at-hand escape hatch for generic configuration.

The `serde` representation is likewise unversioned during `0.x`. Persisted
configuration that must survive Kokage upgrades should live behind an
application-owned versioned schema rather than deserializing `RestartPolicy`
directly forever.

```rust
# use std::time::Duration;
# use kokage::{RestartPolicy, prelude::*};
# struct Press;
# impl Actor for Press {
#     type Msg = String;
#     async fn handle(&mut self, _j: String, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
let spec = ActorSpec::new("press", || Press)
    .restart(RestartPolicy::on_failure().limit(5, Duration::from_secs(5)));
# let _ = spec;
```

## When restarting stops helping

If a child blows through its restart budget, restarting is evidently not
fixing the problem. The supervisor gives up on the *whole scope*: the scope
fails, and — if it is nested inside a larger tree — its parent now sees a
failed child and applies *its* policy. This escalation is the heart of
supervision-tree design: a persistent failure climbs the tree, taking out
progressively larger (but still bounded) parts of the system, until some
level either absorbs it or the root gives up and
`RunningTree::wait`/`shutdown` returns
`SupervisorError::RestartIntensityExceeded`.

Which brings us to shaping those trees.

[`RestartPolicy`]: https://stokes.io/kokage/api/kokage/enum.RestartPolicy.html
[`Backoff`]: https://stokes.io/kokage/api/kokage/enum.Backoff.html
