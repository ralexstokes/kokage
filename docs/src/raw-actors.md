# Raw Actors

The [`Actor`] trait hands you a message-at-a-time loop: receive, `handle`,
repeat, drain on shutdown. That framework-owned loop is right for almost
everything — but "almost" earns this chapter. When you need to *own the
loop* — batch greedily, select over a socket alongside the mailbox, or
implement a custom drain — drop one level down to [`raw::RawActor`].

## Owning the receive loop

A raw actor implements `run`, which receives the [`raw::RawContext`] by
value and *is* the actor's whole life:

```rust
use kokage::{
    prelude::*,
    raw::{RawActor, RawContext},
};

struct BatchPress;

impl RawActor for BatchPress {
    type Msg = String;

    async fn run(&mut self, mut ctx: RawContext<String>) -> ExitResult {
        // recv() returns None as soon as shutdown is requested.
        while let Some(job) = ctx.recv().await {
            // Greedily gather whatever else is already queued.
            let mut batch = vec![job];
            while let Some(more) = ctx.try_recv() {
                batch.push(more);
            }
            println!("printing a batch of {} jobs", batch.len());
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Raw actors are declared and supervised exactly like handler actors.
    let mut tree = Tree::new();
    let press = tree.add_actor("batch-press", || BatchPress);
    let runtime = tree.spawn()?;

    for n in 1..=4 {
        press.send(format!("job {n}")).await?;
    }

    runtime.shutdown().await?;
    Ok(())
}
```

Nothing upstream changes: the same [`ActorSpec`], the same trees, policies,
refs, and observability. In fact every `Actor` *is* a `RawActor` — the
handler trait is a blanket implementation whose `run` is the provided loop.
You are swapping the loop, not the ecosystem.

The contract you take on:

- **Return promptly once `recv()` yields `None`** — shutdown has been
  requested and your grace period is ticking. Queued-but-undelivered
  messages can be drained with `try_recv()` first; that choice (and how much
  of it) is now yours. `Ok(())` is a clean exit, `Err` is a failure with the
  usual supervised consequences.
- **Everything else arrives through the context**: `myself()`, `watch`,
  `offload`, `run_blocking`, `send_after` / `interval` timers,
  `shutdown_token()`, and `scope()` are all available on `RawContext`, so a
  custom loop gives up none of the toolkit.
- If the supervisor should hold later ordered siblings until you have
  finished initializing, override `readiness_gated()` to return `true` and
  call `ctx.mark_ready()` when ready — the raw analogue of a task's
  `wait_for_ready`.

## Hosting an actor without a tree

Supervision trees are the normal host, but sometimes the actor must live
inside somebody else's runtime — a test harness, an existing task system.
Enable the opt-in `host` Cargo feature for this integration surface.
[`ActorSpec::into_host`] surrenders one declared actor as an owning
[`raw::ActorHost`] you drive yourself:

```rust
use kokage::{CancellationToken, prelude::*, raw::DEFAULT_SHUTDOWN_BOUND};

struct Worker;

impl Actor for Worker {
    type Msg = u32;

    async fn handle(&mut self, n: u32, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("crunching {n}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec = ActorSpec::new("worker", || Worker);
    let worker = spec.actor_ref(); // grab the ref before consuming the spec
    let host = spec.into_host();

    let stop = CancellationToken::new();
    let run = tokio::spawn({
        let stop = stop.clone();
        async move {
            host.run_once(stop.cancelled(), Shutdown::graceful_for(DEFAULT_SHUTDOWN_BOUND))
                .await
        }
    });

    worker.send(7).await?;

    stop.cancel();
    run.await??;
    Ok(())
}
```

`run_once` consumes the host: it drives one incarnation until the given
future resolves, applies the [`Shutdown`] policy on the way out, and
terminates the mailbox binding — so `ActorRef` senders fail fast afterwards
instead of waiting for a rebind that cannot happen.

Restarting is *your* loop's job in this mode. `run_incarnation` takes the
host by `&mut` and returns an [`raw::IncarnationExit`] instead of consuming:
the binding stays alive between calls, so the same `ActorRef` handles carry
over to the next incarnation you start — inspect the exit (catching panics
yourself if you intend to survive them) and either go again or drop the host
to make the binding terminal. The rustdoc for
[`run_incarnation`](https://stokes.io/kokage/api/kokage/raw/struct.ActorHost.html#method.run_incarnation)
walks through a complete hand-rolled supervision loop.

Two things a directly hosted actor gives up: there is no supervisor
applying a [`RestartPolicy`] for you, and the actor's `ctx.scope()` is a
stub — control operations fail and observation streams are closed. The
escape hatch is deliberately shaped like one: reach for it at the edges,
embedding kokage actors in a foreign framework, not as an alternative
architecture.

[`Actor`]: https://stokes.io/kokage/api/kokage/trait.Actor.html
[`raw::RawActor`]: https://stokes.io/kokage/api/kokage/raw/trait.RawActor.html
[`raw::RawContext`]: https://stokes.io/kokage/api/kokage/raw/struct.RawContext.html
[`ActorSpec`]: https://stokes.io/kokage/api/kokage/struct.ActorSpec.html
[`ActorSpec::into_host`]: https://stokes.io/kokage/api/kokage/struct.ActorSpec.html#method.into_host
[`raw::ActorHost`]: https://stokes.io/kokage/api/kokage/raw/struct.ActorHost.html
[`raw::IncarnationExit`]: https://stokes.io/kokage/api/kokage/raw/enum.IncarnationExit.html
[`RestartPolicy`]: https://stokes.io/kokage/api/kokage/struct.RestartPolicy.html
[`Shutdown`]: https://stokes.io/kokage/api/kokage/enum.Shutdown.html
