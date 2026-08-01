# Lifecycle and Timers

So far our actors only reacted to messages from outside. This chapter is
about what an actor can do *on its own*: run setup and teardown code, stop
itself, keep working without new input, and schedule messages to its future
self.

## Lifecycle hooks

[`Actor`] has two optional hooks around the required `handle`:

```rust
use kokage::{BoxError, prelude::*};

struct Press;

impl Actor for Press {
    type Msg = String;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("warming up the rollers");
        Ok(())
    }

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("printing: {job}");
        Ok(())
    }

    async fn on_stop(&mut self, _ctx: &mut StopContext<'_, Self>) -> Result<(), BoxError> {
        println!("powering down heaters");
        Ok(())
    }
}
```

- `on_start` runs before any message, once per incarnation — including after
  every restart. This is the place for fallible or async setup (the factory
  closure itself is synchronous and infallible by design).
- `on_stop` runs after the receive loop has finished during a *clean* stop.
  It does **not** run when the actor failed — a crashed run gets no goodbye —
  so it must never be your only line of defense for critical cleanup.

`on_stop` receives a deliberately narrow [`StopContext`]: identity, the
shutdown token, `run_blocking`, and the scope — no timers, sends-to-self, or
other machinery for a world that is ending.

## Stopping yourself

Inside `handle`, [`Context::stop`] requests a clean stop: the current
callback finishes, the loop winds down (draining per the actor's mailbox
shutdown policy), and `on_stop` runs. A cleanly stopped actor is *done* under
the default `RestartPolicy::on_failure()` — only `RestartPolicy::always()` brings it
back.

[`Context::is_draining`] reports whether work queued by the current callback
can still run. The separate shutdown token answers whether runtime shutdown
has been requested.

## Working without input: `continue_with`

Sometimes one message means hours of work — printing a 500-page manual.
Doing it all in one `handle` call would make the actor deaf to everything
else (including shutdown) for the duration. [`Context::continue_with`]
enqueues a message to self for the next turn, letting you slice long work into
resumable steps:

```rust
# use kokage::prelude::*;
enum PressMsg {
    Print { title: String, pages_left: u32 },
}

struct Press;

impl Actor for Press {
    type Msg = PressMsg;

    async fn handle(&mut self, msg: PressMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        let PressMsg::Print { title, pages_left } = msg;
        if pages_left == 0 {
            println!("finished: {title}");
            return Ok(());
        }
        // print one page, then schedule the rest
        ctx.continue_with(PressMsg::Print { title, pages_left: pages_left - 1 });
        Ok(())
    }
}
```

Between steps the runtime can observe shutdown. After one continuation, a
ready mailbox message or offload completion gets a chance before another
continuation, so a long chain remains responsive to ordinary input.
Continuations bypass mailbox capacity, and any continuations still queued when
the actor stops are dropped (with a warning log), not delivered to the next
incarnation.

## Timers

Handler actors schedule future messages through loop-owned keyed timers:

```rust
use std::time::Duration;

use kokage::prelude::*;
use tokio::sync::mpsc;

const NIGHTLY: TimerKey = TimerKey::new("nightly-maintenance");

#[derive(Clone)]
enum PressMsg {
    Job(String),
    Maintain,
}

struct Press {
    maintained: mpsc::UnboundedSender<()>,
}

impl Actor for Press {
    type Msg = PressMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_interval(NIGHTLY, PressMsg::Maintain, Duration::from_millis(10));
        Ok(())
    }

    async fn handle(&mut self, msg: PressMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match msg {
            PressMsg::Job(job) => println!("printed: {job}"),
            PressMsg::Maintain => {
                println!("cleaning rollers");
                self.maintained.send(()).expect("receiver alive");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (maintained_tx, mut maintained_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let press = tree.add_actor("press", move || Press {
        maintained: maintained_tx.clone(),
    });
    let running_tree = tree.spawn()?;

    press.send(PressMsg::Job("posters x20".to_owned())).await?;
    maintained_rx.recv().await; // observe one maintenance pass
    running_tree.shutdown().await?;
    Ok(())
}
```

[`set_timeout`] arms a one-shot message, while [`set_interval`] repeats a
message and therefore requires `Msg: Clone`. Their semantics are tuned for
actor protocols:

- The [`TimerKey`] names the timer: setting the same key again *replaces* the
  pending one-shot or interval (perfect for deadline-style "reset on activity"
  logic), and [`clear_timer`] cancels either kind.
- Delivery bypasses mailbox capacity and conflation — a full mailbox cannot
  starve your deadline. A fire counts as received but not accepted mailbox
  traffic.
- Intervals skip missed ticks rather than accumulating catch-up work.
- Timers are owned by the current run: they are dropped on stop and restart,
  never fired into a fresh incarnation that doesn't remember arming them.
  Re-arm in `on_start` (as above) if the schedule should survive restarts.

[`send_after_to`] and [`interval_to`] deliberately use ordinary mailbox
delivery to any `ActorRef`; pass `&ctx.myself()` when backpressure or conflation
on self-delivery is part of the protocol. These operations are owned by their
returned [`Guard`], so retain it or call [`Guard::detach`] to keep them alive.
Raw actors have the same `_to` forms plus self-directed [`RawContext::send_after`]
and [`RawContext::interval`], because their custom loops do not have the keyed
timer table.

```rust
# use std::time::Duration;
# use kokage::prelude::*;
# enum PressMsg { Wake }
# struct Press;
# impl Actor for Press {
#     type Msg = PressMsg;
#     async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
ctx.send_after_to(&ctx.myself(), PressMsg::Wake, Duration::from_secs(30))
    .detach();
#         Ok(())
#     }
#     async fn handle(&mut self, _m: PressMsg, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
```

[`Actor`]: https://stokes.io/kokage/api/kokage/trait.Actor.html
[`StopContext`]: https://stokes.io/kokage/api/kokage/struct.StopContext.html
[`Context::stop`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.stop
[`Context::is_draining`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.is_draining
[`Context::continue_with`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.continue_with
[`set_timeout`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.set_timeout
[`set_interval`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.set_interval
[`TimerKey`]: https://stokes.io/kokage/api/kokage/struct.TimerKey.html
[`clear_timer`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.clear_timer
[`send_after_to`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.send_after_to
[`interval_to`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.interval_to
[`RawContext::send_after`]: https://stokes.io/kokage/api/kokage/raw/struct.RawContext.html#method.send_after
[`RawContext::interval`]: https://stokes.io/kokage/api/kokage/raw/struct.RawContext.html#method.interval
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
