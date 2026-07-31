# Mailboxes and Backpressure

Every running actor owns a **bounded** mailbox. Bounded is a feature: when the
press falls behind, the pressure is felt by the senders — where you can do
something about it — instead of silently growing a queue until memory runs
out. This chapter covers the three ways to send, sizing and storage policy,
and the delivery contract they all share.

## Three ways to send

```rust
use std::sync::Arc;

use kokage::{SendErrorKind, prelude::*};
use tokio::sync::Semaphore;

struct SlowPress {
    gate: Arc<Semaphore>,
}

impl Actor for SlowPress {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        // The press works only when main() releases the gate.
        self.gate.acquire().await?.forget();
        println!("printed: {job}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gate = Arc::new(Semaphore::new(0));
    let spec = ActorSpec::new("press", {
        let gate = gate.clone();
        move || SlowPress { gate: gate.clone() }
    })
    .mailbox(Mailbox::queue(1));

    let mut tree = Tree::new();
    let press = tree.add_actor_spec(spec);
    let runtime = tree.spawn()?;

    // Accepted immediately; the press picks it up and blocks on the gate.
    press.send("job A".to_owned()).await?;
    // Waits until the press has taken job A, then fills the single slot.
    press.send("job B".to_owned()).await?;

    // The mailbox is now full. `send` would wait; `try_send` refuses instead.
    let rejected = press.try_send("job C".to_owned()).unwrap_err();
    assert_eq!(rejected.kind, SendErrorKind::Full);
    println!("rejected while busy: {}", rejected.message);

    // Release the press twice and drain on shutdown.
    gate.add_permits(2);
    runtime.shutdown().await?;
    Ok(())
}
```

The three flavors, and when to reach for each:

- **[`send`]** — awaits until the message is accepted. It applies
  backpressure to you, and it *rides through restart windows*: if the actor
  is mid-restart, `send` waits for the replacement and delivers to it. It
  fails only when the actor is permanently gone
  (`SendErrorKind::Terminated`). This is the default choice inside a
  supervised system.
- **[`try_send`]** — never waits. Rejections come back immediately as
  `NotRunning` (not started or between runs), `Full`, or `Terminated`. Use
  it at ingest edges where dropping is better than stalling.
- **[`send_timeout`]** — like `send`, but gives up after a bound, adding
  `SendErrorKind::TimedOut`. Prefer it over wrapping `send` in
  `tokio::time::timeout`: cancelling a `send` future *drops the message*,
  while `send_timeout` hands the unaccepted message back to you.

Every rejection is a [`SendError`] carrying `actor_id`, the `kind`, and —
importantly — `message`, the value you tried to send, so nothing is lost
silently. Call `.into_message()` to recover it, or `.into_boxed()` to erase
the payload while retaining the actor id and rejection kind in an application
error.

## Sizing the mailbox

The default capacity is 64 messages. Configure it per actor on the spec, or
for a whole tree:

```rust
# use kokage::prelude::*;
# struct Press;
# impl Actor for Press {
#     type Msg = String;
#     async fn handle(&mut self, _job: String, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
let spec = ActorSpec::new("press", || Press).mailbox(Mailbox::queue(8));
let tree = Tree::new().mailbox_capacity(128); // default for actors in this tree
# let _ = (spec, tree);
```

Small mailboxes surface overload early; large ones smooth bursts. Since
`send` already blocks producers when the consumer is behind, capacity is
about *how much burst you want to absorb*, not about correctness.

## Storage policy: queue or latest-wins

One [`Mailbox`] value chooses both what the mailbox keeps and how much it can
hold:

- `Mailbox::queue(capacity)` — the bounded FIFO described above.
- `Mailbox::latest()` — a single latest-wins slot. Sends never wait;
  a newer message simply replaces an unread older one. Perfect for
  status-display style consumers where only the freshest value matters.
- `Mailbox::latest_by_key(capacity, f)` — latest-wins *per key*, keeping one
  unread message for each key up to `capacity` keys (oldest key
  evicted).

```rust
# use kokage::prelude::*;
#[derive(Clone)]
struct Telemetry {
    press_id: u32,
    temperature_c: f64,
}
# struct Dashboard;
# impl Actor for Dashboard {
#     type Msg = Telemetry;
#     async fn handle(&mut self, _t: Telemetry, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }

// Keep only the freshest reading per press.
let spec = ActorSpec::new("dashboard", || Dashboard)
    .mailbox(Mailbox::latest_by_key(16, |t: &Telemetry| t.press_id));
# let _ = spec;
```

Latest-wins mailboxes make no FIFO guarantee and apply no backpressure — and
they must **not** be combined with `call`: a conflated-away request takes its
`Reply` with it, so the caller sees `ReplyDropped`.

## The delivery contract: at-most-once

All of this rests on one honest rule: delivery is **at-most-once**. A mailbox
belongs to one *run* of an actor. If that run fails, messages it had already
accepted die with it — deliberately. Preserving the queue across a restart
would redeliver the very message that crashed the actor, turning one failure
into a restart loop. Likewise, shutdown bounds how long draining may take, so
a message can be dropped at the end of the world.

When you need stronger guarantees, build them in the protocol: use
[`call`](request-reply.md) to get an acknowledgement, and re-send from the
caller if it doesn't come. The library gives you exactly-once *nowhere* and
truthful failure signals *everywhere* — which is the raw material
acknowledgements are made of.

[`send`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.send
[`try_send`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.try_send
[`send_timeout`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.send_timeout
[`SendError`]: https://stokes.io/kokage/api/kokage/struct.SendError.html
[`Mailbox`]: https://stokes.io/kokage/api/kokage/struct.Mailbox.html
