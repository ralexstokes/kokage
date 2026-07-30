# Actor Timers

An actor schedules one-shot self messages through two deliberately different
mechanisms. Choose by the delivery and cancellation semantics you need:

| API | Delivery | Ownership and cancellation |
| --- | --- | --- |
| `send_after(message, delay)` | Ordinary self-mailbox send: capacity, FIFO backpressure, conflation, and accepted-message statistics apply. | An independent `Guard` cancels the operation; there is no key for exact replacement or retraction. |
| `set_timeout(key, message, delay)` | Actor-loop delivery: no mailbox transit, capacity, or conflation; received-message statistics increase, accepted-message statistics do not. | The loop owns the entry; the same key exactly replaces it, and `clear_timeout(key)` exactly retracts it until delivery. |

This distinction matters even though both calls schedule one self-message.
Use `send_after` for an independently guarded mailbox delivery. Use
`set_timeout` for protocol deadlines whose pending value must be exactly
replaced or retracted. Periodic mailbox delivery uses `interval`.

## Self-scheduling

Handler-style actors can schedule one-shot self messages through either an
independently guarded mailbox delivery or a loop-owned keyed timer table.
Periodic work uses the self-first `interval(message, period)` and returns a
`Guard`:

```rust,ignore
use std::time::Duration;

use kokage::{Guard, TimerKey, prelude::*};

#[derive(Clone)]
enum Message {
    Reconnect,
    Refresh,
    Reconcile,
}

#[derive(Default)]
struct Worker {
    reconcile: Option<Guard>,
}

const RECONNECT: TimerKey = TimerKey::new("reconnect");

impl Actor for Worker {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(RECONNECT, Message::Reconnect, Duration::from_secs(5));
        ctx.send_after(Message::Refresh, Duration::from_secs(10)).detach();
        self.reconcile =
            Some(ctx.interval(Message::Reconcile, Duration::from_secs(30)));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Message,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        match message {
            Message::Reconnect => { /* reconnect once */ }
            Message::Refresh => { /* independently scheduled mailbox work */ }
            Message::Reconcile => { /* reconcile periodically */ }
        }
        Ok(())
    }
}
```

`set_timeout` owns its message and delivers it once inside the actor loop;
reusing its key replaces the prior entry and `clear_timeout` cancels it. These
deliveries count as received messages but not accepted mailbox messages. The
keyed table drops with the incarnation, so a restart cannot leak stale local
timeouts into fresh state.

`interval` clones its message for each tick and awaits `ActorRef::send`.
Missed ticks are skipped, but a full target mailbox delays the next send. A
conflating target may replace an unread tick, and each successful send counts
as an accepted mailbox message. The returned `Guard` cancels the interval when
dropped; retain it for the desired lifetime or call `detach()` for explicit
fire-and-forget scheduling. Calling `cancel` also cancels the operation. A zero
period returns an already-finished guard. The interval also ends with the
scheduling incarnation or when the target permanently terminates; those
environmental endings set `is_finished()` without setting `is_cancelled()`;
await `finished()` to wait for termination without polling.

When the operation is independently owned rather than a replaceable protocol
deadline, use `send_after(message, delay)` and retain its `Guard`. That
deliberately takes the ordinary self-mailbox path: mailbox capacity and
conflation apply, and a successful delivery increments accepted-message
statistics. It cannot be exactly replaced or retracted by key. By contrast,
`set_timeout(key, message, delay)` is stored in the actor loop, bypasses the
mailbox, and can be exactly replaced or cleared until delivery.

## Replaceable timeouts

Replaceable timeouts are keyed with `TimerKey`. `set_timeout` replaces the
entry at that key and `clear_timeout` retracts it. Actor state should track
whether the protocol currently expects a timeout; the timer table does not
expose a second source of truth. Other keys remain independent:

```rust,ignore
use std::time::Duration;

use kokage::{TimerKey, prelude::*};

enum Message {
    Filled,
    FillTimedOut,
    Cancelled,
}

enum Phase {
    PendingFill,
    Cancelling,
    Complete,
}

struct Order {
    phase: Phase,
}

const FILL: TimerKey = TimerKey::new("fill");
const CANCEL: TimerKey = TimerKey::new("cancel");

impl Actor for Order {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(FILL, Message::FillTimedOut, Duration::from_millis(500));
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Message,
        ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        match (&self.phase, message) {
            (Phase::PendingFill, Message::Filled) => {
                ctx.clear_timeout(FILL);
                self.phase = Phase::Complete;
            }
            (Phase::PendingFill, Message::FillTimedOut) => {
                self.phase = Phase::Cancelling;
                ctx.set_timeout(CANCEL, Message::Cancelled, Duration::from_secs(2));
            }
            (Phase::Cancelling, Message::Cancelled) => {
                ctx.clear_timeout(CANCEL);
                self.phase = Phase::Complete;
            }
            _ => {}
        }
        Ok(())
    }
}
```

This is the Rust counterpart of Erlang `gen_statem`'s `state_timeout`, without
the prefix: retraction is explicit and is also useful to actors that do not
model named states.

For independent protocol deadlines, use one key per deadline:

```rust,ignore
const LEASE: TimerKey = TimerKey::new("lease");
const HEARTBEAT: TimerKey = TimerKey::new("heartbeat");

ctx.set_timeout(LEASE, Message::LeaseExpired, Duration::from_secs(30));
ctx.set_timeout(HEARTBEAT, Message::PeerSilent, Duration::from_secs(5));
ctx.clear_timeout(LEASE);
```

Keys are static strings because they name protocol vocabulary and remain easy
to inspect in a debugger. Setting one key replaces only that key. These named
entries follow `gen_statem`'s `{timeout, Name}` lineage.

### Ordering at fire time

An external message accepted before a deadline gets the chance to retract or
replace that elapsed timer. At fire time the loop snapshots mailbox depth,
handles that bounded queued prefix, then delivers the timer only if the same
entry survived. Actor-local continuations retain priority throughout. Messages
accepted after the snapshot wait for the next ordinary loop iteration.

This preserves the useful OTP race semantics: a `Filled` accepted just before
an order deadline can clear the timeout instead of allowing a stale
`FillTimedOut` side effect.

## Cross-actor timers

The cross-actor forms are `send_after_to` and `interval_to` on `Context` and
`host::RawContext`. Pass the target's `ActorRef`; the context binds the timer
to the scheduling incarnation internally:

```rust,ignore
ctx.send_after_to(
    &ledger,
    LedgerMsg::Expire { key },
    Duration::from_secs(30),
)
.detach();
ctx.interval_to(
    &monitor,
    MonitorMsg::Heartbeat,
    Duration::from_secs(5),
)
.detach();
```

These timers really do cross an actor boundary, so delivery uses the target's
ordinary `ActorRef::send` path and mailbox policy. A full FIFO mailbox delays
the timer task until capacity opens; a conflating mailbox may replace an unread
earlier delivery. Successful sends increment accepted-message counters. None
of those behaviors applies to keyed loop-owned timeouts, which bypass capacity
and conflation and increment only received-message counters. The cross-actor
timer tasks stop when cancelled, when the scheduling incarnation ends, or when
the target permanently terminates. A target that merely restarts receives
later deliveries through its restart-stable ref. Messages should carry a key
or generation when the target must reject stale cross-actor work.

`Guard` owns the authority to stop the timer operation. It exposes `cancel`,
`is_cancelled`, `is_finished`, and `finished`; the actor lifetime token itself
remains private to the context. Dropping a guard cancels its operation, while
`detach()` deliberately leaves it running.

## `host::RawActor` deadlines

A `host::RawActor` can use `RawContext::send_after` and
`RawContext::interval` for ordinary self-mailbox delivery, plus their `_to`
forms for other actors. It has no framework-owned keyed timer table because it
owns its receive loop. For exact loop-local replacement and retraction, use
Tokio's `sleep_until` directly beside `ctx.recv()`:

```rust,ignore
let deadline = tokio::time::sleep_until(Instant::now() + IDLE);
tokio::pin!(deadline);

loop {
    tokio::select! {
        message = ctx.recv() => {
            let Some(message) = message else { break };
            // Handle external input and reset `deadline` when appropriate.
        }
        () = &mut deadline => {
            // Handle the actor-local timeout.
            deadline.as_mut().reset(Instant::now() + IDLE);
        }
    }
}
```

That is the same ownership model made explicit: the deadline is local state in
the loop, never a message sent through the mailbox.
