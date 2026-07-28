# Actor Timers

An actor schedules work for two different destinations, and the distinction is
important:

- self-scheduled work belongs to the framework-owned actor loop;
- work scheduled for another actor crosses that boundary through its
  `ActorRef`.

The mailbox therefore remains an inter-actor channel. Self timers do not
consume mailbox capacity, wait behind FIFO backpressure, or conflate an unread
external message.

## Self-scheduling

Handler-style actors have one loop-owned timer table. `send_after` and
`interval` create anonymous entries and return a `CancellationHandle`:

```rust,ignore
use std::time::Duration;

use tokio_otp::prelude::*;

#[derive(Clone)]
enum Message {
    Reconnect,
    Reconcile,
}

#[derive(Default)]
struct Worker {
    reconnect: Option<CancellationHandle>,
    reconcile: Option<CancellationHandle>,
}

impl Actor for Worker {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        self.reconnect = Some(ctx.send_after(Message::Reconnect, Duration::from_secs(5)));
        self.reconcile = Some(ctx.interval(Message::Reconcile, Duration::from_secs(30)));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Message,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            Message::Reconnect => { /* reconnect once */ }
            Message::Reconcile => { /* reconcile periodically */ }
        }
        Ok(Continue)
    }
}
```

`send_after` owns its message and delivers it once. `interval` keeps the
original and clones it at each delivery, so only that method requires `Clone`.
Dropping a handle does not cancel its entry; calling `cancel` on any clone
cancels it exactly until the loop takes the message for delivery. A zero-period
interval is an already-cancelled no-op.

Each interval delivery arms the next period. If a handler is still busy when a
deadline passes, one overdue tick is delivered when the loop is free and the
following deadline starts from there. Missed ticks never pile up.

Timer deliveries count as received messages in `ActorStats`, but not as
accepted mailbox messages. The whole table drops with the incarnation, so a
restart cannot leak a stale timer into fresh state and no helper task is
spawned per self timer.

## Replaceable timeouts

Replaceable timeouts are keyed with `TimerKey`. `set_timeout` replaces the
entry at that key, `clear_timeout` retracts it, and `timeout_armed` reports
whether it exists. Other keys remain independent:

```rust,ignore
use std::time::Duration;

use tokio_otp::prelude::*;

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

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Self>) -> ActorResult {
        ctx.set_timeout(FILL, Message::FillTimedOut, Duration::from_millis(500));
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: Message,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
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
        Ok(Continue)
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

`tokio_otp::timers::send_after_to` and `interval_to` are small utilities built
on public API. Pass the scheduling incarnation's observe-only `Lifetime` and
the target's `ActorRef`:

```rust,ignore
use tokio_otp::timers;

let lifetime = ctx.lifetime();
timers::send_after_to(
    &lifetime,
    &ledger,
    LedgerMsg::Expire { key },
    Duration::from_secs(30),
);
timers::interval_to(
    &lifetime,
    &monitor,
    MonitorMsg::Heartbeat,
    Duration::from_secs(5),
);
```

These timers really do cross an actor boundary, so delivery uses the target's
ordinary `ActorRef::send` path and mailbox policy. A full FIFO mailbox delays
the timer task until capacity opens; a conflating mailbox may replace an unread
earlier delivery. Successful sends increment accepted-message counters. None
of those behaviors applies to loop-owned self timers, which bypass capacity
and conflation and increment only received-message counters. The cross-actor
timer tasks stop when cancelled, when the scheduling lifetime ends, or when
the target permanently terminates. A target that merely restarts receives
later deliveries through its restart-stable ref. Messages should carry a key
or generation when the target must reject stale cross-actor work.

`Lifetime` cannot stop its actor; it only exposes `is_ended` and the awaitable
`ended`. `CancellationHandle` owns the separate authority to stop the timer
operation and exposes the awaitable `cancelled`.

## `RawActor` deadlines

A `RawActor` owns its own receive loop, so it also owns its deadline branches.
Use Tokio's `sleep_until` directly beside `ctx.recv()`:

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
