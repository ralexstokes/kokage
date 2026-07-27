# Actor Timers

Actors often need to schedule work for themselves: retry a connection after a
delay, expire an order, send a heartbeat, or reconcile state periodically.
`ActorContext` turns those events into ordinary typed mailbox messages, so the
same `Actor::handle` method handles both external and timed work.

```rust,ignore
use std::time::Duration;

use tokio_otp::prelude::*;

#[derive(Clone)]
enum Message {
    Reconnect,
    Reconcile,
}

#[derive(Clone, Default)]
struct Worker {
    reconnect: Option<CancellationHandle>,
    reconcile: Option<CancellationHandle>,
}

impl Actor for Worker {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Message>) -> ActorResult {
        self.reconnect = Some(ctx.send_after(Message::Reconnect, Duration::from_secs(5)));
        self.reconcile = Some(ctx.interval(Message::Reconcile, Duration::from_secs(30)));
        Ok(Continue)
    }

    async fn handle(&mut self, message: Message, _ctx: &mut HandleContext<'_, Message>) -> ActorResult {
        match message {
            Message::Reconnect => { /* reconnect once */ }
            Message::Reconcile => { /* reconcile periodically */ }
        }
        Ok(Continue)
    }
}
```

`send_after` owns its message and sends it once after the delay. `interval`
clones its message on each period, so its message type must implement `Clone`.
Both return a cloneable `CancellationHandle`; calling `cancel` on any clone
cancels the same timer. Dropping the handle does not cancel it.

Timer messages use the normal bounded mailbox. When the mailbox is full,
FIFO delivery waits for capacity just like `ActorRef::send`; conflating
mailboxes replace stale unread state instead. Intervals do not build
an unbounded backlog while waiting: missed ticks are skipped. A one-shot timer
that has fired is complete, not cancelled, so its handle still reports
`is_cancelled() == false`.

Timers belong to one actor incarnation. The runtime cancels all of them when
that incarnation stops, restarts, or observes shutdown. A timer task waiting
for mailbox capacity is cancelled too, so it cannot follow the actor's stable
ref and leak a stale message into the next incarnation.

## Cross-actor timers

`send_after_to` and `interval_to` are the cross-actor forms of `send_after`
and `interval`: they take an `ActorRef<T>` and deliver the message to that
actor's mailbox instead of the scheduler's own.

```rust,ignore
ctx.send_after_to(&ledger, LedgerMsg::Expire { key }, Duration::from_secs(30));
ctx.interval_to(&monitor, MonitorMsg::Heartbeat, Duration::from_secs(5));
```

Lifecycle binding stays with the *scheduling* actor: the timer is cancelled
when the scheduler's incarnation stops or restarts, exactly like a self timer.
It is not bound to the target's lifecycle. A target that restarts before the
timer fires receives the message in its next incarnation, so cross-actor timer
messages should carry a key or generation the target's handler can use to
reject deliveries it no longer expects. An interval stops only if the target
terminates permanently.

## State timeouts

Rust enums already make actor state machines explicit. What they do not give
you is the one piece a `gen_statem`-style timeout needs: a timeout belonging to
a state the actor has already left must not be acted on, even when it already
reached the mailbox.

`ActorScope::send_after_retractable` is the primitive. It behaves like
`send_after`, except that cancelling its handle also discards the message
*after* the mailbox accepted it, as long as the actor has not yet received it.
`StateTimeoutSlot` is the one-at-a-time bookkeeping on top: a slot lives in
actor state, `set` cancels and replaces whatever it held, and `clear` empties
it.

```rust,ignore
use std::time::Duration;

use tokio_otp::prelude::*;

#[derive(Clone)]
enum Message {
    Filled,
    FillTimedOut,
    Cancelled,
}

#[derive(Default)]
enum Phase {
    #[default]
    PendingFill,
    Cancelling,
    Complete,
}

#[derive(Default)]
struct Order {
    phase: Phase,
    deadline: StateTimeoutSlot,
}

impl Actor for Order {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &mut StartContext<'_, Message>) -> ActorResult {
        self.deadline
            .set(ctx.send_after_retractable(Message::FillTimedOut, Duration::from_millis(500)));
        Ok(Continue)
    }

    async fn handle(&mut self, message: Message, ctx: &mut HandleContext<'_, Message>) -> ActorResult {
        match (&self.phase, message) {
            (Phase::PendingFill, Message::Filled) => {
                self.deadline.clear();
                self.phase = Phase::Complete;
            }
            (Phase::PendingFill, Message::FillTimedOut) => {
                self.phase = Phase::Cancelling;
                self.deadline
                    .set(ctx.send_after_retractable(Message::Cancelled, Duration::from_secs(2)));
            }
            (Phase::Cancelling, Message::Cancelled) => {
                self.deadline.clear();
                self.phase = Phase::Complete;
            }
            _ => {}
        }
        Ok(Continue)
    }
}
```

`set` returns the handle it armed, so a caller that wants to cancel one
timeout independently can keep it; most do not. A retained handle reports
`is_cancelled() == true` once the slot has been replaced or cleared.

The suppression is a mailbox-level filter: the framework stamps the delivery
with the timer's cancellation token and discards it at receive time if that
token has been cancelled. That is why this cannot be built outside the
framework — recognizing a stale timeout in user code would mean tagging it with
a generation, and an actor's message type belongs to its senders, not to a
wrapper around the actor. It is also why the filter works for `RawActor` loops
reading `ctx.recv()`, not just for the provided `Actor` loop.

Slots are ordinary actor state, so an actor that does not model states carries
nothing for this. Retractable sends are cleared with all other timers when the
actor stops or restarts.
