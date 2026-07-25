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

    async fn on_start(&mut self, ctx: &ActorContext<Message>) -> ActorResult {
        self.reconnect = Some(ctx.send_after(Message::Reconnect, Duration::from_secs(5)));
        self.reconcile = Some(ctx.interval(Message::Reconcile, Duration::from_secs(30)));
        Ok(Continue)
    }

    async fn handle(&mut self, message: Message, _ctx: &ActorContext<Message>) -> ActorResult {
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

Rust enums already make actor state machines explicit. `state_timeout` adds
the one piece that otherwise requires bookkeeping: a single timeout slot that
cancels and replaces the previous timeout whenever the actor enters another
timed state. `clear_state_timeout` cancels the slot when entering an untimed
state.

```rust,ignore
use std::time::Duration;

use tokio_otp::prelude::*;

#[derive(Clone)]
enum Message {
    Filled,
    FillTimedOut,
    Cancelled,
}

#[derive(Clone, Default)]
enum Order {
    #[default]
    PendingFill,
    Cancelling,
    Complete,
}

impl Actor for Order {
    type Msg = Message;

    async fn on_start(&mut self, ctx: &ActorContext<Message>) -> ActorResult {
        ctx.state_timeout(Message::FillTimedOut, Duration::from_millis(500));
        Ok(Continue)
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext<Message>) -> ActorResult {
        match (&*self, message) {
            (Order::PendingFill, Message::Filled) => {
                ctx.clear_state_timeout();
                *self = Order::Complete;
            }
            (Order::PendingFill, Message::FillTimedOut) => {
                *self = Order::Cancelling;
                ctx.state_timeout(Message::Cancelled, Duration::from_secs(2));
            }
            (Order::Cancelling, Message::Cancelled) => {
                ctx.clear_state_timeout();
                *self = Order::Complete;
            }
            _ => {}
        }
        Ok(Continue)
    }
}
```

Calling `state_timeout` takes ownership of the message and returns a
`CancellationHandle`, like `send_after`, but the actor does not need to retain
it. Entering another timed state and calling `state_timeout` automatically
cancels the previous timeout. Call `clear_state_timeout` when entering a state
without a timeout.

Timeout messages that were already accepted by the mailbox cannot be
retracted. Each timeout message must therefore identify the state it was set
for, either with a distinct variant as above or with a state or generation
tag, and the handler must reject tags that do not match its current state.
Replacing or clearing a state timeout cancels its token even if it already
fired, so a retained handle will then report `is_cancelled() == true`. State
timeouts are also cleared with all other timers when the actor stops or
restarts.
