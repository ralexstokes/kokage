# Bounded actor offloads

An actor that awaits slow work inside `handle` stops receiving messages until
that work completes. `ActorContext::offload_or` moves a bounded future off the
handler loop, substitutes an explicit fallback value at the deadline, and maps
the value back into an ordinary typed message:

```rust,no_run
use std::time::Duration;
use tokio_otp::ActorContext;

enum Msg {
    Loaded(String),
}

# async fn load() -> String { String::new() }
# fn start(ctx: &ActorContext<Msg>) {
ctx.offload_or(
    Duration::from_millis(250),
    load(),
    "value remained unknown".into(),
    Msg::Loaded,
);
# }
```

The deadline and fallback are required: timeout cannot be forgotten, but the
common path does not need to expose a separate deadline type in the message
protocol. The completion uses the actor's ordinary mailbox policy. A full
FIFO mailbox backpressures it; conflating mailboxes may replace it like any
other message.

Use the lower-level `ActorContext::offload` when the actor must distinguish a
deadline from a value returned by the future. Its continuation receives the
total `Result<T, OffloadDeadline>` outcome.

## Incarnations and correlation

Every postback is stamped to the incarnation that started it. If that
incarnation fails or restarts, the future is aborted and a racing completion
is silently dropped instead of following the restart-stable `ActorRef` into
fresh in-memory state.

The library owns only this cross-incarnation staleness rule. Correlation among
concurrent offloads in one incarnation remains part of the message protocol:

```rust,no_run
# use std::time::Duration;
# use tokio_otp::{ActorContext, OffloadDeadline};
enum Msg {
    Fetched { request: u64, value: Result<String, OffloadDeadline> },
}
# async fn fetch() -> String { String::new() }
# fn start(ctx: &ActorContext<Msg>, request: u64) {
ctx.offload(Duration::from_secs(1), fetch(), move |value| {
    Msg::Fetched { request, value }
});
# }
```

The request id is still necessary when multiple fetches can overlap. A hand
rolled incarnation or turn tag used only to reject results after restart is
not.

## Pipelining calls from a handler

The most common slow await inside a handler is a `call` to another actor,
which blocks the caller's own mailbox for the full round-trip — see
[head-of-line blocking](request-reply.md#head-of-line-blocking-calls-from-inside-a-handler).
A routing actor avoids that by validating and recording intent on the handle
loop, then starting an offload for the call itself. The continuation maps the
outcome back to an ordinary message, so the state update and the original
caller's reply still happen on the serial loop:

```rust,no_run
use std::{collections::HashMap, time::Duration};
use tokio_otp::prelude::*;

enum VenueMsg {
    Place { order: u64, reply: Reply<bool> },
}

enum RouterMsg {
    Submit {
        venue: &'static str,
        order: u64,
        reply: Reply<bool>,
    },
    // Internal: the pipelined call's outcome.
    Resolved {
        order: u64,
        accepted: bool,
        reply: Reply<bool>,
    },
}

struct Router {
    venues: HashMap<&'static str, ActorRef<VenueMsg>>,
    in_flight: HashMap<u64, &'static str>,
}

impl Actor for Router {
    type Msg = RouterMsg;

    async fn handle(
        &mut self,
        message: RouterMsg,
        ctx: &mut ActorContext<RouterMsg>,
    ) -> ActorResult {
        match message {
            RouterMsg::Submit { venue, order, reply } => {
                // Validate and record intent on the handle loop...
                self.in_flight.insert(order, venue);
                let gateway = self.venues[venue].clone();
                // ...then move the slow call off it.
                ctx.offload_or(
                    Duration::from_millis(250),
                    async move {
                        matches!(
                            gateway
                                .call(Duration::from_millis(250), |reply| {
                                    VenueMsg::Place { order, reply }
                                })
                                .await,
                            Ok(true)
                        )
                    },
                    false,
                    move |accepted| RouterMsg::Resolved {
                        order,
                        accepted,
                        reply,
                    },
                );
            }
            RouterMsg::Resolved { order, accepted, reply } => {
                // Back on the handle loop: apply the outcome to actor state.
                self.in_flight.remove(&order);
                if !accepted {
                    // schedule reconciliation, raise an alert, ...
                }
                reply.send(accepted);
            }
        }
        Ok(tokio_otp::prelude::Continue)
    }
}
```

Reply ownership is what keeps this observationally equivalent to the inline
version: `Reply` moves into the continuation message, so the actor applies its
state update before answering, and a caller follow-up is ordered after that
update in the FIFO mailbox. Incarnation ownership and the order id play the
roles described above — a racing postback is dropped after a restart, while
the domain request id still says which of several concurrent offloads completed.

When per-callee state outgrows what a resolution message can carry, promote
the callee to a dedicated child actor and let supervision manage its lifecycle
instead. The `trading_engine` example's order router demonstrates the full
pattern, including a phase that proves an order for a healthy venue completes
while another venue's call is still waiting out its timeout.

## Abort is not undo

`OffloadHandle::abort`, timeout, actor failure, and discard shutdown all abandon
the local future. They cannot retract a request another actor or external
service already accepted. Its outcome is unknown, not "not executed."

Offload futures should therefore initiate requests rather than mutate untracked
local state directly. Put effects behind actors and protect retryable commands
with idempotency keys and reconciliation. Domain cancellation remains
explicit: capture a `CancellationToken` in the future when the remote protocol
supports it.

## Shutdown

Offloads follow handler actors' `DrainPolicy`:

- `Discard` closes external intake and aborts outstanding offloads as soon as
  shutdown reaches the actor loop.
- `Drain` closes external intake, then processes queued messages and offload
  completions until both are exhausted. A drained message may start another
  offload, which joins the same bounded drain.

Draining must interleave messages and completions. Waiting for all offloads
first would deadlock when a full FIFO mailbox backpressures a completion that
needs the actor to receive one queued message before capacity becomes
available. Each offload future is still bounded by its own required deadline;
the standalone host bound or supervised child grace remains the outer backstop
for slow handlers.

`ActorStats::outstanding_offloads` exposes the current number of owned offloads.
The method lives on the shared `ActorContext` type, but automatic shutdown and
drain integration belongs to the framework-owned `Actor` loop. A `RawActor`
that uses `offload` must define its own receive and shutdown protocol.
