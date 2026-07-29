# Bounded actor offloads

An actor that awaits slow work inside `handle` stops receiving messages until
that work completes. `offload` moves a bounded future off the handler loop and
maps its total result back into an ordinary typed message:

```rust,no_run
use std::time::Duration;
use kokage::host::RawContext;

enum Msg {
    Loaded(String),
}

# async fn load() -> String { String::new() }
# fn start(ctx: &mut RawContext<Msg>) {
ctx.offload(
    Duration::from_millis(250),
    load(),
    |result| Msg::Loaded(result.unwrap_or_else(|_| "value remained unknown".into())),
);
# }
```

The deadline is required and the continuation must handle both success and
`OffloadDeadline`; `unwrap_or` is the one-line spelling when a fallback is all
the protocol needs. The actor loop owns the completion directly. It does not consume
mailbox capacity or participate in conflation, and its ordering relative to
external mailbox messages is unspecified. The loop selects fairly between
the two sources so neither one has priority.

The continuation receives `Result<T, OffloadDeadline>` when the actor must
distinguish a deadline from a value returned by the future.

## Ownership and correlation

Every offload belongs to the actor context that started it. If that incarnation
fails or restarts, dropping the context aborts its offload set and discards any
unreaped completion. Nothing is sent through the restart-stable `ActorRef`, so
an old completion cannot reach fresh in-memory state.

The library owns that structural cross-incarnation boundary. Correlation among
concurrent offloads in one incarnation remains part of the message protocol:

```rust,no_run
# use std::time::Duration;
# use kokage::{OffloadDeadline, host::RawContext};
enum Msg {
    Fetched { request: u64, value: Result<String, OffloadDeadline> },
}
# async fn fetch() -> String { String::new() }
# fn start(ctx: &mut RawContext<Msg>, request: u64) {
ctx.offload(Duration::from_secs(1), fetch(), move |value| {
    Msg::Fetched { request, value }
});
# }
```

The request id is still necessary when multiple fetches can overlap. A
hand-rolled incarnation or turn tag used only to reject results after restart
is not. A panic in the future or continuation resumes on the actor task and is
therefore handled by its normal supervision policy.

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
use kokage::prelude::*;

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
        ctx: &mut Context<'_, Self>,
    ) -> ActorResult {
        match message {
            RouterMsg::Submit { venue, order, reply } => {
                // Validate and record intent on the handle loop...
                self.in_flight.insert(order, venue);
                let gateway = self.venues[venue].clone();
                // ...then move the slow call off it.
                ctx.offload(
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
                    move |result| RouterMsg::Resolved {
                        order,
                        accepted: result.unwrap_or(false),
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
        Ok(())
    }
}
```

Reply ownership is what keeps this observationally equivalent to the inline
version: `Reply` moves into the continuation message, so the actor applies its
state update before answering. Context ownership and the order id play the
roles described above — an unreaped completion is dropped with a failed
incarnation, while the domain request id still says which of several concurrent
offloads completed.

When per-callee state outgrows what a resolution message can carry, promote
the callee to a dedicated child actor and let supervision manage its lifecycle
instead. The `trading_engine` example's order router demonstrates the full
pattern, including a phase that proves an order for a healthy venue completes
while another venue's call is still waiting out its timeout.

## Abort is not undo

`TaskHandle::abort`, timeout, actor failure, and discard shutdown all abandon
the local future. They cannot retract a request another actor or external
service already accepted. Its outcome is unknown, not "not executed."

Offload futures should therefore initiate requests rather than mutate untracked
local state directly. Put effects behind actors and protect retryable commands
with idempotency keys and reconciliation. Domain cancellation remains
explicit: capture a `CancellationToken` in the future when the remote protocol
supports it.

## Shutdown

Offloads follow the actor declaration's `Shutdown`:

- `Shutdown::discard_after_current(grace)` closes external intake after an
  in-flight message, discards the queued remainder, and aborts outstanding
  offloads when shutdown reaches the actor loop.
- `Shutdown::drain_for(grace)` closes external intake, then processes queued
  messages and offload completions until both are exhausted. A drained message
  may start another offload, which joins the same bounded drain.

Draining must interleave messages and completions. Waiting for all offloads
first would postpone already accepted external work unnecessarily. Completions
cannot deadlock on mailbox capacity because they remain in the loop-owned task
set until reaped. Each offload future is still bounded by its own required
deadline; the one `Shutdown` grace declared for a standalone or supervised
actor remains the outer backstop for slow handlers.

`observe::ActorStats::outstanding_offloads` exposes the current number of owned offloads.
It falls when the actor loop reaps a completion or observes an abort. The method
lives on `host::RawContext`: `recv` and `try_recv` merge offload
completions with mailbox messages for a `host::RawActor`, but a hand-written raw loop
must still define its own shutdown and drain protocol.
