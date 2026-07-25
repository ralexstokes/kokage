# Bounded Request/Reply

`ActorRef::call` builds request/reply on the ordinary actor mailbox: it creates
a one-shot `Reply<T>`, puts that reply handle in your message, sends the
message, and waits for the actor to answer. Every `call` takes an explicit
timeout so the whole operation is bounded:

```rust,no_run
use std::time::Duration;
use tokio_otp::{ActorRef, Reply};

enum AccountMsg {
    Balance(Reply<u64>),
}

async fn balance(
    account: &ActorRef<AccountMsg>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let balance = account
        .call(Duration::from_millis(250), AccountMsg::Balance)
        .await?;
    Ok(balance)
}
```

The timeout covers both phases of a call:

1. **Delivery:** `call` waits for the same conditions as `send`. Before the
   actor starts, or during an expected restart, it waits for a mailbox to bind.
   With a full FIFO mailbox, it waits for capacity. If the timeout expires in
   this phase, the call returns `CallError::Timeout` and drops the request
   before acceptance.
2. **Reply:** after the mailbox accepts the request, `call` waits on its
   one-shot reply channel. If the timeout expires now, the call returns
   `CallError::Timeout`, but only the caller's wait is cancelled. The accepted
   request remains in the mailbox, the actor may still process it, and
   `Reply::send` silently discards a late result.

This boundary means that a timed-out call can have an **unknown outcome**. The
caller cannot generally tell whether the request never reached the actor, is
still queued, is currently running, or completed after the timeout.

## Side effects and retries

For read-only operations, ignoring a late reply is often enough. For commands
that write to a database, charge a card, publish an event, or otherwise affect
an external system, do not treat a timeout as proof that nothing happened.
Give each logical operation an idempotency key that the actor and external
system persist, or provide a reconciliation query that lets the caller learn
the final status. Retry with the same key rather than creating a second
logical operation.

Neither `CallError::Timeout` nor cancellation of the `call` future is a
cancellation signal for actor work. If a protocol needs cooperative
cancellation, model it explicitly in the message type and define what happens
when cancellation races with completion.

## Backpressure and restarts

The timeout deliberately includes mailbox backpressure and restart backoff.
Choose one that covers the queueing delay your service is willing to tolerate:

- Use `try_send` for fire-and-forget messages when failing fast on a full
  mailbox beats waiting. There is no fail-fast variant of `call`.
- Use `call(timeout, ...)` when the caller can wait for capacity or a short
  restart window, but needs a firm end-to-end bound.
- Do not use `call` with a conflating mailbox. A newer value can replace the
  request, causing `CallError::ReplyDropped`.

A call waiting during an expected restart can succeed after the new actor
incarnation binds. A request accepted by the old incarnation before it stops
is still subject to the at-most-once delivery contract: it may be lost with
that incarnation, in which case the reply channel closes with
`CallError::ReplyDropped`.

## Head-of-line blocking: calls from inside a handler

An actor processes one message at a time. When a handler awaits a `call` to
another actor, the calling actor's mailbox stops for the full round-trip:
every queued message — a cancel bound for a healthy peer, an urgent status
query — waits behind the outstanding request for up to the call's timeout.
This is the natural way to write a request-routing actor, and it
is the actor-model equivalent of blocking inside an Erlang `gen_server`
callback: one slow callee becomes head-of-line blocking for everything
routed through the intermediary.

For fan-out and routing actors, pipeline the request instead of awaiting it.
The handler validates and records intent, then starts a bounded
`ActorContext::step`. Its continuation maps the outcome back to an ordinary
message, so state updates and the original caller's reply still happen on the
actor's serial handle loop:

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
                ctx.step_or(
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

Three properties make the pattern work:

- **Reply ownership.** `Reply` moves into the continuation message, so the
  actor applies its state update before answering. A caller follow-up is
  therefore ordered after that update in the actor's FIFO mailbox.
- **Incarnation ownership.** A step is aborted when its actor incarnation
  dies, and a racing postback is dropped instead of reaching a fresh
  incarnation through the restart-stable ref.
- **Same-incarnation correlation.** The order id still identifies which of
  several concurrent steps completed. `step` removes the need for a separate
  hand-written restart generation; it does not replace domain request ids.

An outcome that must survive the actor still needs durable state or a
reconciliation path: abort and timeout abandon the wait, not a remote request
already accepted. When per-callee state outgrows what a resolution message can
carry, promote the callee to a dedicated child actor and let supervision
manage its lifecycle instead.

Not every handler needs this treatment. A serial batch operation that
mutates actor state between calls — a reconciliation sweep run while
intake is quiet, for example — can reasonably stay inline, provided
blocking the mailbox for its duration is an explicit, accepted trade-off.

The `trading_engine` example's order router demonstrates the full pattern,
including a phase that proves an order for a healthy venue completes while
another venue's call is still waiting out its timeout.
