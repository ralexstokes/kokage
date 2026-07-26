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

Not every handler needs to avoid it. A serial batch operation that mutates
actor state between calls — a reconciliation sweep run while intake is quiet,
for example — can reasonably stay inline, provided blocking the mailbox for
its duration is an explicit, accepted trade-off.

For fan-out and routing actors it rarely is. Pipeline the request off the
handler loop instead of awaiting it, so the mailbox keeps moving while the
call is outstanding: [Bounded actor offloads](actor-offloads.md) covers the
mechanism and works the routing case end to end.
