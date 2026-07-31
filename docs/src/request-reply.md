# Request and Reply

`send` is fire-and-forget. Often you want an answer back: how much will this
order cost? Kokage builds request–reply on two pieces — a [`Reply`] value
carried *inside* your message type, and [`ActorRef::call`] on the caller's
side.

## Carrying a reply channel in the message

The front desk of our shop takes orders and gives quotes. Its message type
says so directly:

```rust
use kokage::prelude::*;

enum DeskMsg {
    Order(String),
    Quote { pages: u32, reply: Reply<u64> },
}

struct FrontDesk {
    orders_taken: u64,
}

impl Actor for FrontDesk {
    type Msg = DeskMsg;

    async fn handle(&mut self, msg: DeskMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match msg {
            DeskMsg::Order(job) => {
                self.orders_taken += 1;
                println!("accepted: {job}");
            }
            DeskMsg::Quote { pages, reply } => {
                reply.send(u64::from(pages) * 3);
            }
        }
        Ok(())
    }
}
```

[`Reply<T>`] is a one-shot response channel. `reply.send(value)` consumes it;
if the caller has already given up (timed out), the value is silently
discarded. A `Reply` is an ordinary value — you may stash it in the actor's
state and answer later, or forward it inside another message to let a
different actor answer.

## Calling

The caller does not construct a `Reply` itself. It hands
[`ActorRef::call`] a *message constructor* — a closure that receives the
freshly minted `Reply` and returns the message to send:

```rust
# use std::time::Duration;
# use kokage::prelude::*;
# enum DeskMsg { Order(String), Quote { pages: u32, reply: Reply<u64> } }
# struct FrontDesk { orders_taken: u64 }
# impl Actor for FrontDesk {
#     type Msg = DeskMsg;
#     async fn handle(&mut self, msg: DeskMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
#         match msg {
#             DeskMsg::Order(job) => { self.orders_taken += 1; println!("accepted: {job}"); }
#             DeskMsg::Quote { pages, reply } => reply.send(u64::from(pages) * 3),
#         }
#         Ok(())
#     }
# }
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    let desk = tree.add_actor("front-desk", || FrontDesk { orders_taken: 0 });
    let runtime = tree.spawn()?;

    let price = desk
        .call(Duration::from_secs(1), |reply| DeskMsg::Quote { pages: 250, reply })
        .await?;
    println!("quote: {price}");

    desk.send(DeskMsg::Order("250-page manual".to_owned())).await?;

    runtime.shutdown().await?;
    Ok(())
}
```

When the reply slot is a tuple variant holding only the `Reply`, the variant
name itself is already such a constructor, so this reads even tighter:

```rust
# use std::time::Duration;
# use kokage::prelude::*;
enum CounterMsg {
    Total(Reply<u64>),
}
# struct Counter { total: u64 }
# impl Actor for Counter {
#     type Msg = CounterMsg;
#     async fn handle(&mut self, CounterMsg::Total(reply): CounterMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
#         reply.send(self.total);
#         Ok(())
#     }
# }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let mut tree = Tree::new();
# let counter = tree.add_actor("counter", || Counter { total: 0 });
# let runtime = tree.spawn()?;
let total = counter.call(Duration::from_secs(1), CounterMsg::Total).await?;
# assert_eq!(total, 0);
# runtime.shutdown().await?;
# Ok(())
# }
```

## The timeout and what can go wrong

The `Duration` you pass to `call` bounds the *entire* round trip: waiting for
mailbox capacity, the actor picking the message up, and the reply arriving.
[`CallError`] tells you which part failed:

- `CallError::Terminated { .. }` — the request never got in because the actor
  was permanently gone.
- `CallError::Timeout { .. }` — the deadline passed. Note that once the
  request has been *accepted* into the mailbox, timing out cannot retract it:
  the actor may still process the request; only the answer is abandoned.
- `CallError::ReplyDropped { .. }` — the actor (or a forwarder) dropped the
  `Reply` without answering. This is how you notice a handler that forgot a
  code path — or a crashed one: if the actor fails while your request sits in
  its mailbox, the mailbox dies with that run and the reply comes back as
  `ReplyDropped` rather than hanging.

Two practical rules:

- **Always give `call` a real timeout.** It is your protection against a
  stuck collaborator.
- **Don't `call` yourself.** An actor that calls its own ref inside `handle`
  deadlocks until the timeout: the reply can only be produced by the very
  handler that is blocked waiting for it. Between actors, prefer sending a
  message that carries a `Reply` onward instead of blocking inside `handle`
  — an actor awaiting a `call` is not processing its own mailbox in the
  meantime.

Request–reply is also the escape hatch from kokage's at-most-once delivery:
if you need to *know* work happened, make the protocol say so with a reply.
More on that contract in the next chapter.

[`Reply`]: https://stokes.io/kokage/api/kokage/struct.Reply.html
[`Reply<T>`]: https://stokes.io/kokage/api/kokage/struct.Reply.html
[`ActorRef::call`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.call
[`CallError`]: https://stokes.io/kokage/api/kokage/enum.CallError.html
