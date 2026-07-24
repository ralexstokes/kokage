# Actor Graphs

The actor layer of `tokio-otp` models a group of async actors with typed
mailboxes. Each actor declares one message type, and a topology mints
restart-stable `ActorRef<M>` handles that can be stored in other actors'
state.

There are no string-addressed sends and no byte envelope type. If the front
desk sends orders to the press, the front desk owns an `ActorRef<Order>`.
Actor names exist too, but only as *labels* for tracing, stats, and
supervisor child ids — addressing is always a typed ref.

Handler methods return `Ok(Continue)` to receive another message or `Ok(Stop)`
to finish cleanly. A clean stop follows the same drain policy and `on_stop`
hook as an external stop: `Discard` drops the queued mailbox, `Drain` handles
it, and queued `continue_with` continuations are dropped in either case. It is
a normal exit to watches and supervision. `RestartPolicy::Always` restarts it;
`OnFailure` and `Never` do not.

The usual static graph is a struct whose fields are the actors. Deriving
`Topology` gives that struct a `graph` method; its wiring closure receives a
refs struct with one typed `ActorRef` per field, so cycles and forward
references do not require string lookup. Refs you need outside the graph are
captured from the same closure:

```rust,no_run
use tokio_otp::prelude::Continue;
use std::time::Duration;
use tokio_otp::{ActorContext, ActorRef, ActorResult, Actor, GraphBuilder, Reply, Runtime, Topology};

struct Order(String);
struct Parcel(String);

enum ShippingMsg {
    Ship(Parcel),
    Total(Reply<usize>),
}

struct FrontDesk {
    press: ActorRef<Order>,
}

impl Actor for FrontDesk {
    type Msg = Order;

    async fn handle(&mut self, order: Order, _ctx: &ActorContext<Order>) -> ActorResult {
        self.press.send(order).await?;
        Ok(Continue)
    }
}

struct Press {
    shipping: ActorRef<ShippingMsg>,
}

impl Actor for Press {
    type Msg = Order;

    async fn handle(&mut self, Order(order): Order, _ctx: &ActorContext<Order>) -> ActorResult {
        self.shipping
            .send(ShippingMsg::Ship(Parcel(format!("printed[{order}]"))))
            .await?;
        Ok(Continue)
    }
}

#[derive(Default)]
struct Shipping {
    shipped: usize,
}

impl Actor for Shipping {
    type Msg = ShippingMsg;

    async fn handle(
        &mut self,
        message: ShippingMsg,
        _ctx: &ActorContext<ShippingMsg>,
    ) -> ActorResult {
        match message {
            ShippingMsg::Ship(Parcel(parcel)) => {
                self.shipped += 1;
                println!("shipping: {parcel}");
            }
            ShippingMsg::Total(reply) => reply.send(self.shipped),
        }
        Ok(Continue)
    }
}

#[derive(Topology)]
struct PrintShop {
    front_desk: FrontDesk,
    press: Press,
    shipping: Shipping,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = GraphBuilder::new();
    builder.name("print-shop");

    let mut entry_points = None;
    let graph = PrintShop::graph_with(builder, |refs| {
        entry_points = Some((refs.front_desk.clone(), refs.shipping.clone()));
        let press = refs.press.clone();
        let shipping = refs.shipping.clone();
        PrintShopFactories {
            front_desk: move || FrontDesk {
                press: press.clone(),
            },
            press: move || Press {
                shipping: shipping.clone(),
            },
            shipping: Shipping::default,
        }
    })?;
    let (orders, shipping) = entry_points.expect("wiring closure ran");

    let handle = Runtime::builder().graph(graph).build()?.spawn();

    orders.send(Order("business cards x100".into())).await?;
    orders.send(Order("flyers x500".into())).await?;

    let shipped = shipping
        .call(Duration::from_secs(1), ShippingMsg::Total)
        .await?;
    println!("shipped {shipped} jobs");

    handle.shutdown_and_wait().await?;
    Ok(())
}
```

For lower-level hosting, iterate `graph.actors()` and drive each
`RunnableActor::run_until` independently. `tokio-otp` performs that adaptation
for the common supervised runtime.

## Incarnation-local state

Every registration method takes an `ActorFactory`. Its `build` method runs
exactly once for the initial start and once for every supervised restart, so
ordinary actor fields are fresh incarnation-local state. The actor type does
not need `Clone`, and a synchronously constructible non-`Clone` guard or
resource can live directly in the actor. Construction happens inside the
supervised actor future, so a `build` panic follows the same supervision path
as a startup or run panic.

Constructor functions are the common case:

```rust,ignore
builder.actor("worker", Worker::new);
builder.add(Worker::new);
```

Closures automatically implement `ActorFactory`. For a small wired actor,
capture only durable configuration or handles and clone those while
constructing each incarnation:

```rust,ignore
let ledger = ledger_ref.clone();
builder.actor("gateway", move || Gateway::new(ledger.clone()));
```

Larger wiring can give that durable configuration a named spec type. Its
`build` method becomes the single intentional clone-per-incarnation site:

```rust,ignore
use tokio_otp::ActorFactory;

struct GatewaySpec {
    ledger: ActorRef<LedgerMsg>,
    exchange: Exchange,
}

impl ActorFactory for GatewaySpec {
    type Actor = Gateway;

    fn build(&self) -> Gateway {
        Gateway::new(self.ledger.clone(), self.exchange.clone())
    }
}

builder.actor("gateway", GatewaySpec { ledger, exchange });
```

Fallible or asynchronous acquisition belongs in `Actor::on_start` or at the
beginning of `RawActor::run`, where failure already participates in supervision
and readiness. Durable state that should survive restarts belongs behind a
shared handle, in a database, or in another actor.

## Struct Topologies

`#[derive(Topology)]` supports named-field structs whose fields implement
`RawActor`. Field names become actor labels verbatim, so supervisor child
ids, tracing fields, and stats stay human-readable without participating in
type checking or message routing. The generated `graph_with` accepts a
preconfigured `GraphBuilder` for graph name, mailbox capacity, and shutdown
timeouts; the generated `graph` uses `GraphBuilder::new()`.

The derive keeps topology shape in the type system:

- a field whose type is not an actor is a compile error
- wiring a ref with the wrong message type is a compile error
- every factory field must be present exactly once in the generated
  `<Topology>Factories` struct literal
- each factory's `ActorFactory::Actor` must be its declared actor type
- a topology with no actors is a compile error

Configure an individual actor's mailbox or message-size observation with a
normal Rust expression in `#[topology(options = ...)]`. Annotated fields use
`GraphBuilder::slot_with_options`; other fields retain the default FIFO
mailbox without message-size observation:

```rust,ignore
use tokio_otp::{ActorOptions, MailboxMode, Topology};

#[derive(Topology)]
struct MarketData {
    #[topology(options = ActorOptions::new()
        .mailbox(MailboxMode::Conflate)
        .message_size())]
    snapshots: SnapshotActor,
    orders: OrderActor,
}
```

The expression is type-checked against the field actor's message type, so
options that require `MessageSize` add that requirement only to the annotated
actor message.

## Dynamic and Advanced Builder Wiring

Use `GraphBuilder` directly when actors are dynamic, generated in a loop, or
need explicit observability names:

- `builder.add(factory)` registers an actor under its unqualified type name,
  suffixing repeats as `Worker-2`, `Worker-3`, and so on
- `builder.actor(id, factory)` registers an actor under an explicit id
- `builder.actor_with_options(id, factory, options)` combines per-actor settings
  such as mailbox delivery and message-size observation
- `builder.slot::<M>(id)` plus `builder.define(slot, factory)` opens and fills a
  token-protected slot for hand-written cyclic wiring

The direct builder still validates runtime configuration facts:

- duplicate actor labels
- slots that were opened but never filled
- empty graph names, empty actor labels, and zero mailbox capacity

One hazard comes with cyclic wiring: mailboxes are bounded, so two actors
that `send` to each other while both mailboxes are full deadlock permanently,
and a `call` cycle deadlocks at depth one. Use `try_send` on feedback edges,
and `call` only "downhill" along a DAG ordering of the graph.

## Conflating Mailboxes

FIFO mailboxes are right for commands: `send` waits for capacity and
`try_send` reports `MailboxFull`. High-rate state snapshots often need the
opposite policy—a slow consumer should skip stale updates instead of making
the producer fall behind. Configure those actors explicitly:

```rust,ignore
use tokio_otp::{ActorOptions, MailboxMode};

let latest = builder.actor_with_options(
    "latest-market",
    MarketActor::new,
    ActorOptions::new().mailbox(MailboxMode::Conflate),
);

let per_symbol = builder.actor_with_options(
    "market-by-symbol",
    KeyedMarketActor::new,
    ActorOptions::new().mailbox(MailboxMode::conflate_by_key(
        |tick: &Tick| tick.symbol_id,
    )),
);
```

`Conflate` stores at most one unread message. `ConflateByKey` stores one
unread message per distinct key, preserving the first-arrival order of keys;
its number of keys is bounded by `mailbox_capacity`, and a new key evicts the
oldest unread key when full. Both `send` and `try_send` replace stale state
without waiting for capacity. `ActorStats::messages_conflated` counts replaced
or evicted unread messages.

Because a conflating `send` never waits for capacity, awaiting it may complete
without yielding to the Tokio scheduler. A tight producer loop must call
`tokio::task::yield_now()` or rate-limit explicitly so other tasks can run,
especially on a single-thread runtime.

Keyed sends scan at most `mailbox_capacity` unread keys and may evaluate the
extractor for every comparison. Keep the extractor cheap and prefer numeric
or interned ids over allocating keys.

Use these modes only for idempotent state snapshots. They are deliberately
lossy and are unsuitable for commands. In particular, do not use `call`: if a
newer message replaces a request before it is handled, the caller receives
`CallError::ReplyDropped`.

## Runtime Handles

`ActorRef<M>` is the external entry point. It supports:

| Method | Behavior |
|--------|----------|
| `send` | Waits for a bound mailbox and retries across expected restart windows; FIFO queues wait for capacity, while conflating mailboxes replace stale state. |
| `try_send` | Returns immediately; FIFO queues report full capacity, while conflating mailboxes replace stale state. |
| `call` | Sends a message carrying `Reply<T>` and awaits the reply until its required caller-owned timeout expires. |
| `call_unbounded` | Sends a request without a local timeout; use only when another mechanism bounds the protocol lifetime. |

Refs are bound to long-lived mailbox bindings, not one actor incarnation. A
ref minted at wiring time keeps working across per-actor supervised
restarts. Delivery is at-most-once: `Ok` from `send` means the message was
accepted by the current incarnation's mailbox, not that it will be
processed.

See [Bounded request/reply](request-reply.md) for cancellation before and
after mailbox acceptance, FIFO backpressure, restart windows, and the
unknown-outcome rule for external side effects.

## Message Loss at Shutdown and Restart

`Actor` is the usual actor interface: you implement `handle`, and the
framework owns the receive loop. Its default shutdown behavior is fail-fast:
when shutdown is requested, queued messages are dropped and queued `call`
requests see `CallError::ReplyDropped`.

If an actor must finish messages already accepted by its mailbox, return
`DrainPolicy::Drain` from `drain_policy`. Hand-written `RawActor::run` loops are
still available as the escape hatch for custom loop control; after
`ctx.recv().await` returns `None` because shutdown was requested, such actors
can use `ctx.try_recv()` to drain immediately queued messages.

`ctx.try_recv()` returns the crate-owned `tokio_otp::TryRecvError`. Earlier
versions exposed Tokio's channel error at the same path; matching `Empty` and
`Disconnected` remains source-compatible, but code that passed the error to an
API expecting `tokio::sync::mpsc::error::TryRecvError` must now map those two
variants explicitly. This boundary keeps actor code independent of the mailbox
channel implementation.

Restarts have the same loss boundary. Each actor run binds a fresh mailbox, so
messages queued behind the message that makes an actor crash are lost with the
old mailbox. `send` retries while an actor is between bindings, but
it cannot recover messages that were already accepted by the old run.

## Blocking Work

Actors can offload blocking work through `ctx.run_blocking`. Its closure
receives a cancellation token that follows actor shutdown and is also
cancelled if the `run_blocking` future is dropped. Long-running closures
should check it periodically.

```rust,ignore
let engraved = ctx.run_blocking(move |token| {
    if token.is_cancelled() {
        return None;
    }
    Some(engrave(input))
}).await;
```

The closure can return any type, including an application-defined `Result`.
A panic resumes on the actor task. If a closure ignores cancellation, the
actor shutdown timeout remains the backstop; the blocking thread then runs
detached because Tokio cannot abort blocking work after it starts.

For intentionally detached or concurrent work, clone `ctx.myself()`, call
`tokio::task::spawn_blocking` directly, and send the result back as a message.
The `blocking_lifecycle` example demonstrates this mailbox-as-completion
pattern.

[`ActorRef`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorRef.html
