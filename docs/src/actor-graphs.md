# Actor wiring

An `ActorSpec<M>` is the complete declaration for one actor. It combines a
scope-local id, an incarnation factory, mailbox configuration, and per-actor
restart and shutdown policy. The same declaration can be placed in a static
tree, inserted into a dynamic scope, or converted for a direct host.

## Straight-line wiring

For acyclic dependencies, obtain a ref from a spec before moving it:

```rust
# use kokage::{ActorSpec, OrderedTree};
# struct Press;
# struct FrontDesk(kokage::ActorRef<()>);
# impl kokage::Actor for Press { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
# impl kokage::Actor for FrontDesk { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let press_actor = ActorSpec::new("press", || Press);
let press = press_actor.actor_ref();
let front_desk_actor = ActorSpec::new("front-desk", {
    let press = press.clone();
    move || FrontDesk(press.clone())
});
let front_desk = front_desk_actor.actor_ref();

let tree = OrderedTree::new()
    .actor(press_actor)
    .actor(front_desk_actor);
# let _ = (press, front_desk, tree);
```

## Cyclic wiring with slots

When factories refer to each other, create all `ActorSlot` values and refs
first. `define` consumes a slot and returns its `ActorSpec`, making a
partially defined declaration structurally impossible:

```rust
# use kokage::{ActorSlot, OrderedTree};
# struct Left(kokage::ActorRef<()>);
# struct Right(kokage::ActorRef<()>);
# impl kokage::Actor for Left { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
# impl kokage::Actor for Right { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let left_slot = ActorSlot::<()>::new("left");
let left = left_slot.actor_ref();
let right_slot = ActorSlot::<()>::new("right");
let right = right_slot.actor_ref();

let left_actor = left_slot.define({
    let right = right.clone();
    move || Left(right.clone())
});
let right_actor = right_slot.define({
    let left = left.clone();
    move || Right(left.clone())
});

let tree = OrderedTree::new()
    .actor(left_actor)
    .actor(right_actor);
# let _ = (left, right, tree);
```

Rust checks the slot message type and prevents defining the same slot twice.
The tree checks placement rules when it is spawned or inserted.

## Incarnation-local state

Every spec holds an `ActorFactory`. Its `build` method runs once for the
initial incarnation and once for every restart. Factory captures survive actor
failure; values created by `build` reset.

With the default `derive` feature, `#[derive(kokage::ActorFactory)]`
generates a reusable factory from a named-field actor. Ordinary fields are
cloned into each incarnation; `#[factory(default)]` fields are rebuilt with
`Default`.

```rust
# use std::sync::Arc;
# struct Client;
# impl Clone for Client { fn clone(&self) -> Self { Self } }
#[derive(kokage::ActorFactory)]
struct Worker {
    client: Client,
    #[factory(default)]
    pending: Vec<String>,
}
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
```

Fallible or asynchronous initialization belongs in `Actor::on_start`, where
failure participates in supervision and readiness.

## Mailbox and policy configuration

Configure an individual declaration before placement:

```rust
# use kokage::{ActorSpec, MailboxMode, RestartPolicy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = String; async fn handle(&mut self, _: String, _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let worker = ActorSpec::new("worker", || Worker)
    .mailbox_capacity(128)
    .mailbox(MailboxMode::queue())
    .restart(RestartPolicy::Always)
    .message_size(|message: &String| message.len());
# let _ = worker;
```

An ordered scope has a default of 64 messages per actor. Explicit settings on
a spec win. The default is scope-local: a nested subtree starts with 64 rather
than inheriting its parent's setting, so configure the subtree explicitly when
it needs a different default. A zero capacity, an empty actor id, or duplicate
ids in one scope are reported during tree lowering. The same local id in
different sibling scopes is legal.

Bounded cyclic calls can still deadlock through backpressure. Prefer
asynchronous `send`, offload bounded calls from the actor loop, or design a
directional request flow.

## Direct hosting

`ActorSpec::into_runnable` materializes one `host::RunnableActor` for tests or
hosts with their own supervision story without applying supervisor-placement
validation. Tree placement and dynamic insertion reject an explicit zero
mailbox capacity through their ordinary error types; direct hosts are
responsible for validating configuration before running it. Normal
applications should place the spec in `OrderedTree` or `DynamicTree`.
