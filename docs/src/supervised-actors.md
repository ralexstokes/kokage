# Supervised actors

Every actor declaration placed in a tree becomes its own supervised child.
That gives each logical actor independent restart policy, mailbox binding,
lifecycle events, and statistics.

```rust
# use kokage::{ActorSpec, OrderedTree, RestartPolicy, ShutdownPolicy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let worker = ActorSpec::new("worker", || Worker)
    .restart(RestartPolicy::Permanent)
    .shutdown(ShutdownPolicy::default());
let worker_ref = worker.actor_ref();

let runtime = OrderedTree::new()
    .actor(worker)
    .spawn()?;
# let _ = (worker_ref, runtime);
# Ok::<(), kokage::SupervisorBuildError>(())
```

A restart invokes the retained factory, binds a fresh incarnation mailbox, and
reconnects the same `ActorRef`. Messages accepted by the failed incarnation
are lost; delivery is at-most-once.

## Explicit topology

`OrderedTree` is the static construction front door. Nest trees to create
restart boundaries:

```rust
# use kokage::{ActorSpec, OrderedTree, Strategy};
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::MessageContext<'_, Self>) -> kokage::ActorResult { Ok(()) } }
let venues = OrderedTree::new()
    .strategy(Strategy::OneForAll)
    .actor(ActorSpec::new("feed", || Worker))
    .actor(ActorSpec::new("gateway", || Worker));

let tree = OrderedTree::new()
    .actor(ActorSpec::new("router", || Worker))
    .subtree("venues", venues);
# let _ = tree;
```

The id `feed` is local to `venues`. Another sibling scope may also contain a
`feed`; duplicate ids inside the same scope are rejected at spawn.

Scope configuration such as strategy, restart budget, and mailbox-capacity
default is applied where it is declared. Per-actor settings stay on
`ActorSpec`.

## Cycles remain typed

Use `ActorSlot` when factories form a cycle. Mint every ref, define the slots,
then place the returned specs in whichever scopes express the intended failure
relationships. Wiring does not choose topology.

## Dynamic membership

A `DynamicTree` is an initially empty one-for-one scope. Its handle accepts
`ActorSpec` values and nested trees at runtime. Static and dynamic scopes
share runtime handles, snapshots, lifecycle streams, and shutdown behavior;
only their membership capability differs.
