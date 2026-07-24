# Dynamic Actors

A runtime does not need a graph at all: `Runtime::builder().build()` starts
empty and idles until `RuntimeHandle::add_actor` adds a typed actor. Each
added actor becomes a supervised child whose id is the actor's label, and
`add_actor` returns the typed `ActorRef<M>` directly — there is no registry
and no string lookup. Refs travel the way any other value does: cloned into an
incarnation by its `ActorFactory`, or delivered by message. Closures and
zero-argument constructor paths implement `ActorFactory` automatically; named
spec structs are useful when durable configuration deserves its own type.

```rust,no_run
use tokio_otp::prelude::Continue;
use tokio_otp::{Actor, ActorContext, ActorRef, ActorResult, DynamicActorOptions, Runtime};
use tokio_supervisor::Strategy;

struct FrontDesk {
    rush: Option<ActorRef<String>>,
}

enum FrontDeskMsg {
    SetRushPress(ActorRef<String>),
    Order(String),
}

impl Actor for FrontDesk {
    type Msg = FrontDeskMsg;

    async fn handle(
        &mut self,
        message: FrontDeskMsg,
        _ctx: &ActorContext<FrontDeskMsg>,
    ) -> ActorResult {
        match message {
            FrontDeskMsg::SetRushPress(rush) => self.rush = Some(rush),
            FrontDeskMsg::Order(order) => {
                self.rush
                    .as_ref()
                    .expect("rush press ref delivered before orders")
                    .send(order)
                    .await?;
            }
        }
        Ok(Continue)
    }
}

struct RushPress;

impl Actor for RushPress {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &ActorContext<String>) -> ActorResult {
        println!("RUSH printed {order}");
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::builder().strategy(Strategy::OneForOne).build()?;
    let handle = runtime.spawn();

    let orders = handle
        .add_actor("front-desk", || FrontDesk { rush: None }, DynamicActorOptions::default())
        .await?;
    let rush = handle
        .add_actor("rush-press", || RushPress, DynamicActorOptions::default())
        .await?;

    // Distribute the ref by message — the mailbox is the discovery channel.
    orders.send(FrontDeskMsg::SetRushPress(rush.clone())).await?;
    orders.send(FrontDeskMsg::Order("wedding invites x50".into())).await?;
    rush.send("vip banners x2".into()).await?;

    handle.remove_child("front-desk").await?;
    handle.remove_child("rush-press").await?;
    handle.shutdown_and_wait().await?;
    Ok(())
}
```

`DynamicActorOptions` carries the new child's restart policy, shutdown policy,
optional restart intensity, and terminal-removal behavior. A
`RestartPolicy::Never` actor is removed automatically after either a clean or
failed exit, matching OTP temporary-child semantics; other restart policies
retain a terminal child in the supervisor snapshot by default. Override either
default with `remove_on_exit(bool)`. Removal only follows an exit the policy
will not restart, so it never interrupts a restart cycle. Watches still receive
`Down` followed by `Terminated` before the membership disappears. Consequently,
`Terminated` alone does not mean the child id is reusable: wait for removal to
complete or use a fresh id rather than immediately re-adding the same one.

With a group strategy, opting a non-`Never` actor into removal makes timing
observable. If its non-restarted exit is handled before a later group restart,
removal is permanent and that restart cannot revive it. If the actor exits while
an already-active group restart is draining it, the exit belongs to that restart
cycle and the actor is respawned.

These defaults apply only to actors added with `add_actor`. Actors declared in
the static graph remain registered after terminal exit, even when
`RuntimeBuilder::actor_restart` gives them `RestartPolicy::Never`; static
membership can be recreated from the graph when its supervisor restarts.

`add_actor` returns an actor ref matching the factory's actor message type, and
the same ref keeps working across restarts of that actor. Its stability ends at
the membership boundary: remove an actor and that ref becomes terminal. Adding
another actor with the same id returns a fresh ref; the old one never silently
rebinds to the new occupant.

Removal is sequenced supervisor child removal. `remove_child(label)` marks the
membership `Removing` and starts its configured shutdown. When cooperative
shutdown completes within its grace period, an `Actor` stops its normal receive
loop, closes external intake, and applies its `DrainPolicy`: `Drain` handles the
queued prefix, while `Discard` drops it. It then runs `on_stop`, terminates the
mailbox binding, and detaches the child. Immediate abort, or expiry of the
cooperative grace period, can skip any remaining drain or hook work before
detachment. The removal future completes after detachment.

There is an intentional race boundary: a send can be accepted after removal is
requested but before the actor observes cancellation. `Drain` then closes
intake and handles that accepted prefix; `Discard` drops it. After intake closes,
`try_send` can report `MailboxClosed`; an awaited `send` waits for the final
disposition and then reports `ActorTerminated`. There is no separate `Draining`
error. An application that must not lose accepted work during a membership
change needs an ownership protocol, described in
[Ownership during membership transitions](ownership-transitions.md).

A runtime can be reduced back to zero actors and keeps running until
`shutdown()` is requested.

## Adding to a nested supervisor

`RuntimeHandle::add_actor` targets the handle's own supervisor. For subtrees
configured with `RuntimeBuilder::subtree`, obtain the actor-aware nested handle
and add the actor normally:

```rust,ignore
let venue = handle.subtree("coinbase").expect("venue is running");
let subscription = venue
    .add_actor(
        "btc-usd",
        Subscription::new,
        DynamicActorOptions::default(),
    )
    .await?;
```

The actor's label (`"btc-usd"` above) is its child id within the nested
supervisor, so remove it through the same handle with
`venue.remove_child("btc-usd")`. Actors added this way are supervised, restart
normally, and appear in both `venue.actor_stats()` and the parent handle's
recursive `actor_stats()` result.

Subtrees can also be added dynamically. `add_subtree` takes the same
`RuntimeBuilder` used for static composition and returns an actor-aware handle:

```rust,ignore
let sessions = handle
    .add_subtree("sessions", Runtime::builder())
    .await?;
let session = sessions
    .add_subtree(
        session_id,
        Runtime::builder().graph(session_graph),
    )
    .await?;
session
    .add_actor(
        "current-run",
        Run::new,
        DynamicActorOptions::default(),
    )
    .await?;
```

Dynamic subtrees use the same actor registry nodes and recursive stats path as
statically declared subtrees. Removing one with `remove_child` removes that
registry node, and retained handles fail control operations with
`ControlError::Unavailable`.

Restart recovery follows the builder boundary. If a dynamic subtree itself
restarts, actors and nested subtrees declared in the builder are recreated;
children added later through its handle are not and must be replayed by the
application. If the parent supervisor that received `add_subtree` restarts,
the dynamic subtree itself is not recreated. Restart intensity remains per
child, and shutdown remains concurrent under the parent's shared deadline.

Both of those boundaries held up unchanged under the first realistic dynamic
workload, the `agent_control` example's per-conversation subtrees. Per-child
intensity means a storm of short-lived subtree crashes never trips an
aggregate parent budget; the signal that wants to aggregate across children —
run failures across every conversation — is application state, and the
example's guard already owns it (`watch_restarts` plays the same role for
supervisor-driven restarts). And the teardown ordering that mattered —
checkpoint before removal, transient children before the parent that owns
them — was either enforced by the application before it requested removal or
fell out of reverse-declaration-order shutdown inside the subtree, so
concurrent teardown across sibling subtrees needed no cross-sibling drain
sequencing.

If an id is removed and later reused, compare the snapshot's
`membership_epoch` (also present in runtime-scoped `ActorStats`) as well as its
`generation`: restarts keep an epoch and increment the generation, while a new
membership receives a later epoch and starts again at generation zero. For
recursive stats, also compare `ActorStats::supervisor_path`; it distinguishes
otherwise identical local ids and epochs in sibling or restarted subtrees.

For a raw nested `Supervisor` that does not need actor-aware behavior, use
`RuntimeHandle::add_supervisor`, `RuntimeHandle::supervisor`, and the ordinary
`tokio-supervisor` child APIs as the lower-level escape hatch. Raw children are
not part of runtime actor stats. Raw removals also bypass the runtime registry's
immediate bookkeeping; the next `actor_stats()` sample reconciles tracked
entries against the supervisor snapshot and prunes removed children.

## Name-based discovery, when you want it

When an application genuinely wants name-based discovery — plugins looking
each other up at runtime, say — build it as an ordinary actor. A typed
directory is about twenty lines:

```rust,ignore
enum DirectoryMsg<M> {
    Insert(String, ActorRef<M>),
    Get(String, Reply<Option<ActorRef<M>>>),
}

struct Directory<M> {
    entries: HashMap<String, ActorRef<M>>,
}
```

Insert refs as actors are created, and `call` the directory to resolve a name
to a typed ref. Because the directory is self-hosted, *you* choose its
semantics — namespacing, removal, versioning — instead of inheriting a
framework registry's. The runnable version is
`crates/tokio-otp/examples/directory.rs`.

Note that a directory instance is homogeneous: `Directory<M>` holds refs to
actors whose message type is that one `M`, and that is what makes lookups
fully typed with no downcasting. If several message types need discovery, run
one small directory per type. A single heterogeneous registry is possible by
storing `Box<dyn Any>` and downcasting on `Get`, but that reintroduces the
runtime type checks a stringly-typed framework registry would have imposed —
which is exactly the cost this design avoids.
