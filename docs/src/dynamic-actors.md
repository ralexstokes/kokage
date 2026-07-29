# Dynamic Actors

A dynamic tree does not need a graph: `DynamicTree::new().spawn()` returns a
`DynamicRuntime`, starts empty, and idles until that runtime adds a typed actor. Each
added actor becomes a supervised child whose id is the actor's label, and
`add_actor` returns the typed `ActorRef<M>` directly — there is no registry
and no string lookup. Refs travel the way any other value does: cloned into an
incarnation by its `ActorFactory`, or delivered by message. Closures and
zero-argument constructor paths implement `ActorFactory` automatically; named
spec structs are useful when durable configuration deserves its own type.

```rust,no_run
use kokage::{
    Actor, ActorRef, ActorResult, ActorSpec, MessageContext, DynamicTree,
};

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
        _ctx: &mut MessageContext<'_, Self>,
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
        Ok(())
    }
}

struct RushPress;

impl Actor for RushPress {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        println!("RUSH printed {order}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = DynamicTree::new().spawn()?;

    let orders = runtime
        .add_actor(ActorSpec::new("front-desk", || FrontDesk { rush: None }))
        .await?;
    let rush = runtime
        .add_actor(ActorSpec::new("rush-press", || RushPress).mailbox_capacity(32))
        .await?;

    // Distribute the ref by message — the mailbox is the discovery channel.
    orders.send(FrontDeskMsg::SetRushPress(rush.clone())).await?;
    orders.send(FrontDeskMsg::Order("wedding invites x50".into())).await?;
    rush.send("vip banners x2".into()).await?;

    runtime.remove_child("front-desk").await?;
    runtime.remove_child("rush-press").await?;
    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

`ActorSpec` is the same declaration used for graph registration and tree
placement. It carries the new child's factory, mailbox settings, restart and
shutdown policies, optional restart configuration, and terminal-membership
behavior. A runtime's restart and shutdown defaults are inherited unless the
declaration uses the `restart(...)` or `shutdown(...)` builder methods to override them. Those
methods are how the runtime distinguishes an explicit override from an
inherited default.

The hosting runtime scope's mailbox
capacity is the default: graph-backed scopes inherit their graph builder's
setting, while graphless scopes use the library default.
`ActorSpec::mailbox_capacity` overrides that default for one actor, and
unkeyed `MailboxMode::conflate()` always stores one unread message and ignores
either capacity. A zero per-actor capacity is rejected by `add_actor`.

`ActorSpec` retains terminal memberships by default everywhere, whether the
declaration is registered in a graph, placed directly in an ordered tree, or
inserted into a dynamic scope. For an ephemeral child, select
`terminal_membership(TerminalMembership::Remove)` explicitly. An exit is
terminal only when the restart policy declines to restart it, so removal never
interrupts a restart cycle. Watches still receive `Down` followed by
`Terminated` before the membership disappears. The child id becomes reusable
when removal completes; wait for the snapshot to drop the membership before
re-adding the same id.

The terminal-membership choice has the same meaning for static placement and
dynamic insertion: `Retain` leaves the stopped membership visible, while
`Remove` detaches it after the terminal lifecycle events. Dynamic scopes always
use `Strategy::OneForOne`, so one actor's exit, restart, or removal never starts
a sibling restart cycle. Once terminal removal completes, only a new
`add_actor` call can create another membership with that child id.

`add_actor` returns an actor ref matching the factory's actor message type, and
the same ref keeps working across restarts of that actor. Its stability ends at
the membership boundary: remove an actor and that ref becomes terminal. Adding
another actor with the same id returns a fresh ref; the old one never silently
rebinds to the new occupant. The add reply means the membership was inserted
and startup was scheduled. In sequential mode the actor may still be queued
behind another child's readiness gate; the ref is usable immediately, while
`RuntimeHandle::wait_started()` waits for readiness.

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
`try_send` can report `TrySendError::Closed`; an awaited `send` waits for the
final disposition and then returns `SendError`. There is no separate `Draining`
error. An application that must not lose accepted work during a membership
change needs an ownership protocol, described in
[Ownership during membership transitions](ownership-transitions.md).

A runtime can be reduced back to zero actors and keeps running until
`shutdown()` is requested.

## Adding to a nested supervisor

`DynamicRuntime::handle()` preserves that statically known membership
capability in a `DynamicRuntimeHandle`. `RuntimeHandle::dynamic()` remains for
runtime-discovered scopes, where the scope kind is not known until navigation.
For subtrees
declared with `OrderedTree::subtree`, obtain the actor-aware nested handle
and add the actor normally:

```rust,ignore
let venue = handle.subtree("coinbase").expect("venue is running");
let subscription = venue
    .dynamic().expect("dynamic venue")
    .add_actor(ActorSpec::new("btc-usd", Subscription::new))
    .await?;
```

The actor's label (`"btc-usd"` above) is its child id within the nested
supervisor, so remove it through the same handle with
`venue.dynamic().expect("dynamic venue").remove_child("btc-usd")`. Actors added
this way are supervised, restart normally, and appear in both
`venue.actor_stats()` and the parent handle's recursive `actor_stats()` result.

Subtrees can also be added dynamically. `add_subtree` consumes an
`OrderedTree` or `DynamicTree` and returns its actor-aware handle:

```rust,ignore
let sessions = handle
    .dynamic().expect("dynamic parent")
    .add_subtree("sessions", DynamicTree::new())
    .await?;
let session = sessions
    .dynamic().expect("dynamic sessions scope")
    .add_subtree(
        session_id,
        OrderedTree::graph(session_graph),
    )
    .await?;
session
    .dynamic().expect("dynamic session scope")
    .add_actor(ActorSpec::new("current-run", Run::new))
    .await?;
```

Dynamic subtrees use the same actor registry nodes and recursive stats path as
statically declared subtrees. Removing one with `remove_child` removes that
registry node, and retained handles fail control operations with
`ControlError::Unavailable`. `add_subtree` resolves when insertion and
immediate startup are scheduled. These operations require a dynamic parent;
an ordered parent's `dynamic()` accessor returns `None`.

Restart recovery follows the declaration boundary. If a dynamic subtree itself
restarts, actors and nested subtrees in its tree are recreated;
children added later through its handle are not and must be replayed by the
application. If the parent supervisor that received `add_subtree` restarts,
the dynamic subtree itself is not recreated. Restart intensity remains per
child. Dynamic siblings shut down concurrently; each child escalates at its
own configured grace, so total teardown is bounded by the largest grace plus
that child's tidy-abort accounting beat.

## Obtain the handle before the scope exists

Every tree owns a stable identity immediately. Call `handle()` when an actor
factory must capture the scope it will later own or reconcile:

```rust,ignore
let sessions_tree = DynamicTree::new();
let sessions = sessions_tree.handle();

let router = ActorSpec::new("router", move || Router::new(sessions.clone()));
let router_ref = router.actor_ref();

let app_tree = OrderedTree::new()
    // Declaration order makes sessions ready before Router::on_start.
    .subtree("sessions", sessions_tree)
    .actor(router);
let app_handle = app_tree.handle();
let app = app_tree.spawn()?;
# drop((router_ref, app_handle, app));
```

Here `sessions` is a `DynamicRuntimeHandle`, so it can add and remove members
directly once the nested scope starts. Use `sessions.as_runtime_handle()` when
only the common observation and lifecycle surface is needed, or
`sessions.into_runtime_handle()` to erase the statically known capability.

`OrderedTree` and `DynamicTree` deliberately do not implement `Clone`: one
identity can bind to one eventual runtime. Moving a tree through `subtree` or
`actor_with_scope` transfers that identity into the parent, so no `OnceLock`
or post-spawn handle injection is needed.

Before binding, control operations return `ControlError::Unavailable`, while
`snapshot()` exposes declared children as starting and lifecycle/snapshot
subscriptions are already valid. `wait_started()` waits for a real bound
incarnation, including an empty dynamic scope. Dropping an unspawned tree,
failing `spawn()`, or having an insertion rejected makes the identity terminal
and closes retained streams.

The same rule applies to the builders returned by `Supervisor::ordered()` and
`Supervisor::dynamic()`, whose `handle()` returns a raw
`SupervisorHandle`. Supervisor declarations are single-use; handle clones
continue to address one identity.

## Scope handles inside actors

Every actor stage has the same safe scope surface: `host::ActorContext`,
`StartContext`, `MessageContext`, and `StopContext` return `RestrictedScope`
from `supervisor()` and `children()`. Observation always works. Insertion
(`add_actor`, `add_subtree`) schedules startup rather than waiting for it and
lives on the capability returned by `RestrictedScope::dynamic()`. Ordered
scopes return `None`. `subtree()` returns another `RestrictedScope`, so
navigation cannot widen the surface or expose a full `RuntimeHandle`.

Lifecycle waits are withheld because their progress may depend on the current
actor returning from startup, its receive loop, a handler, or teardown. Dynamic
membership removal is available, but awaiting removal of the current actor (or
another child whose teardown depends on this callback returning) has the same
cycle and must be avoided. During `on_start` or `handle`, use
`LiveContext::spawn_scope_wait` when a lifecycle wait must happen. The wait
runs outside the actor task and its result returns through the actor's ordinary
mailbox:

```rust,ignore
let children = ctx.children().expect("leader has a child scope");
ctx.spawn_scope_wait(
    &children,
    |children| async move {
        children.wait_started().await.map_err(|_| ())?;
        children
            .dynamic()
            .ok_or(())?
            .add_actor(ActorSpec::new("worker", || Worker))
            .await
            .map_err(|_| ())?;
        Ok::<_, ()>(())
    },
    |result| Msg::ScopeReady(result),
);
```

The task belongs to the current actor incarnation: stop or restart cancels it,
and `DrainPolicy::Drain` does not wait for it. Its mapped result goes through
the starting incarnation's ordinary mailbox, so mailbox capacity, FIFO order,
and conflation still apply; it cannot leak into a later restart. Retain the
returned `TaskHandle` when a message-driven wait needs explicit
cancellation, and monitor `ActorStats::outstanding_scope_waits` for waits that
do not finish. A panic in the wait or mapper that the receive loop observes
while the incarnation is live fails the actor normally under supervision. As
with an offload, shutdown or restart can instead win the race and abort the
task; an unobserved result or panic is then discarded with that incarnation.

Shutdown has the mirror-image restriction, so `StopContext::supervisor()` and
`StopContext::children()` return the same `RestrictedScope`. A stopping child
is still attached: cooperative removal waits for `on_stop` to return before
detaching it. Awaiting `wait()`, `shutdown_and_wait()`, or
`remove_child()` on its own id from `on_stop` therefore waits on a detach that
is waiting on `on_stop`, and the cycle breaks only when the shutdown grace
period expires and aborts the actor — a clean stop reported as a timed-out
one. Fire-and-forget `shutdown()`, observation, and insertion remain. Teardown
cannot start a new scope wait because `StopContext` does not implement
`LiveContext`. Work that must observe another child's teardown belongs in a
separate, explicitly owned actor or task whose lifetime is not already ending.

For the common leader-and-workers shape, declare an actor-owned scope:

```rust,ignore
let sessions = OrderedTree::new().actor_with_scope(
    "session-runtime",
    session_actor,
    DynamicTree::new(),
    Strategy::RestForOne,
);
```

This lowers to an ordered node containing `[session_actor, children]`.
`children()` is `Some` only for that leader and returns the inner scope's
pre-spawn handle without changing the actor factory signature. The inner scope
starts after the leader reports ready and stops before the leader is cancelled.
Consequently, work launched during `on_start` must be pipelined: let `on_start`
return, wait for `children.wait_started()` in the pipelined work, then add
members. Every stage yields a `RestrictedScope`; insertion itself is available
through `dynamic()` because it only schedules startup, while lifecycle waits
require `spawn_scope_wait` during the live stages. Shutdown cannot start such
work.

`actor_with_scope` takes the restart relationship explicitly. `RestForOne`
means leader failure recycles the leader and owned scope, while a worker
failure stays inside the owned scope. Pass `Strategy::OneForAll` when either
side failing must recycle both. Snapshot paths are
`root.<node>.<leader-label>` and `root.<node>.children.<worker>`.

## Keep dynamic membership single-writer and reconcile it

Choose one actor as the membership writer and route all adds, removals, and
replay decisions through it. On every writer incarnation, align its durable
intent with the truthful supervisor view in this order:

1. Start `watch_lifecycle().direct_children()` first. The underlying lifecycle
   watch is recursive; omit the depth filter when descendant events are part
   of the reconciliation. `watch_lifecycle_to` is the mapped direct-child
   convenience when delivery should go through an actor mailbox.
2. Read `snapshot()` second and reconcile its current children.
3. Remember `snapshot.lifecycle_seq`.
4. Apply only ordinary watched events with a larger `seq`; treat `Added` as an
   idempotent upsert.
5. On `Lagged`, fetch and reconcile a fresh snapshot and replace the sequence
   baseline with its `lifecycle_seq` before consuming more events.

Because a pre-spawn handle can start the watch before spawn, the same recipe is
gap-free for the first incarnation as well as restarts. A pre-spawn snapshot
already projects declared membership, while the first actual `Added` events
arrive only after bind. Dynamically added children are not replayed by the
runtime after their parent restarts: the single writer compares durable intent
with this ordered view and re-adds or removes members as needed.

Both of those boundaries held up unchanged under the first realistic dynamic
workload, the `agent_control` example's per-conversation subtrees. Per-child
intensity means a storm of short-lived subtree crashes never trips an
aggregate parent budget; the signal that wants to aggregate across children —
run failures across every conversation — is application state, and the
example's guard already owns it (the lifecycle pump's cumulative counters play
the same role for supervisor-driven restarts). And the teardown ordering that mattered —
checkpoint before removal, transient children before the parent that owns
them — was either enforced by the application before it requested removal or
fell out of reverse-declaration-order shutdown inside the subtree, so
concurrent teardown across sibling subtrees needed no cross-sibling drain
sequencing.

If an id is removed and later reused, compare the snapshot's
`lineage` (also present in runtime-scoped `observe::ActorStats`) as well as its
`generation`: restarts keep a lineage and increment the generation, while a new
membership receives a later lineage and starts again at generation zero. For
recursive stats, also compare `observe::ActorStats::supervisor_path`; it distinguishes
otherwise identical local ids and lineages in sibling or restarted subtrees.

Use `RuntimeHandle::dynamic().expect("dynamic scope").add_child(host::ChildSpec)` for a non-actor task in a dynamic
scope and `add_subtree` for a nested actor-aware scope. Task children are not
part of runtime actor stats, but they remain visible in snapshots and lifecycle
watches. Applications that need raw `Supervisor` construction or
`SupervisorHandle` APIs should depend on `kokage-supervisor` directly; those
low-level construction and control types are not re-exported by `kokage`.

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
`crates/kokage/examples/directory.rs`.

Note that a directory instance is homogeneous: `Directory<M>` holds refs to
actors whose message type is that one `M`, and that is what makes lookups
fully typed with no downcasting. If several message types need discovery, run
one small directory per type. A single heterogeneous registry is possible by
storing `Box<dyn Any>` and downcasting on `Get`, but that reintroduces the
runtime type checks a stringly-typed framework registry would have imposed —
which is exactly the cost this design avoids.
