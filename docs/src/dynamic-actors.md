# Dynamic actors

A `DynamicTree` is an initially empty one-for-one scope whose membership can
change at runtime. It owns identity like an ordered tree and exposes its
dynamic capability before spawn. Both tree kinds spawn the same `RunningTree`
owner; after spawn, use `runtime.scope()` directly. `ScopeRef::kind()` reports
whether a navigated scope has dynamic membership.

## Standalone dynamic scope

```rust
use kokage::prelude::*;

struct Worker;

impl Actor for Worker {
    type Msg = String;

    async fn handle(
        &mut self,
        message: String,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        println!("{message}");
        Ok(())
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let runtime = DynamicTree::new().spawn()?;
let scope = runtime.scope();

let worker = scope
    .add_actor(ActorSpec::new("worker", || Worker))
    .await?;
assert!(scope.snapshot().child("worker").is_some());
worker.send("ready".to_owned()).await?;

scope.remove_child("worker").await?;
assert!(scope.snapshot().child("worker").is_none());
runtime.shutdown_and_wait().await?;
# Ok(())
# }
```

`add_actor` validates the declaration, reserves its scope-local id, and
returns the typed ref. Success means startup was scheduled; use
`wait_started`, a readiness protocol, or snapshots when subsequent work
requires the actor to be ready.

Terminal dynamic actors remain as inactive memberships by default. Select
`Restart::remove_when_done()` on an `ActorSpec` for an ephemeral child that
removes itself after terminal exit; `remove_child` explicitly removes either
kind.

The default retain/remove behavior is the same for declared and dynamically
added actors. A declared membership that removes itself can be recreated if an
enclosing declared supervisor later restarts; a runtime-added membership is not
replayed automatically. Dynamic scopes always use `Strategy::OneForOne`, so an
actor's exit, restart, or removal never initiates a sibling restart cycle.

Removal is a sequenced supervisor operation. `remove_child(id)` marks the
membership `Removing`, applies its configured `Shutdown`, runs `on_stop` when
cooperative shutdown reaches it, and finally detaches the child.
`Shutdown::drain_for` closes intake and drains the accepted message prefix
within its bound; `Shutdown::discard_after_current` finishes only the in-flight
handler and drops queued work. Grace expiry or `Shutdown::abort` may skip
unfinished drain and hook work. The removal future completes after detachment.

There is an intentional race boundary: a send may be accepted after removal is
requested but before the actor observes cancellation.
`Shutdown::drain_for` handles that accepted prefix if its bound permits;
`Shutdown::discard_after_current` drops any of the prefix still queued. Once
intake closes, `try_send` can return `SendErrorKind::NotRunning`, while an
awaited `send` waits for the final disposition and returns
`SendErrorKind::Terminated`. The `SendError` carrier returns the rejected
message for retry or rerouting; `send_timeout` also returns an unaccepted
message with `SendErrorKind::TimedOut` when its capacity/restart bound expires.
Applications that cannot lose already accepted work still need an explicit
[ownership protocol](ownership-transitions.md).

After detachment the same id can be added again, but the old `ActorRef` remains
terminal and never rebinds to the replacement. Compare snapshot `lineage` as
well as `generation`: restarts preserve lineage and increment generation; a
new membership gets a later lineage and starts at generation zero.

## Mailbox and restart policy

The inserted `ActorSpec` carries its own mailbox, restart, shutdown, and
message-size settings:

```rust
# use kokage::prelude::*;
# struct Worker;
# impl kokage::Actor for Worker { type Msg = String; async fn handle(&mut self, _: String, _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let worker = ActorSpec::new("worker", || Worker)
    .mailbox_capacity(32)
    .mailbox(MailboxMode::queue())
    .restart(Restart::on_failure());
# let _ = worker;
```

A scope's mailbox-capacity setting is its local default. Explicit spec settings
win.

## Adding to a nested scope

Mount a dynamic declaration inside an ordered tree, then navigate by subtree
id:

```rust
# use kokage::prelude::*;
# struct Worker;
# impl kokage::Actor for Worker { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let tree = OrderedTree::new().subtree("sessions", DynamicTree::new());
let runtime = tree.spawn()?;
runtime.scope().wait_started().await?;
let sessions = runtime
    .scope()
    .subtree("sessions")
    .expect("declared subtree");

let session = sessions
    .add_actor(ActorSpec::new("session-42", || Worker))
    .await?;
# let _ = session;
runtime.shutdown_and_wait().await?;
# Ok(())
# }
```

To insert a complete subtree atomically, use `add_subtree(id, tree)`. Tree
lowering validates all declarations before startup is scheduled. Duplicate
ids in the destination scope fail without partially mounting the subtree.
The same actor id may appear in different sibling scopes.

## Obtain the reference before the scope exists

A declaration's `ScopeRef` is stable before spawn, so actor factories can capture
future dynamic scopes directly:

```rust
# use kokage::prelude::*;
# struct Router(kokage::ScopeRef);
# impl kokage::Actor for Router { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let sessions = DynamicTree::new();
let sessions_handle = sessions.scope();
let router = ActorSpec::new("router", move || Router(sessions_handle.clone()));

let app = OrderedTree::new()
    // Start the captured scope before the actor that depends on it.
    .subtree("sessions", sessions)
    .actor(router);
# let _ = app;
```

No global `OnceLock` is needed, and moving the declaration into the parent
preserves the issued reference's identity.

## Advanced orchestration: scope references inside actors

Actor lifecycle contexts expose the containing scope through
`ctx.scope()`. Resolve a declared nested scope by id:

```rust,ignore
let children = ctx
    .scope()
    .subtree("children")
    .expect("declared child scope");
let worker = children
    .add_actor(ActorSpec::new("worker", WorkerFactory::default()))
    .await?;
```

This advanced orchestration surface returns a `ScopeRef`. The lookup works
during `on_start`, before the child scope starts. Do not await lifecycle
progress that depends on the current callback returning: for example, an actor
cannot await its own scope's readiness from `on_start`. To receive a lifecycle
result as a later actor message, clone the scope into an `async move` block and
pass the bounded future to `Context::offload`, such as
`ctx.offload(deadline, async move { children.wait_started().await }, map)`.

Declare a leader-owned scope explicitly:

```rust
# use kokage::prelude::*;
# struct Leader;
# impl kokage::Actor for Leader { type Msg = (); async fn handle(&mut self, (): (), _: &mut kokage::Context<'_, Self>) -> kokage::ExitResult { Ok(()) } }
let session = OrderedTree::new().subtree(
    "session-runtime",
    OrderedTree::new()
        .strategy(Strategy::OneForAll)
        .actor(ActorSpec::new("leader", || Leader))
        .subtree("children", DynamicTree::new()),
);
# let _ = session;
```

The explicit shape makes the restart relationship reviewable. `OneForAll`
shares fate between leader and child scope; `RestForOne` can instead encode
declaration-order ownership.

## Keep membership single-writer

A practical dynamic design assigns one actor as the membership writer. That
actor serializes add/remove decisions and reconciles ambiguous outcomes with a
snapshot. Treat caller cancellation as an unknown result: the control request
may already have reached the supervisor.

Stable domain ids should be distinct from incarnation ids when a replacement
can overlap a predecessor's removal. An epoch or generation suffix prevents a
late cleanup from targeting its successor.

## Name-based discovery

Typed refs should be passed during construction whenever possible. If the
application needs discovery, implement it as an ordinary directory actor.
That keeps names, permissions, stale-entry cleanup, and consistency policy in
application code rather than the runtime.
