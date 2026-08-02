# Architecture

This document explains how the `kokage` codebase is structured: what the core
machinery is, what is layered on top of it, and where the boundaries are. For
*usage* documentation, see the book under `docs/` and the crate rustdoc; this
document is about how the library is built.

## The big picture

Kokage is built as a small stack of layers, each one strictly on top of the
one below:

```
┌─────────────────────────────────────────────────────────────┐
│ satellite crates: kokage-derive, kokage-console             │
├─────────────────────────────────────────────────────────────┤
│ public tree layer: Tree / RunningTree / ScopeRef            │
│   crates/kokage/src/supervision.rs, src/runtime.rs          │
├─────────────────────────────────────────────────────────────┤
│ Actor abstraction: the Actor trait + generated event loop   │
│   crates/kokage/src/actor/handler.rs, context.rs            │
├─────────────────────────────────────────────────────────────┤
│ actor machinery: RawActor, bindings, mailboxes, monitors    │
│   crates/kokage/src/actor/ (raw.rs, binding.rs, graph.rs…)  │
├─────────────────────────────────────────────────────────────┤
│ supervision engine (private, actor-unaware)                 │
│   crates/kokage/src/supervisor/                             │
└─────────────────────────────────────────────────────────────┘
```

Two facts define the whole design:

1. **The supervision engine does not know about actors.** Its child kinds are
   exactly `Task` and `Supervisor` (`supervisor/child.rs`). An actor is
   lowered into an ordinary supervised task child.
2. **The high-level `Actor` trait is not a separate execution path.** It is a
   blanket implementation of `RawActor` (`actor/handler.rs`) — one generated
   receive loop. Everything an `Actor` does ultimately runs through the same
   machinery a hand-written raw actor uses.

A third rule shapes the public surface: **no `tokio` or `tokio_util` types
appear in the public API** (enforced by `scripts/check-public-api.sh`). Kokage
runs on Tokio today, but the boundary types (`CancellationToken`, `Guard`,
refs, watches) are its own.

## Layer 1: the supervision engine (`src/supervisor/`)

This module is **private** (`lib.rs` declares `mod supervisor;` with no `pub`).
Only policy and observation types are re-exported. The engine's own types are
wrapped one-for-one by the public layer:

| internal (`supervisor/`)   | public                              |
|----------------------------|-------------------------------------|
| `Supervisor`               | `Tree` / `DynamicTree`              |
| `RunningSupervisor`        | `RunningTree`                       |
| `SupervisorHandle`         | `ScopeRef`                          |
| `DynamicSupervisorHandle`  | `DynamicScopeRef`                   |
| `ChildSpec`                | `TaskSpec` / `ActorSpec` / `SubtreeSpec` |

A *scope* is one supervisor node in the tree. `ScopeKind` (`scope.rs`) is
either `Ordered` — declared sequence, sequential readiness-gated startup,
reverse-order teardown, immutable membership — or `Dynamic` — runtime
membership, concurrent start/stop, always `OneForOne`.

### Children and policies

A child is a `ChildDefinition` (`child.rs`): id, `RestartPolicy`, `Shutdown`
policy, readiness mode, and a `ChildKind` (task factory or nested supervisor).
Policies are deliberately split:

- `Strategy` (`strategy.rs`) lives on the *scope* and decides fate-sharing
  between siblings: `OneForOne`, `OneForAll`, `RestForOne`.
- `RestartPolicy` (`restart.rs`) is per-child and carries the condition
  (`Always` / `OnFailure` / `Never`), the restart budget (`max_restarts`
  within a window), and `Backoff` (fixed or exponential with jitter).
- `Shutdown` (`shutdown.rs`) is per-child: `Graceful { grace }` or `Abort`.

### The runtime loop

`SupervisorRuntime` (`runtime/supervision.rs`) is the per-scope state machine,
running as one Tokio task. All children spawn into a single `JoinSet`; a
`biased` select loop prioritizes shutdown > control commands > child readiness
> nested snapshots > deadlines > child joins.

Identity is a triple: **key** (slab slot) / **lineage** (which membership) /
**generation** (which restart of that membership). Nearly every event handler
re-validates this triple before acting, which is the core defense against
stale joins from displaced incarnations.

Exits are classified (`runtime/exit.rs`) into clean stop, failure, panic,
abort, or cancellation — a `CompletionFlag` per child distinguishes "finished
on its own" from "supervisor cancelled it". The restart decision then flows
through the child's `RestartPolicy` and the scope's `Strategy`:
`OneForOne` restarts one child; `OneForAll` drains *all* children and remints
the group cancellation token; `RestForOne` drains the declared suffix.
Restart intensity is a sliding window of restart timestamps
(`runtime/intensity.rs`); exceeding the budget is fatal to the scope and
escalates to the parent.

### Startup and shutdown ordering

Ordered scopes start children one at a time; a readiness-gated child
(`ChildReadiness::Manual` or `Automatic`) blocks the sequence until it reports
ready. Nested supervisors report ready recursively, once all *their* initial
children have started. Note that `tree.spawn()` itself is synchronous — it
launches the supervisor task and returns immediately; the readiness barrier
callers await is `scope().wait_started()`, which resolves once the whole
declared tree is up. A gated child that dies before readiness aborts the
whole startup.

Shutdown (`runtime/shutdown.rs`) is the reverse. Each child gets an
escalation ladder: cooperative cancellation (shutdown token) → grace expiry →
abort token (a short "tidy" window for wrappers) → hard Tokio abort. Ordered
scopes walk children in reverse declaration order, one at a time with full
grace each; dynamic scopes cancel the whole group at once and drain
concurrently. Three cancellation signals per child implement this:
`shutdown_token` (cooperative), `abort_token` (post-grace), and the Tokio
`AbortHandle` (hard kill). Aborting an ancestor arms a recursive hard-abort
cascade through nested scopes.

### Restart-stable identity and ownership

`StableSupervisorChannels` (`handle.rs`) is the crux: an identity that
outlives individual incarnations of a scope. It owns the snapshot watch, the
lifecycle event hub, and the control-channel binding; each incarnation binds
to it with a monotonically increasing epoch so late publications from a
displaced incarnation are rejected. This is what makes handles and observers
survive restarts.

Ownership is single and explicit: `RunningSupervisor` (wrapped as
`RunningTree`) is the sole owner of a spawned root, and dropping it requests
graceful shutdown. Every handle (`ScopeRef` etc.) is non-owning and freely
droppable. When an identity can never run again, its channels are
*terminalized*, which is what makes waiting observers resolve instead of hang.

## Layer 2: actor machinery (`src/actor/`)

### RawActor

`RawActor` (`actor/raw.rs`) is the minimal actor contract:

```rust
pub trait RawActor: Send + 'static {
    type Msg: Send + 'static;
    fn manual_readiness(&self) -> Option<Duration> { None }
    fn run(&mut self, ctx: &mut RawContext<Self::Msg>) -> impl Future<Output = ExitResult> + Send;
}
```

`run` owns the receive-loop control flow but borrows the incarnation-owned
`RawContext`, which provides `recv`/`try_recv`, `mark_ready`, and ambient
capabilities (offload, watch, blocking work, scope access). The borrow prevents
the context from escaping the incarnation and lets a raw-actor decorator
inspect it after an inner run or re-enter that actor on the same mailbox. Such
calls share one-shot readiness, stop state, timers, offloads, watches, and
identity. An actor instance is `Send` but not `Sync` or `Clone` — one
incarnation moves into one task. Durability across restarts lives in the
`ActorFactory` (`actor/factory.rs`), which is called once per incarnation to
build a fresh actor value; any closure `Fn() -> A` is a factory.

### Bindings: restart-stable refs

The central concept of this layer is the **binding** (`actor/binding.rs`).
`BindingCore<M>` is a long-lived, per-*membership* slot whose state is
`Unbound | Bound(mailbox) | Terminated`. An `ActorRef<M>` watches that slot,
so refs are restart-stable: `send` rides through restart windows (waiting
while `Unbound`) and only fails once the binding is `Terminated`. A
`BindingGuard` binds a fresh mailbox at incarnation start and unbinds on
drop, with run-id fencing so a late teardown from a cancelled incarnation
cannot clobber its replacement.

Mailboxes come in three kinds: `Mailbox::queue(capacity)` (bounded FIFO with
real backpressure), `Mailbox::latest()` (single conflating slot), and
`Mailbox::latest_by_key(...)`. There is no unbounded mailbox. Delivery is
at-most-once; restart and shutdown windows can drop messages, by design.

Send flavors on `ActorRef`: `send` (waits, restart-transparent), `try_send`
(fail-fast), `send_timeout`, and `call` (request/reply via a `Reply<T>`
embedded in the message, with acceptance and response timeouts distinguished).

### Lowering an actor into the engine

`actor/graph.rs` (note: historically named — it contains no graph, it is the
incarnation runner) holds the glue. `TypedRunner::start` creates the mailbox,
binds it, and *then* calls the factory — so a constructor panic follows the
same supervision path as a run panic. It owns the `RawContext`, lends it to
`RawActor::run`, and after the outermost run returns closes external intake and
reports any dropped continuations. It then destroys the context-owned offloads,
lifetime tasks, and monitor leases before actor state and before reporting the
exit, while the mailbox binding remains installed through actor destruction.
This also keeps the context available for explicit teardown after a
manual-readiness timeout. `RunnableActor` runs one incarnation, races
readiness/shutdown/abort, and
classifies the exit. Finally,
`actor_child_spec` in `runtime.rs` wraps a `RunnableActor` in a plain
`TaskSpec` — this is the seam where an actor becomes an ordinary supervised
task child. The supervisor finds actor metadata again later through generic
*attachments* (`supervisor/attachment.rs`), process-local `Any` values hung
off a child — the engine itself never learns what an actor is.

`ActorHost` (behind the `host` feature, same file) is the escape hatch that
runs incarnations directly without a supervisor.

### Monitors

`actor/monitor.rs` implements peer watching (Erlang-style `monitor`):
`ctx.watch(&ref)` delivers `MonitorEvent`s (started/exited/removed) into the
watcher's own mailbox. Delivery is via a bounded drop-oldest queue per watch;
overflow coalesces into a single `Lagged { dropped }` event, and the terminal
`Removed` is never dropped. Epoch counters ensure a retired membership's
watches cannot attach to its replacement.

## Layer 3: the `Actor` abstraction (`src/actor/handler.rs`)

`Actor` is the framework-loop flavor: `type Msg`, a required `handle`, and
optional `on_start` / `on_stop`. There is no per-message `Handler` trait —
one actor, one message enum, one `handle`; request/reply is a `Reply<T>`
field in a message variant.

The entire abstraction is one blanket impl, `impl<H: Actor> RawActor for H`,
whose `run` is the generated event loop:

1. `on_start`, then automatic readiness.
2. A prioritized loop over three event sources: mailbox/offload deliveries,
   actor-local **continuations** (`ctx.continue_with`), and keyed **timers**.
   Fairness is explicit: a continuation chain cannot starve external input,
   and already-queued messages get one bounded turn to retract an elapsed
   timer before it fires.
3. When the receive loop decides to stop, whether from supervisor shutdown or
   a local stop, close external intake to freeze the accepted prefix;
   optionally drain remaining messages (handlers see `ctx.draining()`), then
   run `on_stop`. An enclosing raw-actor decorator may re-enter the handler on
   the same context, but intake remains closed after that handler stop.

After the outermost raw run returns, `TypedRunner` closes intake for every raw
actor and reports continuations that the handler loop left queued, including
when the handler returned early with an error. Final incarnation cleanup
therefore does not depend on a hand-written raw actor remembering the blanket
loop's exit protocol.

`Context<A>` (what `handle` receives) deliberately exposes *less* than
`RawContext`: no `recv` (the loop owns the mailbox), no `mark_ready`, but
adds keyed timers, `continue_with`, and `stop`. `StopContext` narrows
further. Actors do not spawn children through their context; runtime
membership changes go through `ctx.scope()` → `DynamicScopeRef`.

## Layer 4: trees and scopes (`src/supervision.rs`, `src/runtime.rs`)

This is the public front door — the only way to construct a running system.

- `supervision.rs` is the **declaration** layer: `Tree` / `DynamicTree`
  collect `ActorSpec`s, `TaskSpec`s, and `SubtreeSpec`s plus scope-level
  defaults, then `spawn()` lowers everything into the private engine and
  returns a `RunningTree`.
- `runtime.rs` is the **actor-aware handle** layer (despite the name, it is
  not a scheduler): `RunningTree` (the owner; drop = graceful shutdown),
  `ScopeRef` (non-owning control + observation: shutdown requests, waits,
  snapshots, lifecycle subscriptions, recursive `actor_stats`),
  `DynamicScopeRef` (adds runtime membership mutation: `add_actor`,
  `add_task`, `spawn_once`, `add_subtree`, `remove`), and `TaskRef`.

The vocabulary here mirrors the ref/actor relationship one level up: a
`RunningTree` is to a tree what owning the actor is, and a `ScopeRef` is to a
scope what an `ActorRef` is to an actor — a cheap, restart-stable,
non-owning reference addressing a *membership*, not a particular run
(each run is an *incarnation*).

The prelude (`lib.rs`) is deliberately narrow (~17 items):
composition + actors + tasks. Raw-loop and hosting APIs live under
`kokage::raw`, observation view types under `kokage::observe`, and policies,
errors, and wiring helpers at the crate root.

## Observability

Three independent, restart-stable observation contracts, all rooted in the
engine and surfaced through `ScopeRef`:

1. **Snapshots** (`supervisor/snapshot.rs`) — a conflating `watch` of the
   recursive current state (`subscribe_snapshots`, `wait_for_child`).
2. **Lifecycle events** (`supervisor/lifecycle.rs`) — an ordered, bounded
   event stream; overflow is explicit (`Lagged { dropped }`), and event
   staging is aligned with snapshot publication so a reader woken by an
   event always sees a consistent-or-newer snapshot.
3. **Child observation** (`observe_children`) — a self-recovering reducer
   projection that resets with a full snapshot after lag.

Plus peer-level `watch`/`MonitorEvent` (layer 2) for actor-to-actor
monitoring, and `tracing` spans / optional `metrics` emitted from a single
choke point (`supervisor/observability.rs`).

## Satellite crates

- **`kokage-derive`** — one proc macro, `#[derive(ActorFactory)]`, which
  generates a `{Actor}Factory` struct: unmarked fields are cloned into each
  incarnation, `#[factory(default)]` fields are rebuilt fresh. Re-exported by
  `kokage` behind the `derive` feature.
- **`kokage-console`** (unpublished, experimental) — an axum/WebSocket
  dashboard that is a pure *consumer* of the public observability surface:
  `subscribe_snapshots()`, `subscribe_lifecycle()`, and polled
  `actor_stats()`, serialized via kokage's `serde` feature. Kokage itself has
  no knowledge of the console.

Feature flags on `kokage` (all default-off): `derive`, `host` (direct actor
hosting), `metrics`, `serde`.

## Source map

```
crates/kokage/src/
├── lib.rs             crate docs, module layout, prelude, re-exports
├── supervision.rs     Tree / DynamicTree declaration + lowering
├── runtime.rs         RunningTree, ScopeRef, DynamicScopeRef, TaskRef,
│                      actor_child_spec (the actor→task seam)
├── actor/
│   ├── raw.rs         RawActor trait, ExitResult
│   ├── handler.rs     Actor trait + the blanket RawActor impl (event loop)
│   ├── context.rs     ActorRef, Reply, RawContext / Context / StopContext
│   ├── binding.rs     BindingCore, mailboxes, ActorStats
│   ├── graph.rs       incarnation runner, RunnableActor, ActorHost
│   │                  (historical name; contains no graph)
│   ├── builder.rs     ActorSpec, ActorSlot, ActorOptions (declarations)
│   ├── factory.rs     ActorFactory
│   ├── monitor.rs     peer watching (MonitorEvent plumbing)
│   └── observability.rs  actor-level tracing events
└── supervisor/        the private engine
    ├── owner.rs       Supervisor, RunningSupervisor, spawn entry points
    ├── handle.rs      SupervisorHandle, StableSupervisorChannels
    ├── child.rs       ChildDefinition, ChildKind, TaskSpec
    ├── builder.rs     ordered/dynamic supervisor builders
    ├── strategy.rs    OneForOne / OneForAll / RestForOne
    ├── restart.rs     RestartPolicy, Backoff
    ├── shutdown.rs    Shutdown, MailboxShutdown policies
    ├── lifecycle.rs   LifecycleHub, LifecycleWatch, ChildObservationWatch
    ├── snapshot.rs    SupervisorSnapshot plumbing
    ├── scope.rs       ScopeKind, ScopePathSegment
    ├── guard.rs       Guard (generic cancel-on-drop handle)
    ├── cancellation.rs  CancellationToken
    ├── attachment.rs  AttachedChild (how the actor layer rides the engine)
    ├── observability.rs  tracing/metrics choke point
    └── runtime/       the per-scope state machine
        ├── supervision.rs  SupervisorRuntime select loop, restart dispatch
        ├── spawn.rs        child spawn plan, readiness racing
        ├── exit.rs         exit classification
        ├── shutdown.rs     drain / escalation ladders
        └── intensity.rs    restart budget tracking, backoff, jitter
```

## Invariants worth knowing before changing things

- **Single owner.** Exactly one `RunningTree` owns a running tree; drop means
  graceful shutdown. Everything else is a non-owning ref.
- **Refs address memberships, not incarnations.** `ActorRef::send` and scope
  observation ride through restarts; terminality is the only hard failure.
- **At-most-once delivery.** Restart and shutdown windows may drop messages.
  Nothing in the stack buffers across an incarnation boundary.
- **The readiness handshake is a cross-module protocol.** The blanket
  `Actor::run` must call `defer_automatic_readiness()` first (see comments in
  `actor/handler.rs` and `actor/graph.rs`); the ordered-startup gate depends
  on it.
- **One raw context is coextensive with one incarnation.** `TypedRunner` owns
  it and `RawActor::run` only borrows it. Decorators may re-enter on that same
  state, but cannot move it into work that outlives the run.
- **Exactly one exit report per incarnation.** Guard types in `actor/graph.rs`
  guarantee monitors see one exit even on panic or abort.
- **Public API is runtime-independent.** `just ci` runs
  `scripts/check-public-api.sh` to keep `tokio` types out of the public
  surface; keep it that way when adding APIs.
- **CI is defined in Nix.** `nix/crane-checks.nix` and `flake.nix` are
  authoritative; the `justfile` mirrors them and the two must be kept in
  sync.
