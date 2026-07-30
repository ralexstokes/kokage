# Observability

The actor product collects its public snapshot, lifecycle, outline, completion,
and actor-stat types under `kokage::observe`.

The supervisor layer has two observation primitives:

1. `snapshot()` / `subscribe_snapshots()` for current state
2. `watch_lifecycle()` for ordered, path-carrying tree transitions

`tracing`, pull-based actor stats, optional metrics, and `kokage-console`
are projections for diagnostics and dashboards.

## Snapshots: Current State

`ScopeRef::snapshot()` returns the current tree state, and
`subscribe_snapshots()` returns a crate-owned `SupervisorSnapshotReceiver`
that updates when it changes. Use `latest()` for an unobserved read,
`take_latest()` to mark the current version observed, and `changed()` or
`wait_for(...)` for asynchronous observation. The receiver conflates
intermediate snapshots but never lags.
Snapshots carry cumulative counters — per-child
`observe::ChildSnapshot::restart_count` and supervisor-level
`observe::SupervisorSnapshot::total_restarts` — so counter deltas account for every
restart even when updates are conflated.

Every `ChildSnapshot` also carries a `lineage`. A restart increments
`generation` but retains the lineage; removing a child and adding a new child
under the same id assigns a later lineage even though the replacement starts at
generation zero. Treat `(id, lineage)` as the identity of a direct child
membership. Lineages start at zero, include statically configured children in
declaration order, and are monotonic across every incarnation of one
restart-stable supervisor identity. Each nested supervisor identity allocates
its own local sequence; lineages are not global. In a recursive view, identify
a child by the full supervisor path together with its local `(id, lineage)`.
Each path segment includes the containing supervisor's id, parent-assigned
lineage, and generation.

Removing a subtree and inserting another under the same id creates a new
membership in the parent and a new stable supervisor identity. The new
subtree's local lineage sequence may therefore begin at zero even if its
predecessor used the same local lineages; the parent path distinguishes the
two. The `u64` counter saturates at its maximum rather than changing supervisor
control semantics in the practically unreachable overflow case. For
dynamically added task children, `ScopeRef::add_task` returns the same
lineage that the runtime assigned while inserting the child.
Consumers that need to associate their own state with that exact membership
should retain the returned value rather than performing a later id-based
snapshot lookup.

Readiness and exit details live inside `ChildSnapshot::state` rather than in
parallel booleans and optional fields. `ChildStateView` exposes only details
that are meaningful in that phase: starting/running states can retain the
preceding generation's exit, stopping records whether readiness was reached,
`Stopped` carries the current generation's optional `ChildExitView`, and the
sibling `StartupAborted` variant carries the exit of a generation that ended
permanently before readiness. `ChildExitView` is also the shape used by exit
lifecycle events and records whether the supervisor cancelled the generation.

## Lifecycle Streams: Ordered Transitions

`watch_lifecycle()` observes the watched supervisor and every nested scope.
Call `.direct_children()` on the returned watch to apply a local-depth filter.
The stream reports `ChildAdded`, `ChildStarted`, `ChildExited`,
`ChildRemoved`, and `ChildRestartScheduled`. A `OneForOne` restart of
the child that failed is visible as the ordered sequence `Exited`,
`RestartScheduled`, then `Started`; group strategies can restart siblings
without a per-sibling schedule event. Restart-intensity failure and supervisor
start/stop transitions use the same event vocabulary.
Count restarts from the cumulative counters carried by events, not by inferring
event pairs. Readiness-gated children emit `Started` only after `on_start`
succeeds.

Each child variant of `LifecycleEventKind` carries a monotonic `seq`, child id
and lineage, `total_restarts`, and the child's `child_restart_count`.
`ChildRestartScheduled` is part of the same vocabulary and sequence. A nested supervisor's sequence and total counter continue across its
own incarnations, including recreation by an ancestor. `next()` returns `None`
only after staged events are drained and the stable supervisor identity can
never run again.

### Gap-free snapshot alignment

Register the watch **before** reading the snapshot, then discard events already
represented by that snapshot:

```rust,ignore
let mut lifecycle = handle.watch_lifecycle();
let snapshot = handle.snapshot();

while let Some(event) = lifecycle.next().await {
    if event.supervisor_path.is_empty()
        && event.seq().is_some_and(|seq| seq <= snapshot.lifecycle_seq)
    {
        continue;
    }
    apply(event);
}
```

The supervisor assigns the event sequence, publishes the aligned snapshot,
and stages the event as one ordered transition. The recipe therefore misses
no transition with `seq > snapshot.lifecycle_seq` and does not reapply an
event with `seq <= snapshot.lifecycle_seq`.

`LifecycleEventKind::Lagged` has an empty path and applies to the whole watched
tree. The gap still requires a fresh snapshot before processing later edges.

A stable nested `ScopeRef` can be watched before that scope first spawns. Its
initial snapshot already projects statically configured children as `Starting`,
while the first later `Added` event records installation of that membership
into the running supervisor incarnation and `Started` records readiness. An
edge reducer seeded from the snapshot must therefore treat `Added` as an
idempotent upsert/activation keyed by `(id, lineage)`, rather than an
unchecked row insertion. This preserves the watch-before-spawn guarantee
without pretending configured state and runtime installation are different
child identities.

Every watch has a bounded 128-event buffer. Sustained overload drops the
oldest details and collapses the loss into one `Lagged { dropped }` marker at
the front. Consumers that derive state from edges must fetch a fresh snapshot
and realign. Stream closure is terminality, not an event, and is never dropped.

### Waiting for child state

Subscribe before triggering a transition, capture any baseline fields you
care about, then use `wait_for_child`:

```rust,ignore
let baseline = handle
    .snapshot()
    .child("press")
    .expect("the declared press actor is present")
    .generation;
let mut snapshots = handle.subscribe_snapshots();
press.send(PrintMsg::Jam).await?;
let restarted = snapshots
    .wait_for_child("press", |child| {
        child.generation > baseline && child.state.is_running()
    })
    .await?;
```

The receiver is created before the trigger, so a fast replacement cannot be
missed. `wait_for_child` delegates to the conflating snapshot `wait_for`
primitive and returns the matching `ChildSnapshot`. Because intermediate
snapshots may be skipped, express progress monotonically (`generation >
baseline`), not as an exact intermediate generation. Use the lifecycle stream
when every exit or generation edge matters; wait on the full snapshot when the
condition is that a membership is absent.

### Pumping transitions into an actor

Route library transitions and application-semantic signals into one observer
actor when its mailbox should be the linearization point:

```rust,ignore
let lifecycle_guard = handle
    .subtree("venues")
    .unwrap()
    .watch_lifecycle_to(&breaker, |event| {
        HealthMsg::Lifecycle(event)
    });
```

The pump follows ordinary target actor restarts and applies the target's usual
mailbox policy. It deliberately **does not replay** discrete events to a fresh
target incarnation: replay would fabricate history. A restarted consumer
rehydrates in `on_start` with the watch-then-snapshot alignment recipe. Use a
FIFO mailbox when every transition matters; a conflating mailbox may replace
intermediate lifecycle messages even though the watch buffer itself reports
lag explicitly. As with every actor send, acceptance is not acknowledgement
that the handler processed the message.

Keep the returned `Guard` alive. Dropping or cancelling it stops
the pump, as does permanent target termination. Watched-scope terminality
closes the pump after its staged events have drained. If the live target's
mailbox remains full, cancellation or target termination is the escape hatch.

### Tree watching and depth filtering

`watch_lifecycle()` yields one ordered stream for the watched scope and every
nested supervisor. Each event carries a `supervisor_path` relative
to the watched handle. Every path segment includes the nested supervisor's id,
lineage, and generation, so consumers can distinguish both a restarted
incarnation and a removed-then-reinserted subtree.

`LifecycleEvent` is an envelope containing the supervisor path and a flat
`LifecycleEventKind`. Child variants retain the emitting scope's sequence and
cumulative restart counters. A nested scope's
stable identity is reattached automatically when an ancestor recreates it; the
path then reflects the new ancestor generation.

```rust,ignore
let mut tree = handle.watch_lifecycle();

while let Some(event) = tree.next().await {
    render(event);
}
```

Each watch has one bounded buffer for the whole watched tree. On
overflow, the oldest details collapse into an in-band, tree-wide
`LifecycleEventKind::Lagged { dropped }` marker with an empty
`supervisor_path`. Consumers maintaining derived tree state must read a fresh
recursive snapshot and realign the whole tree.
Stream closure means that the watched stable identity is terminal, after all
staged events have drained.

Use `handle.watch_lifecycle().direct_children()` when per-scope sequence
alignment is the goal. Keep the default tree stream for diagnostics,
dashboards, and any observer that needs a single feed. The
`trading_engine` example's breaker consumes the child event's `total_restarts`;
that counter records scheduled restarts — the
same occurrences as the restart-intensity window, including clean exits
restarted under `Restart::always()`. Group-strategy sibling respawns do not
increment it.

## Tracing And Stats

The actor layer emits actor, mailbox, and message tracing events.
Message events include `source_actor_id` when the sender is another actor;
external sends through an `ActorRef` have no source actor.

Actor spans are nested under the supervisor child span, so `child_path` and
`supervisor_path` provide their scope identity without a separate graph name.
Actor start and exit events include `running_actors`; this is the number of
actors currently running in that immediate scope, not an application-wide
total. Use recursive runtime stats or snapshots for a whole-tree view.

Every `ActorRef` exposes cumulative message counters and current mailbox usage:

```rust,ignore
let stats = worker.stats();
println!("received={} queued={}/{}",
    stats.messages_received, stats.mailbox_depth, stats.mailbox_capacity);
```

Applications that need time-series export periodically sample these values and
the supervisor snapshot — a ~10-line task you own, not a framework pipeline.
The `kokage` `actor_metrics` example prints the result in
Prometheus-shaped text without an actor-layer metrics backend.

Message sizes are application-defined and fully opt-in. Pass a sizing function
to the actor's `ActorSpec`:

```rust,ignore
fn upload_size(message: &Upload) -> usize {
    message.payload.len()
}

let uploads = ActorSpec::new("uploads", UploadActor::new).message_size(upload_size);
let uploads_ref = uploads.actor_ref();
let tree = OrderedTree::new().actor(uploads);
```

The sizing observer is applied when the declaration is materialized, so refs
minted before `message_size` is configured report the same accepted-byte total.

The same declaration configures a dynamic actor before insertion:
`ActorSpec::new("uploads", UploadActor::new).mailbox(MailboxMode::conflate()).message_size(upload_size)`.
Pass it to `ScopeRef::add_actor`; use `actor_ref()` first when callers
need its typed handle.

`ScopeRef::actor_stats()` walks runtime subtrees recursively. A reference
returned by `ScopeRef::subtree` provides the same view scoped to that
subtree, including actors added dynamically through that reference.
These runtime-scoped samples set `observe::ActorStats::lineage` from the
membership identity retained when the actor was registered. They also carry
`observe::ActorStats::supervisor_path`: each containing nested supervisor is identified
by id, lineage, and generation. Use the full supervisor path together
with `(actor_id, lineage)` to join a flattened recursive sample to the
exact current tree node; local lineages can repeat in sibling subtrees. A direct
child has an empty path. Stats sampled directly from an `ActorRef` report
`None` for both runtime-scoped identity fields because a ref has no supervisor
context.

`observe::ActorStats::outstanding_offloads` is a point-in-time gauge of bounded futures
owned by the current actor incarnation. It rises when `ctx.offload`
starts work and falls when the actor loop reaps its completion or observes its
abort, making actors with in-flight requests visible without inspecting
anonymous Tokio tasks.

`observe::ActorStats::outstanding_scope_waits` is the corresponding
point-in-time gauge for lifecycle waits started with
`Context::spawn_scope_wait`. It returns to zero when the actor loop reaps a
result, an explicit `Guard::cancel` is observed, or the incarnation
ends. This makes message-driven code that accumulates never-ending lifecycle
waits visible.

`observe::ActorStats::message_bytes_accepted` is then `Some(total)`; ordinary actors
report `None` and do not sample message sizes. With the `metrics` feature,
each accepted sized message also updates the `actor.message.size` histogram
and `actor.message.bytes_accepted` counter. Metric handles and actor-id labels
are registered lazily on the first accepted message and cached per actor; later
accepted sends only sample `size_hint` and update those handles. Because the
byte total follows `messages_accepted`, a conflated message that is accepted
and later replaced still contributes its size even though it is never received.
Since the cached handles bind to whichever recorder is installed when the first
message is accepted, install your metrics recorder at startup, before actors
begin receiving messages. The feature
continues to enable the supervisor lifecycle counters, gauges, and histograms
as well.

### Supervisor metrics

The `metrics` feature emits the following supervisor instruments:

| Metric | Kind | Meaning |
|--------|------|---------|
| `supervisor.children.running` | gauge | Current running children in a scope. |
| `supervisor.children.started` | counter | Child generations that reached running. |
| `supervisor.children.exited` | counter | Child generations that exited. |
| `supervisor.restarts` | counter | Child restarts performed by the supervisor. |
| `supervisor.restart_intensity_exceeded` | counter | Scope failures caused by an exhausted restart budget. |
| `supervisor.shutdown_timeouts` | counter | Cooperative child shutdowns that exceeded their grace period. |
| `supervisor.child_shutdown.duration` | histogram | Seconds spent draining a child or scope operation. |

Every instrument carries `supervisor`, `path`, and `strategy` labels. Child
transition instruments also carry `child_id`; the exit counter adds `status`.
Shutdown instruments add `operation` (`shutdown`, `remove_child`,
`group_restart`, or `rest_for_one_restart`) and include `child_id` when the
sample refers to one child. `path` is `root` for the root scope and uses
dot-separated child ids below it. These labels are intended for diagnostics;
avoid copying unbounded application data into child ids.

## Web Console

The separate `kokage-console` workspace crate can launch a web console
backed by the runtime's public snapshots, events, and actor stats:

```rust,ignore
let runtime = tree.spawn()?;
let console = kokage_console::ConsoleBuilder::for_runtime(&runtime)
    .bind(([127, 0, 0, 1], 8080))
    .spawn()
    .await?;

println!("console at http://{}", console.local_addr());
```

The console's `actor_stats` WebSocket frames serialize
`observe::ActorStats` directly. Enabling Kokage's `serde` feature gives
`ActorStats` and `SupervisorPathSegment` `Serialize` and `Deserialize`
implementations, so other observers can use the same protocol types without a
console-specific mirror.

Run `cargo run -p kokage-console --example console` to try it from the
workspace checkout. The console is experimental, git-only tooling and is not
a `kokage` feature or dependency.
The default loopback bind remains token-free for convenient local development,
but every request is restricted to the listener address (or `localhost`) and
WebSocket browser origins must match the request host.

Non-loopback binds require an access token. Add the externally visible host
when it differs from the listener address:

```rust,ignore
let console = kokage_console::ConsoleBuilder::for_runtime(&runtime)
    .bind(([0, 0, 0, 0], 8080))
    .access_token("replace-with-a-random-url-safe-token")
    .allowed_host("console.internal:8080")
    .spawn()
    .await?;
```

API clients can send `Authorization: Bearer TOKEN`. To use the dashboard in a
browser, open `http://console.internal:8080/?token=TOKEN` once; the console
redirects to remove the token from the URL and uses an HTTP-only, same-site
cookie afterward. Treat the console as sensitive operational access: snapshots
and events include child identifiers and may include application error strings.

Host checks also apply through an SSH tunnel. Forward the same port and allow
the browser-visible authority—for example, `ssh -L 8080:host:8080 host` with
`.allowed_host("localhost:8080")`. A different local forwarding port must be
listed instead. For non-local deployments, terminate TLS at a trusted reverse
proxy so the token and console data are encrypted in transit.
