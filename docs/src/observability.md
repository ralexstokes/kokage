# Observability

The supervisor layer has two observation primitives:

1. `snapshot()` / `subscribe_snapshots()` for current state
2. `watch_lifecycle()` / `watch_lifecycle_recursive()` for ordered transitions

`tracing`, pull-based actor stats, optional metrics, and `tokio-otp-console`
are projections for diagnostics and dashboards.

## Snapshots: Current State

`RuntimeHandle::snapshot()` returns the current tree state, and
`subscribe_snapshots()` returns a `watch::Receiver` that updates when it
changes. The watch channel conflates intermediate snapshots but never lags.
Snapshots carry cumulative counters — per-child
`ChildSnapshot::restart_count` and supervisor-level
`SupervisorSnapshot::total_restarts` — so counter deltas account for every
restart even when updates are conflated.

Every `ChildSnapshot` also carries a `membership_epoch`. A restart increments
`generation` but retains the membership epoch; removing a child and adding a
new child under the same id assigns a later epoch even though the replacement
starts at generation zero. Treat `(id, membership_epoch)` as the identity of a
direct child membership. Epochs start at zero, include statically configured
children in declaration order, and are monotonic only within one supervisor
incarnation. Nested supervisors allocate their own sequences, so identify a
nested child by its snapshot path, including each parent's membership epoch
and generation. The `u64` counter saturates at its maximum rather than changing
supervisor control semantics in the practically unreachable overflow case.
For dynamically added task children, `SupervisorHandle::add_child` returns the
same epoch that the supervisor assigned while inserting the child. Consumers
that need to associate their own state with that exact membership should retain
the returned value rather than performing a later id-based snapshot lookup.

## Lifecycle Streams: Ordered Transitions

`watch_lifecycle()` observes `Added`, `Started`, `Exited`, and `Removed`
transitions among the watched supervisor's direct children. Readiness-gated
children emit `Started` only after `on_start` succeeds. A restart is the
ordered pair `Exited` then `Started` for the same membership; count restarts
from the event envelope's cumulative counters, not by inferring event pairs.

Each event carries a monotonic `seq`, child id and membership epoch,
`total_restarts`, and the child's `child_restart_count`. A nested supervisor's
sequence and total counter continue across its own incarnations, including
recreation by an ancestor. `next()` returns `None` only after staged events
are drained and the stable supervisor identity can never run again.

### Gap-free snapshot alignment

Register the watch **before** reading the snapshot, then discard events already
represented by that snapshot:

```rust,ignore
let mut lifecycle = handle.watch_lifecycle();
let snapshot = handle.snapshot();

while let Some(event) = lifecycle.next().await {
    if event.seq <= snapshot.lifecycle_seq {
        continue;
    }
    apply(event);
}
```

The supervisor assigns the event sequence, publishes the aligned snapshot,
and stages the event as one ordered transition. The recipe therefore misses
no transition with `seq > snapshot.lifecycle_seq` and does not reapply an
event with `seq <= snapshot.lifecycle_seq`.

A stable nested handle can be watched before that scope first spawns. Its
initial snapshot already projects statically configured children as `Starting`,
while the first later `Added` event records installation of that membership
into the running supervisor incarnation and `Started` records readiness. An
edge reducer seeded from the snapshot must therefore treat `Added` as an
idempotent upsert/activation keyed by `(id, membership_epoch)`, rather than an
unchecked row insertion. This preserves the watch-before-spawn guarantee
without pretending configured state and runtime installation are different
child identities.

Every watch has a bounded 128-event buffer. Sustained overload drops the
oldest details and collapses the loss into one `Lagged { dropped }` marker at
the front. Consumers that derive state from edges must fetch a fresh snapshot
and realign. The marker carries the newest dropped transition's sequence and
cumulative counters, so filtering it against a snapshot is sound and restart
counts resynchronize at the marker. Stream closure is terminality, not an
event, and is never dropped.

Note that a marker's `child_id` and `membership_epoch` describe the newest
discarded transition, so a marker can be stamped with one child while standing
for another child's loss. Filter on `seq`, not on the marker's identity fields.

### Waiting for one restart

`LifecycleWatch::wait_started(id, after_generation)` collapses the common
one-shot wait into a single call, returning the generation that started:

```rust,ignore
let mut lifecycle = handle.watch_lifecycle();
let baseline = handle.snapshot().child("press").unwrap().generation;

lifecycle.wait_started("press", baseline).await;
```

It returns `None` once that start can no longer be observed on this watch —
the membership was removed, the scope became terminal, or a `Lagged` marker
discarded a prefix that may have carried it. `None` means waiting longer is
futile, not which of the three happened; realign from a snapshot to tell them
apart. Consumers maintaining derived state should drive `next()` directly.

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

Keep the returned `LifecycleWatchGuard` alive. Dropping or cancelling it stops
the pump, as does permanent target termination. Watched-scope terminality
closes the pump after its staged events have drained. If the live target's
mailbox remains full, cancellation or target termination is the escape hatch.

### Recursive tree watching

`watch_lifecycle_recursive()` yields one ordered stream for the watched scope
and every nested supervisor. Each event carries a `supervisor_path` relative
to the watched handle. Every path segment includes the nested supervisor's id,
membership epoch, and generation, so consumers can distinguish both a restarted
incarnation and a removed-then-reinserted subtree.

The recursive event kind includes supervisor `Started`, `Stopping`, and
`Stopped` transitions, direct-child lifecycle events, restart scheduling with
its backoff delay, and restart-intensity failure. Direct-child events retain
their emitting scope's sequence and cumulative restart counters. A nested
scope's stable identity is reattached automatically when an ancestor recreates
it; the path then reflects the new ancestor generation.

```rust,ignore
let mut tree = handle.watch_lifecycle_recursive();

while let Some(event) = tree.next().await {
    render(event.supervisor_path, event.kind);
}
```

Each recursive watch has one bounded buffer for the whole watched tree. On
overflow, the oldest details collapse into an in-band, tree-wide
`Lagged { dropped, newest_dropped }` marker. The surrounding event retains the
newest discarded transition's supervisor path, while `newest_dropped` retains
its kind and therefore any per-scope sequence and cumulative counters. This
can cheaply re-anchor those counters, but consumers maintaining derived tree
state must still read a fresh recursive snapshot and realign the whole tree.
Stream closure means that the watched stable identity is terminal, after all
staged events have drained.

Use a direct watch when per-scope sequence alignment is the goal. Use a
recursive watch for diagnostics, dashboards, and any observer that needs a
single tree feed. Both watch types provide a `wait_started` helper; the
recursive form additionally takes the exact supervisor path. The
`trading_engine` example's breaker consumes
direct `event.total_restarts`; that counter records scheduled restarts — the
same occurrences as the restart-intensity window, including clean exits
restarted under `RestartPolicy::Always`. Group-strategy sibling respawns do not
increment it.

## Tracing And Stats

The actor layer emits graph, actor, mailbox, and message tracing events.
Message events include `source_actor_id` when the sender is another actor;
external sends through an `ActorRef` have no source actor.

Every `ActorRef` exposes cumulative message counters and current mailbox usage:

```rust,ignore
let stats = worker.stats();
println!("received={} queued={}/{}",
    stats.messages_received, stats.mailbox_depth, stats.mailbox_capacity);
```

Applications that need time-series export periodically sample these values and
the supervisor snapshot — a ~10-line task you own, not a framework pipeline.
The `tokio-otp` `actor_metrics` example prints the result in
Prometheus-shaped text without an actor-layer metrics backend.

Message sizes are application-defined and fully opt-in. Implement
`MessageSize` for a message type and enable it in the actor's `ActorOptions`:

```rust,ignore
impl MessageSize for Upload {
    fn size_hint(&self) -> usize {
        self.payload.len()
    }
}

let uploads = graph.actor_with_options(
    "uploads",
    UploadActor::new,
    ActorOptions::new().message_size(),
);
```

The same `ActorOptions` value works with `GraphBuilder::slot_with_options`,
`GraphBuilder::add_with_options`, and
`RunnableActorBuilder::actor_with_options`. Dynamic registration exposes the
same settings directly on `DynamicActorOptions`, so mailbox and size settings
can be combined with
`DynamicActorOptions::new().mailbox(MailboxMode::Conflate).message_size()`
and passed to `RuntimeHandle::add_actor`.

`RuntimeHandle::actor_stats()` walks runtime subtrees recursively. A handle
returned by `RuntimeHandle::subtree` provides the same view scoped to that
subtree, including actors added dynamically through the scoped handle.
These runtime-scoped samples set `ActorStats::membership_epoch` from the
membership identity retained when the actor was registered. They also carry
`ActorStats::supervisor_path`: each containing nested supervisor is identified
by id, membership epoch, and generation. Use the full supervisor path together
with `(actor_id, membership_epoch)` to join a flattened recursive sample to the
exact current tree node; local epochs can repeat in sibling subtrees. A direct
child has an empty path. Stats sampled directly from an `ActorRef`,
`RunnableActor`, or standalone `Graph` report `None` for both runtime-scoped
identity fields because those surfaces have no supervisor context.

`ActorStats::outstanding_offloads` is a point-in-time gauge of bounded futures
owned by the current actor incarnation. It rises when `ActorContext::offload`
starts work and returns to zero on completion, timeout, or abort, making actors
with in-flight requests visible without inspecting anonymous Tokio tasks.

`ActorStats::message_bytes_accepted` is then `Some(total)`; ordinary actors
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

## Web Console

The separate `tokio-otp-console` workspace crate can launch a web console
backed by the runtime's public snapshots, events, and actor stats:

```rust,ignore
let handle = runtime.spawn();
let console = tokio_otp_console::Console::for_runtime(&handle)
    .bind(([127, 0, 0, 1], 8080))
    .build()?
    .spawn()
    .await?;

println!("console at http://{}", console.local_addr());
```

Run `cargo run -p tokio-otp-console --example console` to try it from the
workspace checkout. The console is experimental, git-only tooling and is not
a `tokio-otp` feature or dependency.
The default loopback bind remains token-free for convenient local development,
but every request is restricted to the listener address (or `localhost`) and
WebSocket browser origins must match the request host.

Non-loopback binds require an access token. Add the externally visible host
when it differs from the listener address:

```rust,ignore
let console = tokio_otp_console::Console::for_runtime(&handle)
    .bind(([0, 0, 0, 0], 8080))
    .access_token("replace-with-a-random-url-safe-token")
    .allowed_host("console.internal:8080")
    .build()?
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
