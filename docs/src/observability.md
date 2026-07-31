# Observability

A supervised system heals itself — which means things can go wrong *and get
fixed* without anyone noticing. That is only a virtue if, when you do look,
the system can tell you exactly what has been happening. Kokage exposes three
observation contracts:

1. `snapshot()` / `snapshots()` for conflated current state;
2. `changes()` for self-resynchronizing direct-child history, with
   `observe_children()` / `lifecycle_events()` as lower-level escape hatches;
3. `Context::watch()` for one actor's mailbox-ordered view of a peer.

Tracing, actor stats, metrics, and `kokage-console` project those contracts for
diagnostics and dashboards.

## Tracing: free structured logs

Supervisor, actor, and mailbox lifecycles are logged through the
[`tracing`](https://docs.rs/tracing) facade automatically. Install any
subscriber and the story starts flowing:

```rust,ignore
tracing_subscriber::fmt().init();
```

You get `supervisor`, `child`, and `actor` spans carrying names, scope paths
(`root.press-room.press-a`), and generations; INFO events for starts and
stops; WARN events for failures, scheduled restarts (with the delay), and
exceeded restart intensity — each with a `status` that distinguishes
`failed`, `panicked`, `cancelled`, and `shutdown_timed_out`. Messages
themselves are traced at TRACE level (sends, receipts, and rejections with
their reason), which makes a mailbox mystery a filter expression away. Your
payloads are never formatted into logs.

## Snapshots: what is true right now

Any [`ScopeRef`] can answer "what does the tree look like?" —
[`snapshot`] for one point in time, [`snapshots`] for a
conflating, never-lagging feed of changes:

```rust
use kokage::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    tree.add_task("ingest", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    tree.add_task("serve", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let running_tree = tree.spawn()?;

    // Readiness: wait until every child is running.
    let mut snapshots = running_tree.snapshots();
    let ready = snapshots
        .wait_for(|s| s.children.iter().all(|c| c.state.is_running()))
        .await?;
    println!(
        "ready: {} children, {} restarts so far",
        ready.children.len(),
        ready.total_restarts
    );

    running_tree.shutdown().await?;
    Ok(())
}
```

A [`SupervisorSnapshot`] carries the scope's state, strategy, and cumulative
restart count, plus one [`ChildSnapshot`] per child: its `state` (with the
previous exit and its failure message, if any), `generation`,
`restart_count`, `next_restart_in` for a child waiting out a backoff delay,
and — for subtree children — a nested snapshot, recursively. This is the
raw material for health endpoints: liveness is "the tree is running",
readiness is a predicate over children, and "why is it broken" is
`state.last_exit()`. Exit details use [`ExitStatus`], the same completed,
failed, panicked, or aborted vocabulary used by lifecycle and peer-monitor
events.

Two receiver habits keep you honest: the feed *conflates* (you always see
the latest truth, never a backlog), so predicates passed to `wait_for` /
`wait_for_child` must be **monotonic** — write `generation > baseline`, not
`generation == baseline + 1`, because intermediate states may be skipped.
You saw this pattern in [Let It Crash](let-it-crash.md).

## State and events without manual resynchronization

The common stateful lifecycle entry point is [`changes`]. Every stream starts
with [`ScopeChange`]`::Reset(snapshot)`, followed by ordered direct-child events
paired with the current authoritative snapshot.
If the consumer falls behind, Kokage registers a fresh gap-free subscription
and yields another reset instead of exposing bookkeeping as application logic:

```rust,ignore
let mut changes = running_tree.scope().changes();
while let Some(change) = changes.next().await {
    match change {
        ScopeChange::Reset(snapshot) => state = snapshot,
        ScopeChange::Event { snapshot, .. } => state = snapshot,
    }
}
```

The lower-level [`observe_children`] method returns the aligned snapshot and
event stream separately when a consumer needs its own reducer or lag policy.
Initialize from its snapshot, then apply events whose child sequence is greater
than `snapshot.lifecycle_seq`.

## The recursive lifecycle stream: what happened, in order

Snapshots tell you *now*; the lifecycle stream tells you *the story*.
[`lifecycle_events`] on any `ScopeRef` yields every transition in the scope —
recursively for the whole subtree by default, or `.direct_children()` for
one level:

```rust
use kokage::{observe::LifecycleEventKind, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    tree.add_task("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    });
    let running_tree = tree.spawn()?;
    let scope = running_tree.scope();
    let mut events = scope.lifecycle_events();

    scope.request_shutdown();
    while let Some(event) = events.next().await {
        println!("{:?} at {:?}", event.kind, event.scope_path);
        if matches!(event.kind, LifecycleEventKind::SupervisorStopped) {
            break;
        }
    }
    running_tree.wait().await?;
    Ok(())
}
```

[`LifecycleEventKind`] carries supervisor transitions and a single `Child`
variant. Its [`ChildEvent`] envelope holds the child's id, `lineage` (which
membership), sequence number, and cumulative restart counters once; match its
[`ChildEventKind`] for additions, starts, exits, scheduled restarts, and
removals. The enums are `#[non_exhaustive]`; always match with a catch-all arm.

Delivery is buffered, not conflated. A lower-level consumer that falls far
behind gets the oldest details collapsed into one `Lagged { dropped }` marker
— the stream never lies by omission. Prefer `changes()` when automatic
direct-child realignment is sufficient. Recursive streams have per-scope
sequence spaces, so a custom recursive reducer must resynchronize each
affected scope from its snapshot. To feed events into an actor instead of a
loop, choose the desired stream and call [`LifecycleWatch::forward_to`]. It
maps events into any `ActorRef` and returns a `Guard`.

Note how this differs from a peer [`MonitorEvent`] watch
([Watching Peers](watching-peers.md)): monitors give one actor a typed,
mailbox-ordered view of one collaborator; the lifecycle stream gives
operators the whole tree with paths and counters. Same underlying model, a
projection per audience.

With the `serde` feature, snapshot, lifecycle, and scope-change representations
have checked wire fixtures, but remain deliberately unversioned during `0.x`.
Treat them as a same-Kokage-version adapter boundary: mixed-version producers
and consumers, or long-lived persisted event logs, need an application-owned
versioned envelope and migration policy.

## Message counters

[`ActorRef::stats`] returns local [`ActorStats`]: messages received, accepted,
and conflated, sends rejected, outstanding offloads, and current mailbox
depth against capacity. [`ScopeRef::actor_stats`] returns
[`ScopedActorStats`] values that add scope path and lineage for every actor in
the subtree. Counters accumulate across restarts; a restarting actor doesn't
reset your dashboards.

Byte-level accounting is opt-in per actor — give the spec a size estimator
(a plain `fn` pointer) and `message_bytes_accepted` starts counting:

```rust
# use kokage::prelude::*;
# struct Press;
# impl Actor for Press {
#     type Msg = String;
#     async fn handle(&mut self, _j: String, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
let spec = ActorSpec::new("press", || Press).message_size(|job: &String| job.len());
# let _ = spec;
```

## Metrics

With the `metrics` cargo feature, supervisors additionally emit through the
[`metrics`](https://docs.rs/metrics) facade — counters and gauges like
`supervisor.children.running`, `supervisor.children.exited` (labeled by
child and status), `supervisor.restarts`,
`supervisor.restart_intensity_exceeded`, and shutdown-duration histograms;
actors with a `message_size` hint add `actor.message.bytes_accepted` and an
`actor.message.size` histogram. Install whatever recorder your stack uses:

```rust,ignore
let recorder = PrometheusBuilder::new().install_recorder()?;
// ... run ...
println!("{}", recorder.render());
```

Actor-level dashboards need no feature flag at all: the intended pattern is
a small sampler — a supervised task, naturally — that periodically reads
`actor_stats()` and a snapshot, and exports in whatever format you prefer.
The repository's `actor_metrics.rs` example does exactly that.

For a live view during development, the experimental `kokage-console` crate
serves a web dashboard over a running tree
(`cargo run -p kokage-console --example console`).

[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`snapshot`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.snapshot
[`snapshots`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.snapshots
[`SupervisorSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisorSnapshot.html
[`ChildSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.ChildSnapshot.html
[`ExitStatus`]: https://stokes.io/kokage/api/kokage/observe/enum.ExitStatus.html
[`observe_children`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.observe_children
[`changes`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.changes
[`ScopeChange`]: https://stokes.io/kokage/api/kokage/enum.ScopeChange.html
[`lifecycle_events`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.lifecycle_events
[`LifecycleEventKind`]: https://stokes.io/kokage/api/kokage/observe/enum.LifecycleEventKind.html
[`ChildEvent`]: https://stokes.io/kokage/api/kokage/observe/struct.ChildEvent.html
[`ChildEventKind`]: https://stokes.io/kokage/api/kokage/observe/enum.ChildEventKind.html
[`LifecycleWatch::forward_to`]: https://stokes.io/kokage/api/kokage/observe/struct.LifecycleWatch.html#method.forward_to
[`MonitorEvent`]: https://stokes.io/kokage/api/kokage/struct.MonitorEvent.html
[`ActorRef::stats`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.stats
[`ScopeRef::actor_stats`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.actor_stats
[`ActorStats`]: https://stokes.io/kokage/api/kokage/observe/struct.ActorStats.html
[`ScopedActorStats`]: https://stokes.io/kokage/api/kokage/observe/struct.ScopedActorStats.html
