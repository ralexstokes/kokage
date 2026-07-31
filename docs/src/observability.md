# Observability

A supervised system heals itself — which means things can go wrong *and get
fixed* without anyone noticing. That is only a virtue if, when you do look,
the system can tell you exactly what has been happening. Kokage exposes four
complementary views: structured logs, point-in-time snapshots, an ordered
lifecycle event stream, and message-level counters.

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
[`snapshot`] for one point in time, [`subscribe_snapshots`] for a
conflating, never-lagging feed of changes:

```rust
use kokage::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("ingest", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    tree.add_task(TaskSpec::new("serve", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let runtime = tree.spawn()?;
    let scope = runtime.scope();

    // Readiness: wait until every child is running.
    let mut snapshots = scope.subscribe_snapshots();
    let ready = snapshots
        .wait_for(|s| s.children.iter().all(|c| c.state.is_running()))
        .await?;
    println!(
        "ready: {} children, {} restarts so far",
        ready.children.len(),
        ready.total_restarts
    );

    runtime.shutdown_and_wait().await?;
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
`state.last_exit()`.

Two receiver habits keep you honest: the feed *conflates* (you always see
the latest truth, never a backlog), so predicates passed to `wait_for` /
`wait_for_child` must be **monotonic** — write `generation > baseline`, not
`generation == baseline + 1`, because intermediate states may be skipped.
You saw this pattern in [Let It Crash](let-it-crash.md).

## The lifecycle stream: what happened, in order

Snapshots tell you *now*; the lifecycle stream tells you *the story*.
[`watch_lifecycle`] on any `ScopeRef` yields every transition in the scope —
recursively for the whole subtree by default, or `.direct_children()` for
one level:

```rust
use kokage::{observe::LifecycleEventKind, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = OrderedTree::new();
    tree.add_task(TaskSpec::new("worker", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }));
    let runtime = tree.spawn()?;
    let mut events = runtime.scope().watch_lifecycle();

    runtime.shutdown();
    while let Some(event) = events.next().await {
        println!("{:?} at {:?}", event.kind, event.scope_path);
        if matches!(event.kind, LifecycleEventKind::SupervisorStopped) {
            break;
        }
    }
    runtime.wait().await?;
    Ok(())
}
```

[`LifecycleEventKind`] covers child additions, starts, exits (with the full
exit view), scheduled restarts (with their delay), removals, supervisor
transitions, and `RestartIntensityExceeded` — with each event carrying its
scope path and, for child events, the child's identity: id, `lineage`
(which membership), sequence number, and cumulative restart counters. The
enum is `#[non_exhaustive]`; always match with a catch-all arm.

Delivery is buffered, not conflated. A consumer that falls far behind gets
the oldest details collapsed into one `Lagged { dropped }` marker — the
stream never lies by omission. On lag, refetch a snapshot and realign: every
snapshot carries `lifecycle_seq`, and events expose `seq()`, so the gap-free
recipe is *subscribe first, snapshot second, then skip child events with*
`seq() <= snapshot.lifecycle_seq`. To feed events into an actor instead of a
loop, [`watch_lifecycle_to`] pumps them into any `ActorRef` through a
mapping closure, returning a `Guard`.

Note how this differs from a peer [`MonitorEvent`] watch
([Watching Peers](watching-peers.md)): monitors give one actor a typed,
mailbox-ordered view of one collaborator; the lifecycle stream gives
operators the whole tree with paths and counters. Same underlying model, a
projection per audience.

## Message counters

[`ActorRef::stats`] (one actor) and [`ScopeRef::actor_stats`] (every actor
in a scope, recursively) report [`ActorStats`]: messages received, accepted,
and conflated, sends rejected, outstanding offloads, and current mailbox
depth against capacity. Counters accumulate across restarts; a restarting
actor doesn't reset your dashboards.

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
[`subscribe_snapshots`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.subscribe_snapshots
[`SupervisorSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.SupervisorSnapshot.html
[`ChildSnapshot`]: https://stokes.io/kokage/api/kokage/observe/struct.ChildSnapshot.html
[`watch_lifecycle`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.watch_lifecycle
[`LifecycleEventKind`]: https://stokes.io/kokage/api/kokage/observe/enum.LifecycleEventKind.html
[`watch_lifecycle_to`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.watch_lifecycle_to
[`MonitorEvent`]: https://stokes.io/kokage/api/kokage/enum.MonitorEvent.html
[`ActorRef::stats`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html#method.stats
[`ScopeRef::actor_stats`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html#method.actor_stats
[`ActorStats`]: https://stokes.io/kokage/api/kokage/observe/struct.ActorStats.html
