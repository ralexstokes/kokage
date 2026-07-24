# Observability

The crates expose four views into a running system:

1. supervisor events
2. supervisor snapshots
3. `tracing`, pull-based actor stats, and optional metrics
4. the `tokio-otp-console` web UI

## Events And Snapshots

`RuntimeHandle::supervisor_handle()` exposes the lower-level supervisor
control surface. Subscribe there for lossy lifecycle events used by logging,
tracing, and dashboards:

```rust,ignore
let mut events = handle.supervisor_handle().subscribe();
tokio::spawn(async move {
    loop {
        match events.recv().await {
            Ok(event) => println!("event: {event:?}"),
            // The broadcast channel is bounded: a slow subscriber skips
            // missed events and observes the gap as `Lagged`.
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                println!("missed {skipped} events");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
});
```

**Events are lossy observability, not durable control.** The subscription is a
bounded broadcast channel, and events forwarded from nested supervisors can
additionally be dropped without a `Lagged` marker on your receiver. Never
build safety logic by counting events: it can silently under-count. If a
consumer must gate a decision on events anyway, it has to fail closed on
`Lagged` — treat the gap as if the guarded condition occurred.

`RuntimeHandle::snapshot()` returns the current tree state, and
`subscribe_snapshots()` returns a `watch::Receiver` that updates when the
snapshot changes. The watch channel conflates intermediate snapshots but never
lags, and snapshots carry cumulative counters — the per-child
`ChildSnapshot::restart_count` and the supervisor-level
`SupervisorSnapshot::total_restarts` — so counter deltas account for every
restart even when updates are conflated. This is the reliable source to drive
control logic from.

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

## Reliable Restart Counting

For control logic that reacts to restart activity — an aggregate restart
breaker, for example — pump the reliable counts directly into its actor:

```rust,ignore
let restart_watch = handle
    .subtree("venues")
    .unwrap()
    .watch_restarts_to(&breaker, |total| {
        HealthMsg::RestartsObserved { total }
    });
```

`watch_restarts_to` drives a `RestartWatch` over the monotonic
`total_restarts` counter and sends a cumulative total (starting at zero when
the pump is created) through the target's ordinary mailbox policy. Treat the
value as idempotent state, not an additive delta. That contract survives both
snapshot conflation and a latest-wins target mailbox: a newer message already
contains every restart represented by the older one. After a target restart,
the pump sends the latest total again so the fresh incarnation can restore its
state. As with every actor send, acceptance is not an acknowledgement that the
handler processed the message; use an application-level acknowledgement when
processing itself must be confirmed.

Keep the returned `RestartWatchRef` alive for as long as the pump is needed.
Dropping or cancelling it stops the pump; it also stops when the target
permanently terminates or the watched supervisor reaches a terminal state,
even if delivery is currently waiting for mailbox capacity. An actor restart
is not terminal, so the pump follows the stable ref into the next incarnation.

Its scope is the watched supervisor's **direct children**: to cover a nested
subtree, watch each nested runtime handle (`handle.subtree(id)`) —
`total_restarts` does not aggregate across depth, whereas an event subscription
forwards nested events (lossily). For a raw supervisor, navigate explicitly
through `handle.supervisor_handle().supervisor(id)`. Call `watch_restarts()`
directly when a non-actor consumer needs to await the counts itself.
Nested supervisors carry the counter across their own incarnations, so a watch
on a restart-stable handle keeps working through restarts of the watched
supervisor itself, and `next()` returns `None` once the supervisor can never
restart a child again. The `trading_engine` example's phase-7 breaker is built
this way. Note that the counter records scheduled restarts — the same
occurrences the restart-intensity window records, including clean exits
restarted under `RestartPolicy::Always`; under `OneForAll`, sibling respawns
caused by another child's exit do not increment it.

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
`RunnableActorFactory::actor_with_options`. Dynamic registration exposes the
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

`ActorStats::outstanding_steps` is a point-in-time gauge of bounded futures
owned by the current actor incarnation. It rises when `ActorContext::step`
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
    .build()
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
    .build()
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
