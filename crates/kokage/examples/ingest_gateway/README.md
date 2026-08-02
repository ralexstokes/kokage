# Ingest gateway

`ingest_gateway` is the ingress-driven flagship example from #235. It is an
assertion-driven application, not a benchmark: scripted clients use real
loopback TCP and length-prefixed JSON while every nondeterministic edge is
bounded by an explicit wait.

Run the same headless acceptance path used by CI:

```sh
./scripts/dev cargo run --locked -p kokage --example ingest_gateway --features serde
```

Attach `kokage-console` to the verified tree and keep it open until Ctrl-C:

```sh
./scripts/dev cargo run --locked -p kokage --example ingest_gateway --features serde -- --console
```

The script asserts:

- a supervised listener owns the loopback socket and creates one one-shot raw
  actor per accepted `TcpStream` in a dynamic connection scope;
- partial-header, truncated-body, oversized-length, and invalid-JSON clients
  each fail and remove only their own connection actor, while a later healthy
  client still reaches the sink;
- the sink's first two connection attempts fail and restart beneath equal-
  jitter exponential delays that remain inside the configured intensity
  budget and expected delay intervals;
- holding the sink fills the shipper, batcher, and enricher FIFO mailboxes in
  order, propagating backpressure to the edge;
- ingress deliberately sheds only `SendErrorKind::Full`; the application-owned
  report distinguishes intentional overload loss of non-replaceable events
  from pipeline unavailability, which degrades (fails) a connection instead;
- live `ActorStats` exactly match each full mailbox's accepted, received,
  rejected, depth, and capacity values; and
- lifecycle events account for the two sink failures/restarts, four isolated
  malformed-client failures, and all six connection removals without lag.

## API friction and findings

### Fixed: consuming one-shot actors

A connection actor naturally owns a unique, non-`Clone` `TcpStream` and must
leave its dynamic scope when the peer disconnects. Before this example,
`ActorSpec` required a reusable `Fn` factory even when configured with
`RestartPolicy::never()` and `remove_on_terminal_exit()`. The only application
workaround was to hide the socket in shared optional state and rely on the
supervisor never asking the factory twice.

This example adds `OneShotActorSpec` and
`DynamicScopeRef::{spawn_actor_once, spawn_actor_once_spec}`, mirroring the
existing one-shot task API. The consuming `FnOnce` factory makes unique-resource
ownership honest; restart configuration is intentionally absent and terminal
membership removal is the default.

### Observation and policy findings

- `ActorStats` belongs to the target's stable `ActorRef`, so sends performed by
  connection actors correctly accumulate in the enricher's accepted/rejected
  counters. Dynamic connection stats disappear when their membership is
  removed, while the application report retains end-to-end evidence.
- The ingress report publishes each valid frame and its terminal `try_send`
  outcome in one update. Waiting for the valid-frame count therefore also
  orders the exact `ActorStats` assertions after the corresponding send.
- Length-prefixed input needs to distinguish clean EOF before a frame from a
  partial header, truncated body, oversized declaration, and invalid JSON.
  Each protocol error is typed and counted once before its connection fails.
- Equal jitter is deliberately random. Deterministic acceptance should assert
  each lifecycle-reported delay's documented interval and generation order,
  not a specific duration.
- Startup can fail before `Tree::spawn` returns to its caller. A lifecycle
  report that must include those attempts should subscribe through the tree's
  pre-spawn `ScopeRef`, then spawn; subscribing afterward introduces a real
  observation race. The existing pre-spawn handle API covers this cleanly.
- FIFO backpressure and `try_send` load shedding compose without another
  runtime API. The important application decision is explicit: `Full` is a
  shed event, while `NotRunning`/`Terminated` is a degraded connection that
  fails so the client can reconnect.
- Partial-batch flush remains application protocol, appropriately expressed as
  a bounded actor call before shutdown.

### Residual coverage

The scripted sink failures happen during readiness, before ingress begins.
This makes jittered backoff load-bearing for tree startup and exercises restart
intensity and lifecycle evidence without making delivery semantics ambiguous.
Failure after a sink has accepted an in-flight batch remains deliberately out
of scope: demonstrating that case needs an explicit acknowledgement/replay
protocol rather than implying that mailbox restart alone preserves delivery.
