# `shard_store` findings

`shard_store` is an executable acceptance script for a state-owning system
whose topology changes are deliberate. It uses one ordered subtree per shard,
a dynamic parent scope, a single membership-router actor, and an
application-owned directory that atomically replaces range-to-`ActorRef`
bindings. The CI smoke run covers a one-to-two split, buffered traffic during
handoff, a rolling config replacement, and a crash after the old actor has
persisted its handoff image.

## API friction and improvement opportunities

- Planned replacement is intentionally not the same operation as supervised
  restart. Today the application composes `add_subtree`, readiness, a userland
  directory cutover, and exact-handle `remove`. The example now makes that
  transaction explicit: a durable handoff fence is established before the
  snapshot; pre-commit failures remove every mounted successor before aborting
  the fence; directory cutovers are idempotent by operation id; and retirement
  failures are reconciled against exact membership before the router publishes
  its outcome. This is safe but substantial application scaffolding. A future
  dynamic-scope transaction/helper could provide mount/commit/retire hooks and
  compensating cleanup while leaving state transfer application-defined.
- `ActorRef` correctly follows crash restarts of one membership but correctly
  does **not** follow a same-id replacement membership. The distinction makes
  stale handles safe, but planned replacement therefore needs an explicit
  registry/rebind protocol. A documented `ServiceRef`/route-cell pattern (or a
  small library adapter) would make this common boundary easier to discover
  without weakening exact membership identity.
- Kokage snapshots make crash restarts unambiguous through lineage,
  generation, per-child restart count, and scope total restart count. Planned
  remove/add operations are instead visible as fresh lineages at generation
  zero. No runtime change was needed for this distinction; the example keeps
  a domain counter (`planned_rebinds`) beside those runtime counters.
- There is no durability opinion in the library, which is appropriate. The
  example keeps both its prepared image and active drain fence outside the
  actor incarnation. `ReplyDropped` is safe to retry because preparation is
  idempotent, but an immediate single retry can still race the failed mailbox.
  The example therefore waits for an application-owned actor-start signal and
  retries only that known-safe outcome under one overall deadline. A
  `ResponseTimedOut` is not retried: it is reconciled against the durable
  prepared state. The library's error taxonomy is sufficient, though a public
  way to await the next `ActorRef` incarnation would remove the need for the
  application-owned start signal.
- Calls already offloaded before the router marks a range as transitioning are
  tracked by key. The router records the transition first, buffers every later
  request for that range, and launches handoff only after the accepted prefix
  has completed. This keeps caller-visible operations live instead of exposing
  the shard's internal drain fence. Direct holders of a stale endpoint are
  still protected by that durable fence: the shard mutex orders each write
  either before the snapshot (and therefore includes it) or after the fence
  (and explicitly rejects it for retry).
- The example's deterministic transition gates use Tokio `Notify` together
  with atomic evidence. Every wait registers and enables its notification
  future before testing the atomic predicate, avoiding the check-then-subscribe
  lost-wakeup race. Focused tests cover accepted-request quiescence, pause and
  counter notifications, the stale-endpoint crash window, failures before both
  split mounts and cutover, and reconciliation of lost cutover and retirement
  outcomes. The same test target repeats crash recovery 128 times to guard the
  previously observed `ReplyDropped` flake.
