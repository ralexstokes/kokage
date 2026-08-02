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
  directory cutover, and exact-handle `remove`. That is expressive, but every
  state-owning application must also design rollback for failures between
  those steps. A future dynamic-scope transaction/helper could mount a
  successor and retire the predecessor with explicit commit/abort hooks while
  leaving state transfer application-defined.
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
  example keeps a prepared handoff image outside the actor incarnation and
  retries the stable source ref after `ReplyDropped`. The existing call error
  taxonomy was sufficient to distinguish the known crash-before-reply case
  from an unknown timeout outcome.
