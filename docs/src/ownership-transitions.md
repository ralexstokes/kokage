# Ownership during membership transitions

Removing an actor is not an atomic handoff of application traffic. A message
can be accepted just before the actor observes shutdown, and mailbox acceptance
only means at-most-once delivery to that incarnation. The default
`Shutdown::drain_for(5s)` handles the queued prefix during a clean removal,
within the actor's configured shutdown grace. A queued message can still
disappear if the actor selects `Shutdown::discard_after_current`, crashes, is
aborted, or exhausts its shutdown grace before draining. The runtime cannot
infer who should own that work, whether it is safe to replay, or how to
deduplicate its effects.

For work that must survive dynamic removal, use an application-level handoff:

```text
                       one writer for membership
sender ──message──▶ router ── Active(ref, "key#7") ──▶ session
                       │                                  │
                       │ Evict("key#7"),                  │ late message
                       │ re-sent until honored            │ after Evict sent
                       ▼                                  │
             Removing [buffer] ◀────────bounce────────────┘
                       │ pipelined remove_child("key#7") … Reaped
                       │ await add_subtree("key#8") — insertion reply
                       ▼
                Active(ref, "key#8") ──▶ session rehydrates from journal
```

The protocol has five parts:

1. **Choose one membership writer.** A router owns the map from logical key to
   a slot: `Active` with the live subtree id and session `ActorRef`, or a
   transition state with a buffer. No other task adds, removes, or swaps a
   session, so membership state and traffic routing are serialized by one
   actor mailbox.
2. **Give each incarnation its own subtree id, from an allocator that
   outlives the writer.** The writer mints `key#epoch` ids, so a replacement
   never contends with a predecessor whose removal is still draining, and a
   writer reborn after a crash never re-mints an id that still exists — which
   is why the allocator (and the mount handle) must live in the writer's
   factory captures, not its state.
3. **Retire by name, and repeat the request until it is honored.** `Evict`
   carries the subtree id of the incarnation requesting retirement. The
   writer honors it only against the slot that minted that id; any other
   `Evict` — a duplicate from a reborn session, or an orphan minted by a
   previous writer incarnation that the reborn writer no longer routes to —
   is removed by name without touching live state. Because the retiree
   re-sends its request every idle sweep until teardown lands, orphans
   self-retire instead of leaking.
4. **Pipeline a removal whose drain depends on the writer; distinct adds may
   be awaited.** `remove_child` deliberately resolves only after detachment. If
   the retiree must bounce messages through this writer to finish draining,
   awaiting that removal would stop the writer from receiving those messages
   and self-deadlock until the grace expires. Keep a `Removing` state, pipeline
   that operation, and consume bounces until its completion arrives. The
   supervisor loop continues dispatching unrelated commands during the drain,
   so an awaited `add_subtree` for fresh `key#8` resolves as soon as the new
   membership is inserted and immediate startup is scheduled. A separate
   `Mounting` buffer is no longer required merely to avoid control-loop
   serialization.
5. **Bounce the race and drain.** After requesting eviction, the retiree
   sends any late arrival back to the router and uses `Shutdown::drain_for`.
   FIFO mailboxes preserve sequential enqueue order from one sender, so the
   retiree's `Evict` reaches the router before its later bounce, and the
   bounce lands in the transition buffer (or mints the replacement) instead
   of being forwarded back in a loop. Configure cooperative shutdown with
   enough grace for this drain to finish: immediate abort or expiry of the
   grace period can skip remaining drain work.

The runnable `agent_control` example implements a conservative version of this
recipe. Its offload-based router retains symmetric `Mounting` and `Removing`
states, although only the removal must be pipelined for control-plane safety.
The slot machine, epoch-minted `add_subtree` membership, and explicit draining
shutdown declaration live in `crates/kokage/examples/agent_control/router.rs`;
the retiree bounce and retirement re-request live in `session.rs`; phase 7 in
`main.rs` injects traffic inside the eviction window and proves the replacement
session answers it with replayed context.

An earlier revision of the example negotiated the same transition over a
*reused* child id: a generation counter shared between router and sessions and
stamped into `Evict` for match-on-arrival, plus an `Evicting(buffer)` state
gated on a removal-completed handshake. Per-incarnation subtree ids deleted
the shared counter and the same-id coordination — the id itself is the
incarnation identity, which a bare generation could not provide across writer
restarts — and subtree ownership deleted the retiree's teardown-flush
machinery. The `Removing` buffer remains part of the application handoff: it
owns raced traffic while the predecessor drains. A distinct-id `Mounting`
buffer is optional now that additions are dispatched during that drain and
reply on insertion.

## Put durable ownership outside the mailbox

The bounce closes the clean-removal race, but it is not durable. If the
process dies, in-memory mail can still vanish. For end-to-end delivery, append
at the transport boundary, acknowledge only after append, and redeliver
unacknowledged envelopes. Consumers should deduplicate stable envelope or effect keys. The
`agent_control` chat simulator, journal, and tool host demonstrate those three
pieces.

## Scale rehydration without blocking appends

The example keeps appends, reports, and every session `Replay` request in one
journal actor. That makes ordering straightforward, but it also makes the
mailbox the serialization point: a cold-start or restart storm queues all
rehydration reads behind earlier appends and replays. Watch the journal's
mailbox depth and message-processing latency so this pressure is visible before
it stretches recovery time or delays acknowledgements.

At production scale, separate or reduce the replay work while preserving an
explicit consistency boundary:

- **Partition by session or tenant.** Route appends and replays for the same key
  to the same journal shard, allowing unrelated sessions to recover in
  parallel.
- **Snapshot state.** Periodically persist a compact session snapshot and replay
  only the journal tail after its sequence number.
- **Add a read-only replay path.** Keep appends serialized through the writer,
  but serve replay from a database read connection, replica, or immutable log
  segments that do not share the writer's mailbox. Define which committed
  sequence a reader must observe before a recovered session accepts traffic.

These are application storage choices, not actor-runtime guarantees. Splitting
reads from writes without a sequence or snapshot boundary can make recovery
fast but stale.

`remove_child` deliberately does not return undelivered `Vec<M>` values. The
supervisor handle is untyped, such a return would require downcasting, and it
would not cover crashes or already-executed effects. Delivery guarantees belong
at the journal/acknowledgement boundary, and the per-incarnation-subtree shape
above leaves no membership buffer to extract a helper from.
