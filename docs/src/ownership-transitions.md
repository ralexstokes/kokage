# Ownership during membership transitions

Removing an actor is not an atomic handoff of application traffic. A message
can be accepted just before the actor observes shutdown, and mailbox acceptance
only means at-most-once delivery to that incarnation. If the default
`DrainPolicy::Discard` is used, the queued message can disappear during
removal. The runtime cannot infer who should own that work, whether it is safe
to replay, or how to deduplicate its effects.

For work that must survive dynamic removal, use an application-level handoff:

```text
                       one writer for membership
sender ──message──▶ router ──(key → subtree, ref)──▶ session
                       │                                │
                       │ Evict                          │ late message
                       │                                │ after Evict sent
                       │ drop map entry, pipeline       │
                       │ remove_child("key#7")          │
                       ▼                                │
                   no entry ◀──────bounce───────────────┘
                       │ next message or bounce
                       ▼
            add_subtree("key#8") — fresh id
                       │ rehydrate from journal
                       ▼
                    session (replacement)
```

The protocol has four parts:

1. **Choose one membership writer.** A router owns the map from logical key to
   the live subtree id and session `ActorRef`. No other task adds, removes, or
   swaps a session, so membership state and traffic routing are serialized by
   one actor mailbox.
2. **Give each incarnation its own subtree id.** The writer mints
   `key#epoch` ids, so a replacement never contends with a predecessor whose
   removal is still draining. That is what lets removal be pipelined without
   any intermediate membership state: on `Evict` the router drops the map
   entry and issues `remove_child`; nothing routes on the removal's
   completion. It also removes the need to stamp `Evict` with a generation —
   an incarnation sends at most one `Evict`, and its successor can only be
   created after that `Evict` was consumed, so a stale `Evict` can never
   target a fresh entry.
3. **Bounce the race and drain.** After requesting eviction, the retiree sends
   any late arrival back to the router and uses `DrainPolicy::Drain`. FIFO
   mailboxes preserve sequential enqueue order from one sender, so the
   retiree's `Evict` reaches the router before its later bounce. The bounce
   therefore finds no membership entry and is routed to a fresh subtree
   instead of being forwarded back in a loop. Configure cooperative shutdown
   with enough grace for this drain to finish: immediate abort or expiry of
   the grace period can skip remaining drain work.
4. **Rehydrate instead of replaying mail.** The replacement subtree's static
   session is reborn from its builder and rebuilds context from the journal.
   No in-memory buffer of undelivered messages changes hands; the only
   message that crosses incarnations is the bounced one, and it travels
   through the writer like any other traffic. Old subtree handles and refs
   remain terminal and cannot address the new membership.

The runnable `agent_control` example implements this exact recipe. The
router's epoch-minted `add_subtree` membership and pipelined removal live in
`crates/tokio-otp/examples/agent_control/router.rs`; the retiree bounce and
`DrainPolicy::Drain` live in `session.rs`; phase 7 in `main.rs` injects
traffic inside the eviction window and proves the replacement session answers
it with replayed context. An earlier revision of the example negotiated the
same transition over a reused child id — a shared generation counter stamped
into `Evict`, an `Evicting(buffer)` membership state, and a
removal-completed handshake before replaying the buffer. Unique per-incarnation
subtree ids made all three unrepresentable rather than merely handled.

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
