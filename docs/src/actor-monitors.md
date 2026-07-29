# Watching actors

An actor can watch another actor when a request or piece of state depends on
that peer. A watch follows the *logical* actor — the same restart-stable
identity an `ActorRef` points at — and turns each lifecycle transition into an
ordinary typed message in the observer's mailbox:

```rust
use kokage::{Actor, ActorRef, ActorResult, Context, MonitorEvent};

enum CoordinatorMsg {
    Worker(MonitorEvent),
}

#[derive(Clone)]
struct Coordinator {
    worker: ActorRef<String>,
}

impl Actor for Coordinator {
    type Msg = CoordinatorMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ActorResult {
        ctx.watch(&self.worker, CoordinatorMsg::Worker).detach();
        Ok(())
    }

    async fn handle(
        &mut self,
        message: Self::Msg,
        _ctx: &mut Context<'_, Self>,
    ) -> ActorResult {
        let CoordinatorMsg::Worker(event) = message;
        match event {
            MonitorEvent::Up { actor_id, generation } => {
                eprintln!("{actor_id} generation {generation} is up");
            }
            MonitorEvent::Down { actor_id, generation, reason } => {
                eprintln!(
                    "{} generation {} exited: {:?}",
                    actor_id, generation, reason
                );
            }
            MonitorEvent::Lagged { actor_id, dropped } => {
                eprintln!("{actor_id} watch lagged, {dropped} transitions dropped");
            }
            MonitorEvent::Terminated { actor_id, .. } => {
                eprintln!("{actor_id} is permanently gone");
            }
            _ => {}
        }
        Ok(())
    }
}
```

A watch belongs to the observing and watched actor memberships. It survives
restarts on both sides, so it never needs to be re-registered while both
memberships live. Events that arrive while the observer is between
incarnations wait for its next mailbox binding. Events arrive in lifecycle
order:

- `MonitorEvent::Up` — an incarnation started. The first actor run is
  generation `0`, and each restart increments the generation. Registering a
  watch on an already-running target delivers an immediate `Up` for the
  current incarnation, so the observer always learns the current state.
- `MonitorEvent::Down` — the current incarnation exited. `DownReason::Normal`
  covers a clean return and cooperative shutdown; `DownReason::Failure` covers
  errors, panics, aborts, and a dropped actor run. If the supervisor restarts
  the actor, a matching `Up` follows.
- `MonitorEvent::Lagged` — one or more transitions were dropped because the
  observer could not keep up under sustained overload. This is a
  resynchronization point, not an edge: treat the events that follow it as the
  target's current state rather than assuming strict `Up`/`Down` alternation. A
  healthy observer never sees it.
- `MonitorEvent::Terminated` — the actor is permanently gone (its binding was
  terminated, or it was dropped without ever starting). No further events
  will be delivered.

A watch registered before the target's first start stays silent until
generation `0` starts, and a watch registered between incarnations stays
silent until the next `Up`. This removes actor-start and restart ordering
races from a declaration set: there is never a reason to retry registration.

`ctx.watch` returns a cloneable `Guard`. Dropping any retained guard cancels
the watch; call `detach()` when membership ownership should keep it alive
without storing a guard. Calling `cancel` on any clone suppresses future
delivery. Cancellation cannot retract an event already accepted by the
mailbox. Permanently removing either actor membership also ends the watch; a
watched actor's final `Terminated` event is queued before its membership
removal completes. Once that terminal event is delivered, `is_finished()` is
true while `is_cancelled()` remains false; explicit cancellation and observer
membership removal report cancellation as well as eventual completion.

Calling `watch` again for the same observer/subject pair is idempotent. This
keeps the common pattern of registering in `on_start` and detaching safe when
the observer restarts: the replacement incarnation gets the existing watch
rather than a duplicate immediate `Up`. The mapping closure from the first
registration is membership-owned and remains in use until cancellation, so it
should capture durable configuration rather than incarnation-local state. This
also applies to repeated calls within one incarnation: every returned `Guard`
aliases the same watch, the later mapping closures are unused, and dropping or
cancelling any non-detached alias cancels the pair.

That no-snapshot behavior depends on detaching the guard. If an actor retains
the guard in incarnation state, a crash drops it and cancels the watch. The
replacement actor therefore installs a fresh watch and receives an immediate
`Up` when the target is running. Target transitions in the gap between the old
guard's drop and the new registration are not staged. Choose detached
membership ownership when the watch and its transition history must survive
observer restarts; choose a retained guard when each incarnation should own a
fresh registration and snapshot.

Events that a detached watch delivered to a previous incarnation and then lost
in a crash are not replayed, so an observer that needs its last known target
state after restart must persist that state durably. To deliberately trade
staged transition history for a fresh snapshot, cancel or drop the existing
guard and call `watch` again. A running target then emits an immediate `Up`, a
terminated target emits an immediate `Terminated`, and a target between
incarnations remains silent until its next `Up`.

Watch notifications use the observer's ordinary mailbox. FIFO mailboxes wait
for capacity and preserve every accepted notification. A conflating mailbox
may replace an unread event with a later one, so use FIFO when every
transition must be observed.

Undelivered events are held in a bounded per-watch buffer. Under normal load
this is never a factor — lifecycle events are rare. But if an observer's
mailbox stays full while its target restarts in a tight loop, the buffer caps
the memory that can accumulate: the oldest staged events are dropped. The loss
is never silent — the dropped span is folded into a single `Lagged` marker, so
the observer learns it fell behind and can resynchronize to the current state
rather than replaying the entire storm. Because no mailbox mode can guarantee
that every transition is observed under sustained overload, consumers that
react to individual transitions should treat `Lagged` as a resync point. The
terminal `Terminated` event is always the newest event, so it is never
dropped.

For one-shot, incarnation-scoped monitoring — "tell me if the incarnation I
am talking to right now dies" — watch the target, act on the first `Down`,
and cancel the handle.
