# Supervision Trees

One press was a workshop. A real shop has structure: a press *room* with
machines that work as a unit, and a front desk in front of it. Supervision
trees let you draw that structure explicitly — and failure handling follows
the drawing.

## Nesting scopes

An [`OrderedTree`] can contain actors, tasks, and *other trees*. The shop:

```rust
use kokage::prelude::*;

struct Press {
    name: &'static str,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("[{}] printed: {job}", self.name);
        Ok(())
    }
}

struct FrontDesk {
    presses: Vec<ActorRef<String>>,
    next: usize,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let press = &self.presses[self.next % self.presses.len()];
        self.next += 1;
        press.send(job).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The press room: two presses that share their fate.
    let mut press_room = OrderedTree::new().strategy(Strategy::OneForAll);
    let press_a = press_room.add_actor(ActorSpec::new("press-a", || Press { name: "press-a" }));
    let press_b = press_room.add_actor(ActorSpec::new("press-b", || Press { name: "press-b" }));

    // The shop: the press room first, then the front desk that feeds it.
    let mut shop = OrderedTree::new();
    shop.add_subtree("press-room", press_room);
    let desk = shop.add_actor(ActorSpec::new("front-desk", move || FrontDesk {
        presses: vec![press_a.clone(), press_b.clone()],
        next: 0,
    }));

    let runtime = shop.spawn()?;

    desk.send("posters x20".to_owned()).await?;
    desk.send("stickers x300".to_owned()).await?;

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

Two structural rules do a lot of quiet work here:

- **Startup order is declaration order.** The press room comes up before the
  front desk, so by the time the desk takes its first order, the presses
  exist. (Refs tolerate out-of-order anyway — `send` waits for the target to
  bind — but ordered startup makes readiness reasoning easy.)
- **Shutdown is the reverse.** The desk stops first — draining its queued
  orders into the still-running presses — then the press room. Consumers
  outlive their producers.

Child ids are local to their scope, so `"press-a"` may appear in two
different rooms without conflict. Validation happens at `spawn`: duplicate
ids in one scope, an empty id, a zero mailbox capacity, or a zero restart
window all return a [`BuildError`] instead of a half-built tree.

## Strategies: who shares the fate?

Each scope has a [`Strategy`] deciding what a child's failure means for its
siblings:

- `Strategy::OneForOne` (default) — only the failed child restarts.
  Independent workers.
- `Strategy::OneForAll` — all children in the scope restart together. Use it
  when siblings hold state about each other; our two presses share
  calibration, so a fresh `press-a` must be paired with a fresh `press-b`.
- `Strategy::RestForOne` — the failed child *and every child declared after
  it* restart. This encodes a startup pipeline: later children depend on
  earlier ones, so a failure invalidates everything downstream of it, but
  not upstream.

Because the strategy lives on the scope — not on the actors — you choose
fate-sharing by *drawing the tree*, not by threading flags through actor
code. Wiring (who holds a ref to whom) never changes execution topology.

## Policies: defaults and overrides

Restart and shutdown policies compose along the same structure. A tree sets
defaults for its own children; each child may override; a nested subtree's
*edge* in its parent is configured on the [`SubtreeSpec`]:

```rust
# use std::time::Duration;
# use kokage::{SubtreeSpec, prelude::*};
# let press_room = OrderedTree::new();
// Defaults for children declared in this scope.
let mut shop = OrderedTree::new()
    .default_restart(Restart::on_failure().limit(5, Duration::from_secs(30)))
    .default_shutdown(Shutdown::drain_for(Duration::from_secs(5)));

// The press room as a child of the shop: its own restart budget as a unit.
shop.add_subtree(
    "press-room",
    SubtreeSpec::from(press_room)
        .restart(Restart::on_failure().limit(2, Duration::from_secs(60))),
);
# let _ = shop;
```

Note the two layers are different things: `default_restart` *inside*
`press_room` would govern each press individually; the `SubtreeSpec` restart
governs the press room *as a whole* when it exhausts its internal budget and
fails upward. Nested scopes do not inherit the parent's defaults — each scope
states its own.

This is also your bulkhead design tool from the last chapter: a subtree is a
blast compartment. Give a flaky subsystem its own scope with a modest budget,
and its worst day costs the shop a compartment restart instead of the tree.

## Seeing the shape

Trees can describe themselves before spawning. [`outline`] returns the
declared structure — and `{:?}` on a tree prints it — which is handy in
tests that pin down an application's topology:

```rust
# use kokage::prelude::*;
# struct Press;
# impl Actor for Press {
#     type Msg = String;
#     async fn handle(&mut self, _j: String, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
let mut shop = OrderedTree::new();
shop.add_subtree("press-room", OrderedTree::new());
shop.add_actor(ActorSpec::new("front-desk", || Press));
assert_eq!(shop.outline().child_ids(), ["press-room", "front-desk"]);
```

Actors are not the only thing a tree can supervise. Next: plain async tasks
as first-class children.

[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`BuildError`]: https://stokes.io/kokage/api/kokage/enum.BuildError.html
[`Strategy`]: https://stokes.io/kokage/api/kokage/enum.Strategy.html
[`SubtreeSpec`]: https://stokes.io/kokage/api/kokage/struct.SubtreeSpec.html
[`outline`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html#method.outline
