# Wiring Patterns

Two chapters of plumbing remain before we turn to operations: how to wire
actors that refer to *each other*, and how to declare actor factories without
hand-writing clone boilerplate.

## Cycles with `ActorSlot`

`Tree::add_actor` returns the ref after receiving the id and factory, which
works while dependencies flow one way. The moment two actors need each other
— the front desk sends jobs to the press, the press reports completions back
to the desk — neither can be declared "first".

[`ActorSlot`] breaks the cycle by separating *identity* from *definition*.
Create every slot, take refs from all of them, then consume each slot into a
spec:

```rust
use kokage::{ActorSlot, prelude::*};
use tokio::sync::mpsc;

enum DeskMsg {
    Order(String),
    Printed(String),
}

struct FrontDesk {
    press: ActorRef<String>,
    receipts: mpsc::UnboundedSender<String>,
}

impl Actor for FrontDesk {
    type Msg = DeskMsg;

    async fn handle(&mut self, msg: DeskMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match msg {
            DeskMsg::Order(job) => self.press.send(job).await?,
            DeskMsg::Printed(job) => self.receipts.send(job).expect("receiver alive"),
        }
        Ok(())
    }
}

struct Press {
    desk: ActorRef<DeskMsg>,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("printing: {job}");
        self.desk.send(DeskMsg::Printed(job)).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (receipts_tx, mut receipts_rx) = mpsc::unbounded_channel();

    // 1. A slot per actor: identity and typed ref exist before any definition.
    let desk_slot = ActorSlot::<DeskMsg>::new("front-desk");
    let desk_ref = desk_slot.actor_ref();
    let press_slot = ActorSlot::<String>::new("press");
    let press_ref = press_slot.actor_ref();

    // 2. Consume each slot into a spec, wiring refs in both directions.
    let desk_spec = desk_slot.define({
        let press = press_ref.clone();
        let receipts = receipts_tx.clone();
        move || FrontDesk { press: press.clone(), receipts: receipts.clone() }
    });
    let press_spec = press_slot.define({
        let desk = desk_ref.clone();
        move || Press { desk: desk.clone() }
    });

    // 3. Place the specs wherever they belong.
    let mut tree = Tree::new();
    tree.add_actor_spec(desk_spec);
    tree.add_actor_spec(press_spec);
    let runtime = tree.spawn()?;

    desk_ref.send(DeskMsg::Order("menus x50".to_owned())).await?;
    println!("receipt for: {}", receipts_rx.recv().await.expect("printed"));

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

Because `define` consumes the slot, "defined twice" is unrepresentable, and
a slot you forget to define shows up as an actor that never binds — refs to
it wait, they don't dangle. The refs-first shape also means startup order
stops mattering for wiring: the desk starts before the press exists and its
first `send` simply waits for the press to bind.

There is no string-keyed registry to typo and no `Option<ActorRef>` to
unwrap-later: cyclic wiring stays fully typed, checked at compile time.
(When you *want* name-based, late-bound lookup — plugins, sessions — build a
small directory actor; that's a userland protocol, and the repository's
`directory.rs` example shows one.)

## Factories without boilerplate: `#[derive(ActorFactory)]`

Every `ActorSpec` needs a factory — something that builds a fresh actor per
incarnation. Closures work (`ActorFactory` is implemented for any
`Fn() -> A`), but for actors with several configuration fields, the
`clone-outside, clone-inside` dance gets repetitive. The derive generates a
reusable factory struct instead:

```rust
use std::collections::VecDeque;

use kokage::prelude::*;

#[derive(kokage::ActorFactory)]
struct Press {
    supplier_url: String,
    #[factory(default)]
    queue: VecDeque<String>,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.queue.push_back(job);
#         let _ = &self.supplier_url;
        Ok(())
    }
}

# fn main() {
let spec = ActorSpec::new("press", PressFactory {
    supplier_url: "https://paper.example".to_owned(),
});
# let _ = spec;
# }
```

`#[derive(ActorFactory)]` on `Press` generates a `PressFactory` struct with:

- one field per *configuration* field (`supplier_url`), cloned into each new
  incarnation — these fields must be `Clone`;
- no field for anything marked `#[factory(default)]` (`queue`), which is
  rebuilt with `Default::default()` per incarnation — exactly what you want
  for per-run working state that must *not* leak across restarts.

The derive works on non-generic structs with named fields, and lives behind
the (default-on) `derive` feature. It is intentionally not in the prelude:
write `#[derive(kokage::ActorFactory)]` or `use kokage::ActorFactory;`.

One design note that applies to *all* factories, closure or derived: a
factory is synchronous and infallible. Anything async or fallible — opening
sockets, reading config files — belongs in `on_start`, where a failure is a
supervised, restartable event rather than a construction-time surprise.

[`ActorSlot`]: https://stokes.io/kokage/api/kokage/struct.ActorSlot.html
