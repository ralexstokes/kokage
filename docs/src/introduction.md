# Introduction

**kokage** 木陰 — *actors in the shade of a supervision tree*.

Kokage brings OTP-style supervision trees and typed actors to Rust, as a thin
layer over an async scheduler (Tokio today). The organizing idea is the one
that has kept telecom switches running for decades: **let it crash**. Instead
of defensively handling every failure where it happens, you split your program
into small, isolated units of work — actors and tasks — and let a *supervisor*
restart the ones that fail.

Three properties make that practical here:

- **Typed mailboxes.** Every actor declares one message type, and the
  [`ActorRef`] you use to reach it is typed accordingly. There is no `Any`
  downcasting and no stringly-typed dispatch.
- **Restart-stable references.** An [`ActorRef`] addresses the actor's
  *membership* in the tree, not one particular run of it. When a supervisor
  restarts an actor, existing refs transparently reconnect to the
  replacement.
- **One front door.** Supervision trees are the only way to build a running
  system, so every actor and task is supervised, observable, and shut down in
  a known order — there are no orphans.

## What this book is

A tutorial. It builds up a small fault-tolerant service — a print shop with a
front desk, presses that occasionally jam, and a growing cast of helpers —
starting from a single actor and ending with dynamic membership, custom
receive loops, and production observability.

The chapters are ordered from simple to advanced:

- **First Steps** — define an actor, spawn a tree, send messages, ask for
  answers, and understand mailboxes and backpressure.
- **Fault Tolerance** — crash things on purpose: restart policies, supervision
  strategies, nested trees, and plain async tasks as supervised children.
- **The Actor Toolkit** — lifecycle hooks, timers, blocking work, bounded
  offloads, and watching peer actors.
- **Advanced Composition** — trees whose membership changes at runtime,
  cyclic wiring, ownership and shutdown semantics, and raw actors with
  hand-written receive loops.
- **In Production** — tracing, metrics, snapshots, and lifecycle streams.

Every Rust code block in this book is compiled — and most are run — against
the current `kokage` sources as part of CI, so what you read is what the
library actually does.

## Prerequisites

You should be comfortable with basic Rust and have seen async/await and Tokio
before. No prior actor-system or Erlang/OTP experience is assumed.

Kokage needs one dependency, and your binary needs Tokio for its entry point:

```toml
[dependencies]
kokage = { git = "https://github.com/ralexstokes/kokage" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The crates are early-stage and not yet on crates.io; APIs may change.

## A taste

Here is the whole shape of a kokage program — declare actors, place them in a
tree, spawn it, talk to them, shut down:

```rust,no_run
use kokage::prelude::*;

struct Press;

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("printing {job}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    let press = tree.add_actor("press", || Press);
    let runtime = tree.spawn()?;

    press.send("business cards x100".to_owned()).await?;

    runtime.shutdown().await?;
    Ok(())
}
```

If the press panics or returns an error, its supervisor restarts it and the
`press` ref keeps working. The next chapter takes this apart piece by piece.

Alongside the tutorial, the [API documentation](https://stokes.io/kokage/api/)
covers the full reference surface, and the repository's
[`examples/`](https://github.com/ralexstokes/kokage/tree/main/crates/kokage/examples)
directory holds runnable programs for every feature this book touches.

[`ActorRef`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html
