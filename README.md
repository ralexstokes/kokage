# kokage 木陰

> actors in the shade of a supervision tree

OTP-style supervision trees and typed actors — a thin layer over an async
scheduler (Tokio today).

The core idea is the one that has kept telecom switches running for decades:
**let it crash**. Instead of defensively handling every failure in place, you
organize your program into small, isolated tasks and let a *supervisor*
restart the ones that fail.

The actor product needs one dependency. Its prelude covers the day-one actor
surface; hosting and observation APIs are grouped under `kokage::host` and
`kokage::observe`, while advanced actor APIs remain at the crate root. Actor
applications can place raw task children with `kokage::host::ChildSpec`; only
applications using the lower-level supervisor crate directly need a separate
`kokage-supervisor` dependency:

```toml
[dependencies]
kokage = { git = "https://github.com/ralexstokes/kokage" }
```

## A taste

Each actor runs as its own supervised child. When the press crashes, the
supervisor restarts it — and the `orders` ref keeps working across the
restart, transparently reconnecting to the replacement:

```rust
use kokage::prelude::*;

#[derive(Default)]
struct Press;

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ActorResult {
        println!("printing {order}");
        Ok(())
    }
}

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ActorResult {
        self.press.send(order).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Mint the typed ref before moving the declarations into the tree.
    let press_actor = ActorSpec::new("press", Press::default);
    let (press_actor, press) = press_actor.actor_ref();
    let orders_actor = ActorSpec::new("front-desk", move || FrontDesk {
        press: press.clone(),
    });
    let (orders_actor, orders) = orders_actor.actor_ref();

    // Compose the supervision tree, then run it.
    let runtime = OrderedTree::new()
        .actor(press_actor)
        .actor(orders_actor)
        .spawn()?;

    orders.send("business cards x100".to_owned()).await?;

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

`spawn()` returns the owning `Runtime`; keep it alive for as long as the
application should run. Clone `runtime.handle()` when another component needs
non-owning control or observation. Dropping handles has no lifecycle effect,
while dropping the owner requests graceful shutdown—so a discarded
`let _ = tree.spawn()?;` shuts down immediately.

Background actor operations such as watches, mailbox timers, offloads, and
lifecycle/completion pumps return one `kokage::Guard` type. A guard cancels
its operation when dropped. Retain it for scoped ownership, or call
`.detach()` when fire-and-forget behavior is intentional.

The full runnable version is
[`crates/kokage/examples/supervised_actors.rs`](crates/kokage/examples/supervised_actors.rs).

## The crates

`kokage` is the product: typed actors plus the runtime that
supervises them, in one crate. `kokage-supervisor` underneath is deliberately
independent — it knows nothing about actors and is useful on its own for
supervising plain async tasks.

| Crate | Role |
|-------|------|
| [`kokage`](crates/kokage) | The front door: communicating actors with typed mailboxes, restart-stable `ActorRef<M>` handles, request/reply, and cooperative blocking work, with each actor running as its own supervised child under single-use ordered or dynamic trees. |
| [`kokage-supervisor`](crates/kokage-supervisor) | Structured supervision of async tasks: restart policies (`permanent`/`transient`/`temporary`), restart intensity limits, `one_for_one`/`one_for_all` strategies, graceful shutdown, and nested supervision trees. |
| [`kokage-derive`](crates/kokage-derive) | `#[derive(ActorFactory)]` for reusable incarnation factories; re-exported by `kokage` under the default `derive` feature. |
| [`kokage-console`](crates/kokage-console) | *(experimental, git-only)* A live web dashboard for watching a running supervision tree. It is kept outside the published `kokage` feature and dependency surface. |

## Getting started

- **Tutorial book** — builds a small fault-tolerant service from scratch,
  from actor basics through task and actor supervision, dynamic actors, and observability.
  Start at [`docs/src/introduction.md`](docs/src/introduction.md), or run
  `just serve-book` for a local copy.
- **API docs** — `just doc` builds and opens the rustdoc for the workspace.
- **Examples** — each crate ships runnable examples under its `examples/`
  directory, e.g. `cargo run -p kokage --example supervised_actors`. Try
  the console locally with `cargo run -p kokage-console --example console`.

## Status

Early-stage and evolving; APIs may change. The crates are not yet published
to crates.io — use a git dependency as shown above.

## Development

The Nix flake provides both local tooling and CI. Interactively, approve the
checkout for [direnv](https://direnv.net) once and the devshell loads on `cd`;
bare commands work from there:

```sh
direnv allow
just ci      # pull-request fast lane (fmt, clippy, build, tests, book)
just ci-nix  # full clean Nix lane used on main and for Nix-related changes
```

Automation, agents, and other shells direnv never touches use the wrapper
instead, which enters the correct devshell from any state (and is a free
passthrough when one is already active):

```sh
./scripts/dev just ci
```

See [`AGENTS.md`](AGENTS.md) for the details, particularly around git
worktrees.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
