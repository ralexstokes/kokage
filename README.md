# tokio-otp

Erlang/OTP-style fault tolerance for the [`tokio`](https://tokio.rs) ecosystem:
supervision trees, typed actor graphs, and a runtime that composes the two.

The core idea is the one that has kept telecom switches running for decades:
**let it crash**. Instead of defensively handling every failure in place, you
organize your program into small, isolated tasks and let a *supervisor*
restart the ones that fail.

One dependency is all you need — `tokio-otp` exports everything through
its prelude:

```toml
[dependencies]
tokio-otp = { git = "https://github.com/ralexstokes/tokio-otp" }
```

## A taste

Each actor runs as its own supervised child. When the press crashes, the
supervisor restarts it — and the `orders` ref keeps working across the
restart, transparently reconnecting to the replacement:

```rust
use tokio_otp::prelude::*;

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.press.send(order).await?;
        Ok(Continue)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Wire a static graph with typed, restart-stable actor refs.
    let mut builder = GraphBuilder::new();
    let (press_slot, press_ref) = builder.slot::<String>("press", ActorOptions::new());
    let (orders_slot, orders) = builder.slot("front-desk", ActorOptions::new());
    builder.define(orders_slot, move || FrontDesk {
        press: press_ref.clone(),
    });
    builder.define(press_slot, Press::default); // an actor that occasionally jams
    let graph = builder.build()?;

    // Run every actor as its own supervised child.
    let runtime = Runtime::builder()
        .graph(graph)
        .strategy(Strategy::OneForOne)
        .build()?;
    let handle = runtime.spawn();

    orders.send("business cards x100".to_owned()).await?;

    handle.shutdown_and_wait().await?;
    Ok(())
}
```

The full runnable version is
[`crates/tokio-otp/examples/supervised_actors.rs`](crates/tokio-otp/examples/supervised_actors.rs).

## The crates

`tokio-otp` is the product: typed actor graphs plus the runtime that
supervises them, in one crate. `tokio-supervisor` underneath is deliberately
independent — it knows nothing about actors and is useful on its own for
supervising plain async tasks.

| Crate | Role |
|-------|------|
| [`tokio-otp`](crates/tokio-otp) | The front door: static graphs of communicating actors — typed mailboxes, restart-stable `ActorRef<M>` handles, request/reply, cooperative blocking work — with each actor running as its own supervised child under one integrated `Runtime` supporting dynamic actors and observability. |
| [`tokio-supervisor`](crates/tokio-supervisor) | Structured supervision of async tasks: restart policies (`permanent`/`transient`/`temporary`), restart intensity limits, `one_for_one`/`one_for_all` strategies, graceful shutdown, and nested supervision trees. |
| [`tokio-otp-derive`](crates/tokio-otp-derive) | `#[derive(ActorFactory)]` for reusable incarnation factories and `#[derive(Supervision)]` for cyclic actor graphs and their supervision scopes; re-exported by `tokio-otp` under the default `derive` feature. |
| [`tokio-otp-console`](crates/tokio-otp-console) | *(experimental, git-only)* A live web dashboard for watching a running supervision tree. It is kept outside the published `tokio-otp` feature and dependency surface. |

## Getting started

- **Tutorial book** — builds a small fault-tolerant service from scratch,
  from supervision fundamentals through dynamic actors and observability.
  Start at [`docs/src/introduction.md`](docs/src/introduction.md), or run
  `just serve-book` for a local copy.
- **API docs** — `just doc` builds and opens the rustdoc for the workspace.
- **Examples** — each crate ships runnable examples under its `examples/`
  directory, e.g. `cargo run -p tokio-otp --example supervised_actors`. Try
  the console locally with `cargo run -p tokio-otp-console --example console`.

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
