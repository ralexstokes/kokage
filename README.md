# kokage 木陰

> actors in the shade of a supervision tree

OTP-style supervision trees and typed actors — a thin layer over an async
scheduler (Tokio today).

The core idea is the one that has kept telecom switches running for decades:
**let it crash**. Instead of defensively handling every failure in place, you
organize your program into small, isolated tasks and let a *supervisor*
restart the ones that fail.

Kokage combines declared and runtime-mutated supervision trees, typed actor
mailboxes and request/reply, supervised async tasks, restart and shutdown
policies, and gap-aware lifecycle observation in one crate.

Add Kokage alongside the Tokio runtime that drives it. Its prelude covers the
common actor, task, and tree surface; raw actor execution and observation APIs
are grouped under `kokage::raw` and `kokage::observe`, while less common types
remain at the crate root.

```toml
[dependencies]
kokage = { git = "https://github.com/ralexstokes/kokage" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
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

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        println!("printing {order}");
        Ok(())
    }
}

struct FrontDesk {
    press: ActorRef<String>,
}

impl Actor for FrontDesk {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.press.send(order).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Add actors in startup order and retain their typed refs.
    let mut tree = Tree::new();
    let press = tree.add_actor("press", Press::default);
    let orders = tree.add_actor("front-desk", move || FrontDesk {
        press: press.clone(),
    });

    // Compose the supervision tree, then run it.
    let runtime = tree.spawn()?;

    orders.send("business cards x100".to_owned()).await?;

    runtime.shutdown().await?;
    Ok(())
}
```

The full runnable version is
[`crates/kokage/examples/supervised_actors.rs`](crates/kokage/examples/supervised_actors.rs).

## The API shape

`Tree` declares ordered membership up front. `DynamicTree` starts empty and
exposes runtime membership for actors, tasks, jobs, or subtrees. Their
`spawn()` methods return `RunningTree` and `RunningDynamicTree`, respectively.
Keep that owner alive for as long as the application should run: dropping it
requests graceful shutdown, so a discarded `let _ = tree.spawn()?;` shuts down
immediately.

`runtime.scope()` returns a cheap, cloneable, non-owning reference, parallel to
an `ActorRef`. `ScopeRef` is the common observation and control surface:
snapshots, self-resynchronizing `changes()`, subtree traversal, and either
non-waiting `request_shutdown()` or waiting `shutdown().await`. Dynamic scopes
return `DynamicScopeRef`, which adds membership operations such as `add_actor`,
`add_task`, `spawn_job`, and `remove_child`. A scope found through untyped tree
traversal can request that capability with `scope.dynamic()`.

The common actor operations own their natural lifetimes:

- `watch` is owned by the two restart-stable actor memberships and follows
  both actors across restarts.
- `offload` is owned by the current actor incarnation and is aborted on stop or
  restart so stale work cannot reach a replacement.

Use `watch_scoped` or `offload_scoped` when a narrower lifetime needs a
cancel-on-drop `Guard`. Timers and lifecycle pumps likewise return guards;
retain one for scoped ownership or call `.detach()` for intentional
fire-and-forget work.

The escape hatches stay next to the concise paths: `Reply::channel()` separates
request-acceptance and response deadlines beneath `ActorRef::call`;
`raw::RawActor` provides a custom receive loop beneath `Actor`; and
`ScopeRef::observe_children` / `lifecycle_events` expose lower-level lifecycle
streams beneath `changes()`.

## The crates

`kokage` is the product: typed actors, raw task children, and the runtime that
supervises both in one crate. The implementation layer stays private so trees
remain the only construction front door.

| Crate | Role |
|-------|------|
| [`kokage`](crates/kokage) | The front door: communicating actors with typed mailboxes, raw task children, restart-stable handles, restart policies and strategies, graceful shutdown, and single-use ordered or dynamic supervision trees. |
| [`kokage-derive`](crates/kokage-derive) | `#[derive(ActorFactory)]` for reusable incarnation factories; re-exported by `kokage` under the opt-in `derive` feature. |
| [`kokage-console`](crates/kokage-console) | *(experimental, git-only)* A live web dashboard for watching a running supervision tree. It is kept outside the published `kokage` feature and dependency surface. |

## Getting started

- **Tutorial book** — builds a small fault-tolerant service from scratch,
  from actor basics through task and actor supervision, dynamic actors, and observability.
  Start at [`docs/src/introduction.md`](docs/src/introduction.md), or run
  `just serve-book` for a local copy.
- **API docs** — `just doc` builds and opens the rustdoc for the workspace.
- **Examples** — runnable examples live under each package's `examples/`
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
