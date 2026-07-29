# Getting started

## Dependencies

The crates are not yet published to crates.io, so use a git dependency (or a
path dependency if you are working inside this repository). `kokage` is the
one dependency needed for actor applications:

```toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
kokage = { git = "https://github.com/ralexstokes/kokage" }
```

`kokage::prelude` covers the day-one actor traits, contexts, graph builder, and
ordered runtime. Advanced policies and dynamic membership remain explicit
imports from the crate root. Applications that supervise plain async tasks
without actors can instead depend directly on `kokage-supervisor`; the next
chapter takes that optional one-level-deeper tour.

## Your first actor

An actor owns state and handles one typed message at a time. Register its
factory with a [`GraphBuilder`], move the completed graph into an
[`OrderedTree`], and keep the returned [`Runtime`] alive:

```rust,no_run
use kokage::prelude::*;

struct Greeter;

impl Actor for Greeter {
    type Msg = String;

    async fn handle(
        &mut self,
        name: String,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        println!("hello, {name}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphBuilder::new();
    let greeter = graph.actor("greeter", || Greeter);

    let runtime = OrderedTree::graph(graph.build()?).spawn()?;
    greeter.send("print shop".to_owned()).await?;

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

A few details establish the model used throughout the book:

- `Greeter` is ordinary Rust state. Its factory constructs fresh state for
  every supervised restart.
- `ActorRef<String>` is the typed, restart-stable address returned during
  graph wiring. Senders keep using the same ref if the actor is replaced.
- `ActorResult` reports whether a callback completed or failed. Restart policy
  belongs to the supervision tree, not to the actor.
- `Runtime` owns the spawned tree. Clone `runtime.handle()` when another
  component needs a non-owning [`RuntimeHandle`]; dropping handles has no
  lifecycle effect, while dropping the owner requests graceful shutdown.

The example shuts down explicitly so it can await the result. A discarded
`let _ = tree.spawn()?;` drops the owner at the end of the statement and asks
the runtime to stop immediately.

## The layer underneath

Every actor runs as a supervised task, but actor applications normally express
topology with `OrderedTree` / `DynamicTree` and control it with `Runtime` /
`RuntimeHandle`. The independent `kokage-supervisor` crate exposes the lower
layer directly as `Supervisor` / `RunningSupervisor` / `SupervisorHandle` for
programs whose children are plain futures rather than actors.

The next chapter uses that lower layer to explain restart, shutdown, and
strategy semantics once. The actor chapters then apply the same policies
through the actor-facing tree and runtime vocabulary.

[`GraphBuilder`]: https://stokes.io/kokage/api/kokage/struct.GraphBuilder.html
[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`Runtime`]: https://stokes.io/kokage/api/kokage/struct.Runtime.html
[`RuntimeHandle`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html
