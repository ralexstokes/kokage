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
imports from the crate root. Plain async tasks can live beside actors through
`kokage::host::ChildSpec`; the next chapter uses task children to explain
restart, shutdown, and strategy policies without changing the tree/runtime
vocabulary.

## Your first actor

An actor owns state and handles one typed message at a time. Register its
factory with a [`GraphBuilder`] and use the builder's flat one-for-one
[`spawn`](https://stokes.io/kokage/api/kokage/struct.GraphBuilder.html#method.spawn)
convenience when no custom tree shape is needed. Keep the returned [`Runtime`]
alive:

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
    let greeter = graph.actor(ActorSpec::new("greeter", || Greeter));

    let runtime = graph.spawn()?;
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

## One tree for actors and tasks

Every actor runs as a supervised task, but actor applications normally express
topology with `OrderedTree` / `DynamicTree` and control it with `Runtime` /
`RuntimeHandle`. Those trees also accept plain futures as
`kokage::host::ChildSpec` task children, so mixed applications retain the same
topology, ownership, control, and observation model. The next chapter explains
the policies on plain tasks before the later chapters apply them to actors.

[`GraphBuilder`]: https://stokes.io/kokage/api/kokage/struct.GraphBuilder.html
[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`Runtime`]: https://stokes.io/kokage/api/kokage/struct.Runtime.html
[`RuntimeHandle`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html
