# Getting started

## Dependencies

Add the actor crate:

```toml
[dependencies]
kokage = { git = "https://github.com/ralexstokes/kokage" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`kokage::prelude` covers the day-one actor traits, contexts, declarations,
typed refs, ordered trees, and snapshot types. Import advanced policies from
the crate root as needed.

## Your first actor

An actor implements `Actor`. An `ActorSpec` pairs its incarnation factory
with a scope-local id and exposes the stable typed ref before the declaration
moves into a tree.

```rust
use kokage::prelude::*;

struct Greeter;

impl Actor for Greeter {
    type Msg = String;

    async fn handle(
        &mut self,
        name: String,
        _ctx: &mut Context<'_, Self>,
    ) -> ActorResult {
        println!("hello, {name}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let greeter_actor = ActorSpec::new("greeter", || Greeter);
    let (greeter_actor, greeter) = greeter_actor.actor_ref();

    let runtime = OrderedTree::new().actor(greeter_actor).spawn()?;
    greeter.send("world".to_owned()).await?;
    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

The important boundaries are:

- the factory is retained and invoked again for every supervised restart;
- `ActorRef<M>` is cloneable and follows the logical actor across those
  incarnations;
- `ActorSpec<M>` and trees are single-use declarations with one runtime owner;
- actor ids are unique only within their immediate scope.

`spawn()` returns the owning `Runtime`. Dropping it requests graceful
shutdown, so do not discard it with `let _ = ...`. Use `runtime.handle()`
for non-owning control and observation.

## Supervision vocabulary

A **child** is one actor, task, or nested supervisor. A **strategy** selects
which siblings restart together. A **restart policy** decides whether a
particular exit is restartable. A **restart budget** prevents an endless crash
loop. A **shutdown policy** controls graceful-stop bounds.

`OrderedTree` composes static children recursively. `DynamicTree` is a
runtime membership boundary. Both produce the same runtime-handle and
observation model.

## One tree for actors and tasks

Actors and raw task children share a supervision tree. Place actors with
`OrderedTree::actor`, nested scopes with `subtree`, and task children with
`task`. The following chapters apply the same supervision vocabulary to
each kind.
