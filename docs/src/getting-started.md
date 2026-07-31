# Getting Started

Our print shop opens with a single machine: a press. In this chapter you will
define it as an actor, place it in a supervision tree, send it work, and shut
the shop down cleanly.

## Defining an actor

An actor is a value that owns some state and processes messages one at a time.
You define one by implementing the [`Actor`] trait:

```rust
use kokage::prelude::*;

struct Press {
    jobs_done: u64,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.jobs_done += 1;
        println!("printing: {job}");
        Ok(())
    }
}
```

Three things to notice:

- `type Msg` is the *only* message type this actor accepts. Everyone who
  talks to the press does so through an `ActorRef<String>`, and the compiler
  keeps them honest.
- `handle` is `async` and takes `&mut self`: the actor processes one message
  at a time, so it can mutate its state freely with no locks.
- The return type [`ExitResult`] is `Result<(), BoxError>`. Returning `Ok(())`
  means "ready for the next message". Returning an `Err` means the actor
  *failed* — the supervisor tears this run down and applies its restart
  policy. We put that to work in [Let It Crash](let-it-crash.md).

`handle` is the only required method. (There are optional lifecycle hooks —
`on_start` and `on_stop` — covered in
[Lifecycle and Timers](lifecycle-and-timers.md).)

## Declaring and spawning

An actor definition alone does not run anything. You *declare* an instance
by giving a tree an id and a **factory** — a closure that builds a fresh
actor value:

```rust
# use kokage::prelude::*;
# struct Press { jobs_done: u64 }
# impl Actor for Press {
#     type Msg = String;
#     async fn handle(&mut self, _job: String, _ctx: &mut Context<'_, Self>) -> ExitResult { Ok(()) }
# }
let mut tree = Tree::new();
let press = tree.add_actor("press", || Press { jobs_done: 0 });
# let _ = press;
```

The factory matters: it is called once for the first start *and once for every
restart*, so each incarnation begins from a clean state. Anything the actor
should keep across restarts must live outside it (or be re-derived in
`on_start`).

Declarations go into a [`Tree`], the ordinary ordered supervision tree.
`add_actor` returns the typed [`ActorRef`] for reaching the actor, and
`spawn` brings the whole tree to life:

```rust
use kokage::prelude::*;

struct Press {
    jobs_done: u64,
}

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.jobs_done += 1;
        println!("printing: {job}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tree = Tree::new();
    let press = tree.add_actor("press", || Press { jobs_done: 0 });
    let runtime = tree.spawn()?;

    press.send("100 business cards".to_owned()).await?;
    press.send("flyers, glossy".to_owned()).await?;

    runtime.shutdown().await?;
    Ok(())
}
```

Run it and the press prints both jobs before the program exits.

## Who owns what

That short `main` demonstrates the two handle types you will use constantly:

- **[`ActorRef`]** (`press` above) is a cheap, cloneable, *typed sender*.
  `send` waits for mailbox capacity, delivers the message, and — crucially —
  keeps working across supervised restarts of the actor. Clone it freely and
  hand copies to anyone who needs to talk to the press.
- **[`RunningTree`]** (`runtime` above) *owns* the spawned tree. Keep it
  alive for as long as the application should run: dropping it requests a
  graceful shutdown. In particular, `let _ = tree.spawn()?;` shuts the tree
  down immediately — bind it to a real name.

`shutdown().await` asks every child to stop and waits until they have. By
default each actor *drains*: it finishes the messages already in its mailbox
(within a 5-second grace period) before stopping, which is why both jobs
print. Shutdown policy is configurable per actor — see
[Ownership and Shutdown](ownership-and-shutdown.md).

## Where's the error handling?

`press.send(...).await?` can fail only if the press is permanently gone
(tree shut down, actor removed). It does not fail when the press is busy —
the call waits — nor when the press is mid-restart. That is the "shade" of
the supervision tree: callers do not track the lifecycle of their
collaborators; they hold a ref and send.

Next, let's make the conversation two-way.

[`Actor`]: https://stokes.io/kokage/api/kokage/trait.Actor.html
[`ExitResult`]: https://stokes.io/kokage/api/kokage/type.ExitResult.html
[`Tree`]: https://stokes.io/kokage/api/kokage/struct.Tree.html
[`ActorRef`]: https://stokes.io/kokage/api/kokage/struct.ActorRef.html
[`RunningTree`]: https://stokes.io/kokage/api/kokage/struct.RunningTree.html
