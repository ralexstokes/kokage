# Blocking Work and Offloads

An actor processes one message at a time — that is its superpower and its
constraint. A `handle` that awaits a slow supplier API for two seconds is an
actor that ignores its mailbox (and shutdown!) for two seconds. Kokage gives
you two context tools to keep the loop responsive: **offloads** for slow
futures, and **`run_blocking`** for synchronous, CPU- or filesystem-bound
work.

## Offloading a slow future

[`Context::offload`] runs a future in the background and delivers its result
*back to the actor as an ordinary message*. The actor stays free to handle
other traffic in between; state stays single-threaded because the result
re-enters through the mailbox:

```rust
use std::time::Duration;

use kokage::prelude::*;
use tokio::sync::mpsc;

enum DeskMsg {
    RestockPaper { reams: u32 },
    SupplierQuote(Result<u64, String>),
}

struct FrontDesk {
    quotes: mpsc::UnboundedSender<Result<u64, String>>,
}

impl Actor for FrontDesk {
    type Msg = DeskMsg;

    async fn handle(&mut self, msg: DeskMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match msg {
            DeskMsg::RestockPaper { reams } => {
                ctx.offload(
                    // deadline for the whole operation
                    Duration::from_secs(1),
                    // the slow work — pretend this calls the supplier's API
                    async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        u64::from(reams) * 700
                    },
                    // map the outcome (success *or* deadline) back into a message
                    |result| match result {
                        Ok(total) => DeskMsg::SupplierQuote(Ok(total)),
                        Err(_deadline) => {
                            DeskMsg::SupplierQuote(Err("supplier timed out".to_owned()))
                        }
                    },
                );
            }
            DeskMsg::SupplierQuote(quote) => {
                self.quotes.send(quote).expect("receiver alive");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (quotes_tx, mut quotes_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let desk = tree.add_actor("front-desk", move || FrontDesk {
        quotes: quotes_tx.clone(),
    });
    let running_tree = tree.spawn()?;

    desk.send(DeskMsg::RestockPaper { reams: 40 }).await?;
    println!("supplier says: {:?}", quotes_rx.recv().await.expect("quote"));

    running_tree.shutdown().await?;
    Ok(())
}
```

The shape to notice: the continuation is **total**. It receives
`Result<T, OffloadDeadline>` and must produce a message either way, so the
deadline case is handled in the protocol, by construction — there is no way
to forget it. This is why `offload` takes a deadline up front: an unbounded
background operation hiding inside an actor is exactly the kind of silent
liability supervision exists to eliminate.

The fine print:

- `offload` is owned by the actor incarnation automatically. Use
  `offload_scoped` when dropping a returned [`Guard`] should cancel the work,
  for example to tie one request to a state-machine phase.
- Offloads are **incarnation-owned**: if the actor fails or restarts, its
  outstanding offloads are aborted. A fresh run never receives results it
  doesn't remember requesting.
- Completions bypass mailbox capacity and conflation, so a full mailbox
  cannot deadlock an actor against its own pending results.
- A panic in the offloaded future resurfaces on the actor — it becomes an
  ordinary actor failure, handled by the supervisor.

Contrast this with `tokio::spawn` from inside `handle`, which produces an
unsupervised, unbounded, uncancelled orphan whose panics vanish. Offload is
the supervised version of that instinct.

## Synchronous work: `run_blocking`

For work that *blocks a thread* — hashing a file, zipping an archive, a
synchronous database driver — [`Context::run_blocking`] moves a closure onto
the blocking thread pool:

```rust
# use kokage::prelude::*;
# struct Archivist;
# impl Actor for Archivist {
#     type Msg = String;
async fn handle(&mut self, path: String, ctx: &mut Context<'_, Self>) -> ExitResult {
    let bytes = ctx.run_blocking(move |_cancel| std::fs::read(path)).await??;
    println!("archived {} bytes", bytes.len());
    Ok(())
}
# }
```

The closure receives a [`CancellationToken`] (a child of the actor's shutdown
token) so long computations can check for cancellation at convenient points.
The double `?` unpacks two layers: the outer
`Result<_, BlockingCancelled>` — an error only when the runtime shut down
before the queued work ever ran — and whatever `Result` your closure itself
returned. As with offloads, a panic in the closure resurfaces on the actor.

Awaiting `run_blocking` inline (as above) is fine for quick operations, but
it does pause message processing while it waits. For long blocking jobs,
combine the tools: `run_blocking` returns a `'static` future, so you can
hand it to `offload` and get the result delivered as a message with a
deadline, keeping the actor fully responsive.

[`Context::offload`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.offload
[`Context::offload_scoped`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.offload_scoped
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
[`Context::run_blocking`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.run_blocking
[`CancellationToken`]: https://stokes.io/kokage/api/kokage/struct.CancellationToken.html
