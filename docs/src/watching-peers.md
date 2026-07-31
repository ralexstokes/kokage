# Watching Peers

Supervisors *restart* failed actors. But sometimes a peer needs to *react*
to a collaborator's lifecycle in its own logic: pause accepting orders while
the press is down, re-run a handshake when a connection actor comes back,
fail over to a different venue when a feed dies. That is what **monitors**
are for.

## Watching from inside an actor

[`Context::watch`] subscribes the current actor to another actor's lifecycle.
Events arrive as *ordinary messages* — you provide a mapping from
[`MonitorEvent`] into your own message type, and handle them in `handle`
like everything else:

```rust
use kokage::prelude::*;
use tokio::sync::mpsc;

struct Press;

impl Actor for Press {
    type Msg = String;

    async fn handle(&mut self, job: String, _ctx: &mut Context<'_, Self>) -> ExitResult {
        if job == "jam" {
            return Err(std::io::Error::other("paper jam").into());
        }
        println!("printed: {job}");
        Ok(())
    }
}

enum DeskMsg {
    Press(MonitorEvent),
}

struct FrontDesk {
    press: ActorRef<String>,
    log: mpsc::UnboundedSender<String>,
}

impl Actor for FrontDesk {
    type Msg = DeskMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.watch(&self.press, DeskMsg::Press).detach();
        Ok(())
    }

    async fn handle(&mut self, DeskMsg::Press(event): DeskMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let line = match event {
            MonitorEvent::Started { generation, .. } => format!("press up (run {generation})"),
            MonitorEvent::Exited { status, .. } if status.is_failure() => "press down: failure".to_owned(),
            MonitorEvent::Exited { .. } => "press stopped".to_owned(),
            MonitorEvent::Removed { .. } => "press permanently gone".to_owned(),
            MonitorEvent::Lagged { dropped, .. } => format!("missed {dropped} press events"),
            _ => "unrecognized press event".to_owned(),
        };
        self.log.send(line).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (log_tx, mut log_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let press = tree.add_actor("press", || Press);
    let watched = press.clone();
    tree.add_actor("front-desk", move || FrontDesk {
        press: watched.clone(),
        log: log_tx.clone(),
    });
    let runtime = tree.spawn()?;

    println!("{}", log_rx.recv().await.expect("event")); // press up (run 0)
    press.send("jam".to_owned()).await?;
    println!("{}", log_rx.recv().await.expect("event")); // press down: failure
    println!("{}", log_rx.recv().await.expect("event")); // press up (run 1)

    runtime.shutdown_and_wait().await?;
    Ok(())
}
```

## The watch contract

The semantics are tuned so that watching composes with supervision instead
of fighting it:

- **A watch follows the membership, not one run.** Restarts of the target
  produce `Exited` / `Started` pairs on the *same* watch; you are never left
  holding a subscription to a dead incarnation. The watch also survives
  restarts of the *watching* actor's target binding — and if the target is
  already running when you watch, you get an immediate `Started`, so there
  is no startup race to code around.
- **Events use your mailbox.** Ordering is your ordinary message ordering,
  and a watching actor that falls behind sees a `Lagged { dropped, .. }`
  marker rather than unbounded buffering.
- **`Removed` is terminal and guaranteed.** When the target's membership
  leaves the tree permanently, every watcher hears about it, even ones that
  were lagging.
- **The [`Guard`] owns the watch.** Dropping it unsubscribes; `.detach()`
  keeps the watch for the actor's lifetime. Re-watching the same target is
  an alias, not a duplicate subscription.

`Exited` carries the shared observational [`ExitStatus`]: completed, failed,
panicked, or controller-aborted, together with whether shutdown was requested.
It is the same vocabulary snapshots and lifecycle streams use; the original
application error remains producer-side.

## Three lifecycle tools, three audiences

It is worth keeping the trio straight:

- **Supervision** (restart policies) decides what happens *to* a failed
  actor. It is policy, not code.
- **Monitors** (this chapter) let a *peer actor* react in its domain logic,
  through its own typed mailbox.
- **Lifecycle streams** (see [Observability](observability.md)) give
  *operators* a recursive, tree-wide feed with scope paths, sequence numbers,
  and restart counters — the audit-log projection of the same underlying
  events.

If you find an actor subscribing to lifecycle events in order to *re-create*
a collaborator, stop and reshape the tree instead: restarting is the
supervisor's job. Monitors are for reacting, not supervising.

[`Context::watch`]: https://stokes.io/kokage/api/kokage/struct.Context.html#method.watch
[`MonitorEvent`]: https://stokes.io/kokage/api/kokage/enum.MonitorEvent.html
[`Guard`]: https://stokes.io/kokage/api/kokage/struct.Guard.html
[`ExitStatus`]: https://stokes.io/kokage/api/kokage/enum.ExitStatus.html
