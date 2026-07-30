# Task children and supervision

Actors are the usual unit of a kokage application, but a supervision tree can
also host plain async tasks. Define those tasks with [`host::ChildSpec`], place
them in an [`OrderedTree`] or [`DynamicTree`], and control the result through
the same [`RunningTree`] and [`ScopeRef`] used for actor trees.

This example supervises a `front-desk` task that should run forever and a
`press` task that keeps jamming:

```rust,no_run
use std::time::Duration;

use kokage::{
    Backoff, OrderedTree, Restart, Shutdown,
    host::ChildSpec,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let press_restart = Restart::on_failure()
        .limit(3, Duration::from_secs(10))
        .backoff(Backoff::fixed(Duration::from_millis(100)));

    // A press that jams shortly after starting.
    let press = ChildSpec::task("press", |ctx| async move {
        println!("press starting (generation {})", ctx.generation());
        tokio::time::sleep(Duration::from_millis(200)).await;
        Err("paper jam".into())
    })
    .restart(press_restart)
    .shutdown(Shutdown::drain_for(Duration::from_secs(1)));

    // A front desk that runs until its tree asks it to stop.
    let front_desk = ChildSpec::task("front-desk", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(Restart::always());

    let runtime = OrderedTree::new()
        .task(press)
        .task(front_desk)
        .spawn()?;

    match runtime.wait().await {
        Ok(()) => println!("runtime stopped cleanly"),
        Err(error) => println!("runtime gave up: {error}"),
    }
    Ok(())
}
```

The press starts four generations, each separated by 100 ms, before its
restart budget is exhausted. That failure stops the root runtime and is
returned from `RunningTree::wait`:

```text
press starting (generation 0)
press starting (generation 1)
press starting (generation 2)
press starting (generation 3)
runtime gave up: restart intensity exceeded
```

The important boundary is ownership: the tree is single-use configuration,
`OrderedTree` and `DynamicTree` are single-use declarations. Spawning either
produces a `RunningTree` owner, while `RunningTree::scope()` returns the root
`ScopeRef` used for control and observation; nested lookups return more
`ScopeRef` values. Pass those cheaply cloned references to components just as
`ActorRef` addresses an actor. The usual pattern for repeated root operations
is `let root = running.scope();`. A `ScopeRef` does not keep its `RunningTree`
alive: dropping the reference is inert, while dropping the owner initiates
graceful shutdown. Plain tasks do not introduce a second application lifecycle
model.

## Restart policies

Each child has a [`Restart`] that decides whether an exit triggers a
restart:

- **`Restart::always()`** always restarts, even after a clean `Ok(())`
  exit. It suits a service that should never stop, such as the front desk.
- **`Restart::on_failure()`** (the default) restarts after an error, panic,
  or abort, but treats a clean exit as final. It suits the press: a jam should
  be retried, but deliberately finishing should be final.
- **`Restart::never()`** runs at most once. It suits one-shot startup or
  batch work.

Unbounded restarting would turn a persistent fault into a busy loop, so
`Restart::limit` budgets a declaration to at most `max_restarts` within a
sliding `within` window. Exceeding the budget fails the scope and escalates to
its parent.

A [`Backoff`] can delay attempts with a fixed or exponential schedule. It is a
data enum, so code that needs to inspect a declaration can match
`Backoff::None`, `Backoff::Fixed(delay)`, or
`Backoff::Exponential { base, factor, max, jitter }` directly. Because the enum
is `#[non_exhaustive]`, downstream matches also need a catch-all arm. The
convenience constructors remain the recommended spelling when declaring a
policy.
The exponential attempt count is tracked per child and resets after a run
survives longer than the intensity window. Shutdown always wins over a pending
restart delay.

Call `OrderedTree::default_restart` or `DynamicTree::default_restart` to set a
scope-wide declaration, and `ChildSpec::restart` when one task needs its own
mode, budget, backoff, or terminal-removal behavior. To configure the parent
edge of a nested scope, wrap it with
`TreeNode::from(subtree).restart(policy).shutdown(policy)` before passing it to
`OrderedTree::subtree` or `ScopeRef::add_subtree`. The nested
tree's own defaults still configure its children independently.

## Ordered startup and readiness

`OrderedTree` starts declared children one at a time. A task that opts into a
readiness gate holds later siblings until it calls `mark_ready`:

```rust,ignore
let database = ChildSpec::task("database", |ctx| async move {
    connect_and_migrate().await?;
    ctx.mark_ready();
    ctx.shutdown_token().cancelled().await;
    Ok(())
})
.wait_for_ready();

let runtime = OrderedTree::new()
    .task(database)
    .task(api)
    .spawn()?;

runtime.scope().wait_started().await?;
```

The API task is not spawned until the database reports readiness. Plain tasks
without `wait_for_ready` count as ready immediately. Actor children are gated
automatically: their `on_start` hook is the readiness boundary. Use
`Context::continue_with(message)` in `on_start` to queue expensive
follow-up work as the actor's next message without delaying later siblings.

Ordered membership is immutable after spawn. `ScopeRef::kind()` reports that
capability, and membership operations on an ordered scope return
`ControlError::NotDynamic`. Use `DynamicTree` when members should start
independently and be added at runtime. There is no implicit readiness timeout.

## Strategies

An ordered tree's [`Strategy`] selects which siblings are affected by a child
failure:

- **`Strategy::OneForOne`** (the default) restarts only the failed child.
- **`Strategy::OneForAll`** stops and restarts every eligible child together.
  It suits children with interdependent state.
- **`Strategy::RestForOne`** restarts the failed child and every later child
  in declaration order. Earlier children keep running, which suits ordered
  pipelines.

Select it directly on the tree:

```rust,ignore
let runtime = OrderedTree::new()
    .strategy(Strategy::RestForOne)
    .task(outbound)
    .task(progress)
    .task(inbound)
    .spawn()?;
```

For `RestForOne`, declaration order is part of the fault model. An `inbound`
failure in this example restarts only `inbound`; the last child has no later
siblings. Put a bridge before the delivery pair if the pair must restart with
it, or use `OneForAll` if every member must share fate. The runnable
[`agent_control` example] pins this behavior with per-child restart counts.

## Shutdown policies

When a task must stop because its runtime is shutting down, its membership is
removed, or its group is restarting, [`Shutdown`] governs how:

- **`Shutdown::drain_for(grace)`** cancels the child's shutdown token and waits
  for a voluntary exit. Actor children drain accepted messages during the
  grace; for plain task children, the drain distinction is inert.
- **`Shutdown::discard_after_current(grace)`** uses the same cooperative grace,
  while actor children finish only an in-flight message and discard the queued
  remainder.
- **`Shutdown::abort()`** aborts immediately.

After either cooperative grace expires, the child is aborted and shutdown or
removal reports a timeout.

Tokio aborts take effect at `.await` points. A non-yielding loop cannot be
preempted, so put truly blocking work on a blocking pool or in an external
process. Actor code can use `Context::run_blocking` for this boundary.

Ordered trees stop children in reverse declaration order and give each child
its complete grace, so the worst-case grace budget is their sum. Dynamic
scopes cancel siblings together and use the longest single grace as their
overall budget.

A supervised actor has one user-facing shutdown declaration. `Shutdown` is a
data enum: `Drain { grace }`, `Discard { grace }`, or `Abort`; the abort variant
carries no synthetic zero grace. Because the enum is `#[non_exhaustive]`,
downstream matches also need a catch-all arm. The cooperative variants' grace
bounds queued messages, outstanding offloads, and `on_stop`, and the variant
decides whether queued messages are drained or discarded. Offload deadlines
remain independent bounds on individual offloads; they do not extend the
child grace. A host running an actor outside a tree passes the same `Shutdown`
value to `RunnableActor::run_until`; a conventional standalone declaration is
`Shutdown::drain_for(`[`host::DEFAULT_SHUTDOWN_BOUND`]`)`.

### One shutdown clock per child

The cooperative grace bounds the complete actor shutdown. When it expires, the
supervisor records `ChildExitView::Aborted { after_grace: true }`, asks the
actor wrapper to terminate its mailbox and publish final observability, then
hard-aborts the wrapper if that short accounting step does not finish. A root
shutdown or dynamic removal returns `SupervisorError::ShutdownTimedOut`; a
group restart records the same exit shape and continues only after the old
generation has terminated.

Every child owns its clock. Ordered scopes stop in reverse declaration order,
so their worst-case grace is cumulative. Dynamic scopes start the clocks
together, and a short-grace child cannot borrow a longer sibling's budget.

## Stopping when finite work completes

Pipeline and batch trees often have a natural completion point. Arm
`completions(ids).then_shutdown()` on a pre-spawn scope so even a task that finishes
immediately is observed:

```rust,ignore
let tree = OrderedTree::new()
    .task(source)
    .task(indexer.restart(Restart::never()))
    .task(metrics_reporter);

let handle = tree.scope();
// Detach for fire-and-forget; retain the guard instead to keep the
// option of cancelling the watch (dropping it cancels too).
handle
    .completions(["source", "indexer"])
    .then_shutdown()
    .detach();
let runtime = tree.spawn()?;
runtime.wait().await?;
```

The scope stops when every named child is simultaneously completed. A child
counts as completed when its current generation returns `Ok(())` on its own
and no restart is pending. Failure still follows its restart policy, and an
unknown id is rejected. Call `allow_future_members()` explicitly when an id
may be added later to a dynamic scope; ordered scopes report
`CompletionError::NotDynamic`. Use `completions(ids).wait()` to await the same
condition without automatically stopping the runtime.

- Failures still follow the normal restart policy. A `Restart::never()` child that fails
  can never complete, and the scope runs until explicitly stopped.
- A later start un-completes a child, so one cancelled as part of a
  sibling-driven `OneForAll` or `RestForOne` restart must complete again on its
  new generation.
- A child cancelled by shutdown, removal, or a group restart can still return
  `Ok(())`. That is not finished work, and it does not count.
  [`LifecycleEventKind::ChildExited`] carries a `ChildExitView` whose
  `cancelled()` method reports it.

## Nested scopes

Nested scopes need nothing special: a scope that stops itself this way is
observed by its parent as an ordinary clean child exit, so a parent can name it
in its own completion set. Completion watches are strict by default: absent ids
return `CompletionError::UnknownChild`. On a dynamic scope,
`allow_future_members()` keeps those ids pending until their memberships are
added.

## Supervision trees

Subtrees give each subsystem its own restart budget while preserving one
runtime hierarchy:

```rust,ignore
let pressroom = OrderedTree::new()
    .default_restart(pressroom_restart)
    .task(press);

let shop = OrderedTree::new()
    .subtree("pressroom", pressroom)
    .task(front_desk);

let runtime = shop.spawn()?;
```

The subtree forwards lifecycle events and appears in the root snapshot, so
[Observability](observability.md) sees the whole tree. Executable declarations
can also be projected to data; see [Inspectable supervision
trees](supervision-trees.md) when you need to inspect, compare, or assemble
that shape.

## Dynamic task children

`DynamicTree` starts empty. Obtain its scope before spawn or from the runtime
afterward, then add and remove `ChildSpec` tasks:

```rust,ignore
let runtime = DynamicTree::new().spawn()?;
let dynamic = runtime.scope();

let lineage = dynamic
    .add_child(ChildSpec::task("night-shift-press", factory))
    .await?;
dynamic.remove_child("night-shift-press").await?;
```

`add_child` returns the lineage allocated to that membership. The value is
also published in snapshots and distinguishes a removed child from a later
same-id replacement. Task children remain visible through runtime snapshots
and lifecycle watches, but do not appear in actor message statistics.

For dynamic actor construction and actor-owned scopes, continue to [Dynamic
actors](dynamic-actors.md).

[`host::ChildSpec`]: https://stokes.io/kokage/api/kokage/host/struct.ChildSpec.html
[`OrderedTree`]: https://stokes.io/kokage/api/kokage/struct.OrderedTree.html
[`DynamicTree`]: https://stokes.io/kokage/api/kokage/struct.DynamicTree.html
[`RunningTree`]: https://stokes.io/kokage/api/kokage/struct.RunningTree.html
[`ScopeRef`]: https://stokes.io/kokage/api/kokage/struct.ScopeRef.html
[`Restart`]: https://stokes.io/kokage/api/kokage/struct.Restart.html
[`Backoff`]: https://stokes.io/kokage/api/kokage/enum.Backoff.html
[`Strategy`]: https://stokes.io/kokage/api/kokage/enum.Strategy.html
[`Shutdown`]: https://stokes.io/kokage/api/kokage/enum.Shutdown.html
[`host::DEFAULT_SHUTDOWN_BOUND`]: https://stokes.io/kokage/api/kokage/host/constant.DEFAULT_SHUTDOWN_BOUND.html
[`LifecycleEventKind::ChildExited`]: https://stokes.io/kokage/api/kokage/observe/enum.LifecycleEventKind.html#variant.ChildExited
[`agent_control` example]: https://github.com/ralexstokes/kokage/tree/main/crates/kokage/examples/agent_control
