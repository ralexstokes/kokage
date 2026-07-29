# Task children and supervision

Actors are the usual unit of a kokage application, but a supervision tree can
also host plain async tasks. Define those tasks with [`host::ChildSpec`], place
them in an [`OrderedTree`] or [`DynamicTree`], and control the result through
the same [`Runtime`] and [`RuntimeHandle`] used for actor trees.

This example supervises a `front-desk` task that should run forever and a
`press` task that keeps jamming:

```rust,no_run
use std::time::Duration;

use kokage::{
    BackoffPolicy, OrderedTree, RestartConfig, RestartPolicy, ShutdownPolicy,
    host::ChildSpec,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let press_restart = RestartConfig::new(3, Duration::from_secs(10))
        .backoff(BackoffPolicy::Fixed(Duration::from_millis(100)));

    // A press that jams shortly after starting.
    let press = ChildSpec::task("press", |ctx| async move {
        println!("press starting (generation {})", ctx.generation());
        tokio::time::sleep(Duration::from_millis(200)).await;
        Err("paper jam".into())
    })
    .restart_config(press_restart)
    .shutdown(ShutdownPolicy::Cooperative {
        grace: Duration::from_secs(1),
    });

    // A front desk that runs until its tree asks it to stop.
    let front_desk = ChildSpec::task("front-desk", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartPolicy::Always);

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
returned from `Runtime::wait`:

```text
press starting (generation 0)
press starting (generation 1)
press starting (generation 2)
press starting (generation 3)
runtime gave up: restart intensity exceeded
```

The important boundary is ownership: the tree is single-use configuration,
`Runtime` owns the live root, and a cloned `RuntimeHandle` is non-owning
control and observation access. Plain tasks do not introduce a second
application lifecycle model.

## Restart policies

Each child has a [`RestartPolicy`] that decides whether an exit triggers a
restart:

- **`RestartPolicy::Always`** always restarts, even after a clean `Ok(())`
  exit. It suits a service that should never stop, such as the front desk.
- **`RestartPolicy::OnFailure`** (the default) restarts after an error, panic,
  or abort, but treats a clean exit as final. It suits the press: a jam should
  be retried, but deliberately finishing should be final.
- **`RestartPolicy::Never`** runs at most once. It suits one-shot startup or
  batch work.

Unbounded restarting would turn a persistent fault into a busy loop, so
restarts are budgeted by a [`RestartConfig`]: at most `max_restarts` within a
sliding `within` window. Exceeding the budget fails the scope and escalates to
its parent.

A [`BackoffPolicy`] can delay attempts with a fixed or exponential schedule.
The exponential attempt count is tracked per child and resets after a run
survives longer than the intensity window. Shutdown always wins over a pending
restart delay.

Call `OrderedTree::restart_config` or `DynamicTree::restart_config` to set the
scope-wide budget, and `ChildSpec::restart_config` when one task needs its own
budget.

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

runtime.handle().wait_started().await?;
```

The API task is not spawned until the database reports readiness. Plain tasks
without `wait_for_ready` count as ready immediately. Actor children are gated
automatically: their `on_start` hook is the readiness boundary. Use
`LiveContext::continue_with(message)` in `on_start` to queue expensive
follow-up work as the actor's next message without delaying later siblings.

Ordered membership is immutable after spawn, so `RuntimeHandle::dynamic`
returns `None`. Use `DynamicTree` when members should start independently and
be added at runtime. There is no implicit readiness timeout.

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
removed, or its group is restarting, [`ShutdownPolicy`] governs how:

- **`ShutdownPolicy::Cooperative { grace }`** cancels the task's shutdown
  token and waits for a voluntary exit. After `grace`, the task is aborted and
  the shutdown or removal reports a timeout.
- **`ShutdownPolicy::Abort`** aborts immediately.

Tokio aborts take effect at `.await` points. A non-yielding loop cannot be
preempted, so put truly blocking work on a blocking pool or in an external
process. Actor code can use `LiveContext::run_blocking` for this boundary.

Ordered trees stop children in reverse declaration order and give each child
its complete grace, so the worst-case grace budget is their sum. Dynamic
scopes cancel siblings together and use the longest single grace as their
overall budget.

A supervised actor also has one user-facing shutdown deadline: its child
`ShutdownPolicy` grace bounds queued messages, outstanding offloads, and
`on_stop`. Offload deadlines remain independent bounds on individual offloads;
they do not extend the child grace. A host running an actor outside a tree
passes the equivalent explicit bound to `RunnableActor::run_until`; the
recommended standalone value is [`host::DEFAULT_SHUTDOWN_BOUND`].

### One shutdown clock per child

The cooperative grace bounds the complete actor drain. When it expires, the
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
[`shutdown_on_completion`] on a pre-spawn handle so even a task that finishes
immediately is observed:

```rust,ignore
let tree = OrderedTree::new()
    .task(source)
    .task(indexer.restart(RestartPolicy::Never))
    .task(metrics_reporter);

let handle = tree.handle();
// Retain the guard: dropping it cancels the watch.
let _finished = handle.shutdown_on_completion(["source", "indexer"]);
let runtime = tree.spawn()?;
runtime.wait().await?;
```

The scope stops when every named child is simultaneously completed. A child
counts as completed when its current generation returns `Ok(())` on its own
and no restart is pending. Failure still follows its restart policy, and an id
not yet present in a dynamic scope stays pending until added. Use
[`wait_completed`] to await the same condition without automatically stopping
the runtime.

- Failures still follow the normal restart policy. A `Never` child that fails
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
in its own completion set. For dynamic scopes whose ids are not members yet,
use `wait_completed_dynamic` or `shutdown_on_dynamic_completion`; those names
make the future-membership behavior explicit.

## Supervision trees

Subtrees give each subsystem its own restart budget while preserving one
runtime hierarchy:

```rust,ignore
let pressroom = OrderedTree::new()
    .restart_config(pressroom_restart)
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

`DynamicTree` starts empty. Obtain its membership capability from the runtime
handle, then add and remove `ChildSpec` tasks:

```rust,ignore
let runtime = DynamicTree::new().spawn()?;
let dynamic = runtime.handle();

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
[`Runtime`]: https://stokes.io/kokage/api/kokage/struct.Runtime.html
[`RuntimeHandle`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html
[`RestartPolicy`]: https://stokes.io/kokage/api/kokage/enum.RestartPolicy.html
[`RestartConfig`]: https://stokes.io/kokage/api/kokage/struct.RestartConfig.html
[`BackoffPolicy`]: https://stokes.io/kokage/api/kokage/enum.BackoffPolicy.html
[`Strategy`]: https://stokes.io/kokage/api/kokage/enum.Strategy.html
[`ShutdownPolicy`]: https://stokes.io/kokage/api/kokage/enum.ShutdownPolicy.html
[`host::DEFAULT_SHUTDOWN_BOUND`]: https://stokes.io/kokage/api/kokage/host/constant.DEFAULT_SHUTDOWN_BOUND.html
[`shutdown_on_completion`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html#method.shutdown_on_completion
[`wait_completed`]: https://stokes.io/kokage/api/kokage/struct.RuntimeHandle.html#method.wait_completed
[`LifecycleEventKind::ChildExited`]: https://stokes.io/kokage/api/kokage/observe/enum.LifecycleEventKind.html#variant.ChildExited
[`agent_control` example]: https://github.com/ralexstokes/kokage/tree/main/crates/kokage/examples/agent_control
