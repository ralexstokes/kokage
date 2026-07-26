# Supervision fundamentals

Time to open the print shop. In this chapter we stay in `tokio-supervisor`
land and supervise two plain tasks: a `front-desk` that should run forever,
and a `press` that keeps jamming. Along the way we meet every knob a
[`ChildSpec`] has.

```rust,no_run
use std::time::Duration;

use tokio_supervisor::{
    BackoffPolicy, ChildSpec, RestartPolicy, RestartIntensity, ShutdownPolicy, Strategy,
    SupervisorBuilder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A press that jams shortly after starting.
    let press = ChildSpec::new("press", |ctx| async move {
        println!("press starting (generation {})", ctx.generation());
        tokio::time::sleep(Duration::from_millis(200)).await;
        Err("paper jam".into())
    })
    .restart(RestartPolicy::OnFailure)
    .restart_intensity(
        RestartIntensity::new(3, Duration::from_secs(10))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(100))),
    )
    .shutdown(ShutdownPolicy::cooperative_then_abort(Duration::from_secs(1)));

    // A front desk that runs until asked to stop.
    let front_desk = ChildSpec::new("front-desk", |ctx| async move {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    })
    .restart(RestartPolicy::Always);

    let supervisor = SupervisorBuilder::new()
        .strategy(Strategy::OneForOne)
        .child(press)
        .child(front_desk)
        .build()?;

    let handle = supervisor.spawn();
    match handle.wait().await {
        Ok(()) => println!("supervisor stopped cleanly"),
        Err(error) => println!("supervisor gave up: {error}"),
    }
    Ok(())
}
```

Running this prints the press restarting three times, each generation 100 ms
apart, and then the supervisor giving up because the restart intensity limit
was exceeded:

```text
press starting (generation 0)
press starting (generation 1)
press starting (generation 2)
press starting (generation 3)
supervisor gave up: restart intensity exceeded
```

Let's unpack the policies that produced that behaviour.

## Restart policies

Each child has a [`RestartPolicy`] that decides whether an exit triggers a
restart:

- **`RestartPolicy::Always`** — always restart, even after a clean `Ok(())`
  exit. Right for services that should simply never stop, like the front
  desk.
- **`RestartPolicy::OnFailure`** (the default) — restart only on failure (`Err`,
  panic, or abort). A clean exit is final. Right for the press: a jam should
  be retried, but if the press decides it is done, it is done.
- **`RestartPolicy::Never`** — never restart. Runs at most once; useful for
  one-shot startup jobs.

## Restart intensity and backoff

Unbounded restarting would turn a persistent fault into a busy loop, so
restarts are budgeted by a [`RestartIntensity`]: at most `max_restarts`
restarts within a sliding `within` window (the default is 5 restarts within
30 seconds). Exceeding the budget makes the whole supervisor exit with
`SupervisorError::RestartIntensityExceeded` — in a supervision tree, that
escalates the failure to the parent.

A [`BackoffPolicy`] optionally delays each restart attempt: `Fixed`,
`Exponential`, or `JitteredExponential`. The exponential attempt count is a
per-child consecutive-restart counter that resets once a run survives longer
than the intensity window. A shutdown request always wins over a pending
restart delay.

Intensity can be set on the supervisor as a whole
(`SupervisorBuilder::restart_intensity`) or overridden per child, as we did
for the press.

## Ordered startup and readiness

`SupervisorBuilder` creates an ordered scope. It starts the declared child
sequence one at a time, waiting at each opted-in readiness signal:

```rust,ignore
let database = ChildSpec::new("database", |ctx| async move {
    connect_and_migrate().await?;
    ctx.mark_ready();
    ctx.shutdown_token().cancelled().await;
    Ok(())
})
.wait_for_ready();

let supervisor = SupervisorBuilder::new()
    .child(database)
    .child(api)
    .build()?;
```

The API child is not spawned until the database reports readiness. The same
ordering is used when `OneForAll` or `RestForOne` restarts multiple children.
Plain children without `wait_for_ready()` count as ready immediately. Actor
children are gated automatically: their `on_start` hook is the readiness
boundary. Use `ActorContext::continue_with(message)` inside `on_start` to queue
expensive follow-up work as the actor's next message without delaying later
siblings. Call `handle.wait_started().await` when code outside the tree needs
to wait until all current children are running. Ordered membership is immutable
at runtime, so add and remove operations return
`ControlError::UnsupportedByScopeKind`. Use `DynamicSupervisorBuilder` for an
empty runtime-written scope; its children start immediately. There is no
implicit readiness timeout.

Ordered startup latency is cumulative: the scope becomes ready after the sum
of its declared readiness gates' `on_start` times. This is now the default for
`SupervisorBuilder` and `Runtime::builder()`; use a dynamic scope when members
should start independently and immediate runtime insertion is the right
ownership model.

## Strategies

The [`Strategy`] decides who is affected when a child fails:

- **`Strategy::OneForOne`** (default) — only the failed child restarts. The
  front desk never notices the press jamming.
- **`Strategy::OneForAll`** — every child is stopped and restarted together.
  Use this when children hold interdependent state, e.g. a producer/consumer
  pair that must resynchronize from scratch. (`Never` children are drained
  with the group but not respawned.) Draining the old generation is an atomic
  critical section, so control commands wait behind it until every old task
  exits or reaches its shutdown backstop. Post-drain readiness gates are
  non-blocking loop state.
- **`Strategy::RestForOne`** — the failed child and every child declared after
  it are stopped, then eligible children in that suffix restart in declaration
  order. Earlier children remain running. Use this for ordered pipelines. Its
  old-suffix drain has the same bounded control-command blocking window as
  `OneForAll`.

For `RestForOne`, declaration order is therefore part of the fault model. If
children are declared as `outbound`, `progress`, `inbound`, an `inbound`
failure restarts only `inbound`: the last child has no later siblings. If a
failing bridge must take its delivery pair down with it, declare the bridge
first so the pair follows it, or use `OneForAll` when a failure in any member
must restart the whole group. Phase 3 of the runnable [`agent_control`
example] pins the last-child behavior with per-child restart-count assertions.

## Shutdown policies

When a child must stop — on supervisor shutdown, removal, or a group restart —
its [`ShutdownPolicy`] governs how:

- **`ShutdownPolicy::cooperative_strict(grace)`** — cancel the child's token
  and wait up to `grace` for a voluntary exit; abort *and report a timeout
  error* otherwise.
- **`ShutdownPolicy::cooperative_then_abort(grace)`** (default, 5 s grace) —
  same, but the fallback Tokio `abort()` is expected and not reported as an
  error.
- **`ShutdownPolicy::abort()`** — abort immediately.

One caveat inherited from Tokio itself: aborts take effect at `.await` points.
A child stuck in a non-yielding loop cannot be preempted — isolate truly
blocking work behind a blocking pool (as the actor layer's `run_blocking`
does, see the next chapter) or an external process. Ordered teardown advances
after issuing such an abort rather than waiting without a bound. A group
restart is stricter than a shutdown, because it has to respawn what it drained:
a child that is still running when the drain ends fails the restart, but only
after the whole drain group's longest grace has been spent waiting for it. An
abort of a nested supervisor wrapper hard-cascades through that subtree;
cooperative shutdown lets the subtree apply its own ordered or dynamic drain
first.

Ordered shutdown latency is also cumulative: each cooperative child receives
its own grace before the cursor moves to the previous declaration, so the
worst-case grace budget is their sum (with the default, up to 5 seconds × the
number of children). Dynamic scopes cancel siblings together and wait under
one shared maximum-grace deadline.

### Actor and supervisor deadlines

A supervised actor has two nested shutdown deadlines:

1. `GraphBuilder::actor_shutdown_timeout` belongs to the actor layer. Once the
   supervisor cancels the child token, `RunnableActor` passes cancellation to
   the inner actor task, waits for this timeout, and then aborts that inner task.
   An actor-layer timeout during requested shutdown completes the child cleanly
   with a `Cancelled` actor exit.
2. The child's `ShutdownPolicy` belongs to the supervisor layer. It waits for
   the entire child future. Removal uses that child's grace period. An ordered
   supervisor shutdown or group restart applies each affected child's grace in
   reverse declaration order; a dynamic shutdown applies one shared deadline
   equal to the longest affected grace. If the applicable supervisor deadline
   expires first, it aborts the outer future; dropping that future also cancels
   and aborts the inner actor task.

For any one actor, these two deadlines run concurrently rather than one after
the other. To preserve the actor layer's clean completion path, ensure that
actor's supervisor grace is at least `actor_shutdown_timeout`. If the
supervisor deadline wins,
`cooperative_then_abort` still lets supervisor shutdown complete successfully,
while `cooperative_strict` reports a timeout. In either case the supervised
Tokio task has been issued an abort and actor bindings are terminated, but—as
with every Tokio abort—code that never reaches a poll boundary, and blocking
work already running on the blocking pool, can continue outside that task.

Both defaults are five seconds, so local programs can use `Runtime::builder()`
without extra shutdown configuration. Customize the pair only when an actor's
cleanup budget or the host's shutdown deadline requires it.

## Automatic shutdown for finite work

Pipeline and batch subtrees often have a natural completion point. Mark those
children with `ChildSpec::significant()` and select an [`AutoShutdown`] mode on
the supervisor:

```rust,ignore
let batch = SupervisorBuilder::new()
    .auto_shutdown(AutoShutdown::AllSignificant)
    .child(source.restart(RestartPolicy::OnFailure).significant())
    .child(indexer.restart(RestartPolicy::Never).significant())
    .child(metrics_reporter)
    .build()?;
```

`AnySignificant` stops the remaining children after the first significant
child returns `Ok(())`; `AllSignificant` waits until every significant child
has returned `Ok(())`. Failures still follow the normal restart policy and do
not trigger automatic shutdown. Consequently, a significant `Never` child
that fails cannot later satisfy `AllSignificant`; the supervisor continues
until explicitly stopped. The same applies when a significant `Never` child is
cancelled as part of a sibling-driven `OneForAll` or `RestForOne` restart: it
does not count as a natural clean completion and cannot run again.

Significant children must use `OnFailure` or `Never`, and a supervisor with
significant children must select a non-`Never` automatic shutdown mode. Nested
supervisors can be marked significant through `SupervisorSpec::significant()`,
so a completed subtree is observed by its parent as an ordinary clean child
exit.

## Supervision trees

A supervisor is a first-class child kind, giving each subsystem its own
restart budget while failures that exhaust it escalate to the parent:

```rust,ignore
let pressroom = SupervisorBuilder::new()
    .child(press) // ... the flaky press from above
    .build()?;

let shop = SupervisorBuilder::new()
    .supervisor("pressroom", pressroom)
    .child(front_desk)
    .build()?;
```

The nested supervisor forwards its lifecycle events to the parent and shows up
inside the parent's snapshots, so observability (chapter 6) sees the whole
tree.

## Dynamic children

`DynamicSupervisorBuilder` creates an empty scope whose children are added and
removed while it is running:

```rust,ignore
let membership_epoch = handle
    .add_child(ChildSpec::new("night-shift-press", factory))
    .await?;
handle.remove_child("night-shift-press").await?;

// A dynamic nested scope has its own restart-stable handle:
let pressroom = handle.supervisor("pressroom").expect("added dynamically");
pressroom.add_child(child).await?;
```

`add_child` returns the membership epoch allocated atomically for that
insertion. It is the same value published in the child's snapshot and remains
the identity of that membership if the id is removed and reused before the
caller next samples the tree. The reply schedules immediate startup;
`wait_started()` observes the stronger readiness boundary.

Dynamic supervisors start empty and can have their last child removed. At zero
children they keep serving control commands and wait for the next `add_child`
or an explicit shutdown.
We will use a higher-level version of this API in the [Dynamic
actors](dynamic-actors.md) chapter.

[`ChildSpec`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.ChildSpec.html
[`RestartPolicy`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.RestartPolicy.html
[`RestartIntensity`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.RestartIntensity.html
[`BackoffPolicy`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.BackoffPolicy.html
[`Strategy`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.Strategy.html
[`ShutdownPolicy`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.ShutdownPolicy.html
[`AutoShutdown`]: https://stokes.io/tokio-otp/api/tokio_supervisor/enum.AutoShutdown.html
[`agent_control` example]: https://github.com/ralexstokes/tokio-otp/tree/main/crates/tokio-otp/examples/agent_control
