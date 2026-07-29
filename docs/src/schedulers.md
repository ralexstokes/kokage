# Scheduler bindings

Kokage separates supervision policy from the executor that runs it. The
`kokage` and `kokage-supervisor` crates own the public contracts; a binding
supplies task execution, blocking work, and a monotonic clock. Tokio is the
supported binding today and is enabled by `kokage`'s default `tokio` feature.

This split is deliberately narrow. Core still uses Tokio's runtime-independent
channels and `select!` macro internally, but it does not enable Tokio's `rt` or
`time` features and no Tokio or Tokio Util type is reachable through its public
API.

## Using the Tokio binding

The normal actor-tree path is unchanged:

```rust,no_run
use kokage::OrderedTree;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = OrderedTree::new().spawn()?;
# runtime.shutdown();
# Ok(())
# }
```

`OrderedTree::spawn()` and `DynamicTree::spawn()` capture the current Tokio
runtime through `kokage-tokio`. Raw task supervisors make that dependency
explicit by importing the extension trait:

```rust,no_run
use kokage_supervisor::Supervisor;
use kokage_tokio::TokioSupervisorExt as _;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let supervisor = Supervisor::ordered().build()?;
let handle = supervisor.spawn();
# handle.shutdown();
# Ok(())
# }
```

When the Tokio runtime handle comes from somewhere else, construct the adapter
directly and use the runtime-independent entry point:

```rust,no_run
use std::sync::Arc;

use kokage::OrderedTree;
use kokage_tokio::TokioScheduler;

# fn example(handle: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
let scheduler = Arc::new(TokioScheduler::new(handle));
let runtime = OrderedTree::new().spawn_with(scheduler)?;
# runtime.shutdown();
# Ok(())
# }
```

Disable `kokage`'s default features when another binding owns execution:

```toml
[dependencies]
kokage = { git = "https://github.com/ralexstokes/kokage", default-features = false }
kokage-supervisor = { git = "https://github.com/ralexstokes/kokage" }
my-kokage-scheduler = "0.1"
```

## The contract

Bindings implement `kokage_supervisor::Scheduler`:

```rust,ignore
pub trait Scheduler: Send + Sync + 'static {
    fn spawn(&self, future: BoxFuture<()>) -> TaskHandle;
    fn spawn_blocking(&self, function: Box<dyn FnOnce() + Send>) -> TaskHandle;
    fn sleep_until(&self, deadline: std::time::Instant) -> BoxFuture<()>;
    fn yield_now(&self) -> BoxFuture<()>;
    fn now(&self) -> std::time::Instant;
}
```

The trait object is shared as `Arc<dyn Scheduler>`. Its methods may be called
concurrently, and a spawned task may begin before `spawn` returns. A binding
must never poll the same future concurrently.

### Task completion and panic reporting

`TaskHandle::new` takes a join future, an abort callback, and an `is_finished`
callback. The join future must resolve once with the task's terminal state:

- normal return becomes `Ok(())`;
- executor cancellation becomes `TaskError::Cancelled`;
- unwinding becomes `TaskError::Panicked` with the original panic payload.

Panic and cancellation are different supervision outcomes. Collapsing them
loses restart information and is not a conforming implementation.

Dropping the binding's raw join future must only detach observation; it must
not cancel the task itself. Kokage's `TaskHandle` owns the cancellation policy:
dropping it, including by cancelling `TaskHandle::join`, invokes `abort`.
Calling `TaskHandle::detach` is the explicit exception and lets the task
outlive the handle.

### Abort and completion state

The abort callback must be thread-safe, non-blocking, and safe to call more
than once or after completion. It requests cancellation at the scheduler's
next cancellation boundary; it is not required to preempt code that never
yields. After a requested cancellation takes effect, joining reports
`TaskError::Cancelled`.

`is_finished` must become true when joining can no longer wait for task
execution. It may be queried from any thread and after completion. Do not mark
a task finished merely because abort was requested.

`spawn_blocking` has the same join and panic-reporting rules. It must keep the
closure off asynchronous worker threads. A binding may be unable to stop a
blocking closure that has already started, so aborting it is allowed to wait
for that closure before the join becomes terminal.

`yield_now` must defer the current task long enough for other runnable tasks to
make progress. A future that merely wakes itself and is immediately repolled
does not satisfy this guarantee. Kokage uses the primitive at restart and
shutdown fairness boundaries; implementing it with a timer can add a scheduler
tick to zero-delay restarts.

### Monotonic time

`now` and `sleep_until` form one clock domain. A binding must guarantee that:

- observed instants do not move backward;
- a sleep never completes before its deadline;
- a sleep becomes wakeable after that clock reaches or passes its deadline;
- advancing a virtual test clock wakes eligible sleepers.

Kokage builds restart delays, shutdown deadlines, timeouts, and actor timer
policy from these two primitives. Mixing wall-clock time with a monotonic
deadline, or using different clocks for `now` and `sleep_until`, breaks those
semantics.

## Binding checklist

Before publishing a third-party binding, test at least these behaviors:

1. A normal async task joins successfully.
2. Dropping `TaskHandle` aborts a pending task, while `detach` does not.
3. Cancelling a join future aborts the task.
4. A panic and an explicit abort produce different `TaskError` variants.
5. `is_finished` changes only when execution is terminal.
6. Blocking work runs away from async workers and preserves panic reporting.
7. `yield_now` lets an already-runnable peer make progress without waiting for
   a timer tick.
8. Sleeps share the `now` clock, never fire early, and respond to virtual-time
   advancement if the scheduler supports it.
9. `OrderedTree::spawn_with`, nested supervisors, restarts, actor timers, and
   graceful shutdown all run under the binding.

`kokage-tokio` is the reference implementation. A second executor is not
required by Kokage itself, but a binding that satisfies this checklist can use
the same public actor and supervisor APIs without exposing executor-specific
types.
