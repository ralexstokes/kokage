# Sidecar example

`sidecar` is an assertion-driven acceptance script for embedding Kokage inside
a host process that owns `main`, the Tokio runtime, initialization, and
teardown. The root supervision tree contains four plain task services and one
actor subtree as siblings:

```text
sidecar (ordered)
├── config-watcher       plain task, cooperative
├── cache-refresher      plain task, cooperative
├── log-rotator          plain task, immediate abort
├── actor-services       nested actor subtree
│   └── audit            typed actor/mailbox
└── health-prober        plain task, strict bounded cooperation
```

The script deliberately fails the last ordered child once and makes the host
roll back the already-started prefix. It then completes two successful
embed/run/stop cycles with ordinary host work between them. Assertions require
the rotator's immediate `Shutdown::abort()` classification and the prober's
full `Shutdown::graceful_for(...)` window, timeout error, and
`after_grace: true` exit classification. Host teardown is asserted to occur
only after the second sidecar has stopped.

Run it with:

```sh
./scripts/dev cargo run --locked -p kokage --example sidecar
```

## API friction and improvement opportunities

- The issue inventory used the draft names `SupervisorBuilder`, `ChildSpec`,
  `ChildContext`, `ShutdownMode`, and `ShutdownPolicy`. The current public API
  deliberately exposes supervision through `Tree`, `TaskSpec`, `TaskContext`,
  and the combined `Shutdown` policy. The required task-only behavior is fully
  available, but the actor-oriented `Tree` name makes standalone/task-first
  discovery less obvious. A task-first guide or a discoverability alias could
  make this embedding mode easier to find without reopening the private
  supervisor implementation.
- A terminal ordered-startup failure is reported by `wait_started`, but the
  still-owned, partially started tree does not roll itself back. That is
  consistent with host ownership, and this example performs an explicit
  `RunningTree::shutdown`; a future `start_or_shutdown` convenience could make
  the safe pattern harder to omit.
- Strict cooperative grace expiry is correctly observable in structured
  `ExitStatus`, but `SupervisorError::ShutdownTimedOut` carries the affected
  child IDs as one formatted string. A structured collection would let hosts
  branch on identities without parsing presentation text. Changing that public
  error shape is broader than this example and was not hidden with scaffolding
  here: the acceptance assertion intentionally exposes the current contract.
- Re-embedding requires constructing a fresh, single-use `Tree`, as documented
  by the ownership API. Reusing the same process-local configuration and state
  across fresh declarations was straightforward; no runtime correctness gap
  was observed.
