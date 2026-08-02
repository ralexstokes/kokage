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
observe the declaration-ordered prefix still running before explicitly rolling
it back. It then completes two successful embed/run/stop cycles with ordinary
host work between them. Assertions require the rotator's immediate
`Shutdown::abort()` classification and show, from the host's shutdown boundary,
that the stubborn prober reaches its configured `Shutdown::graceful_for(...)`
bound before the timeout error and `after_grace: true` exit classification.
The prober's own events prove cancellation precedes escalation without treating
its scheduler-dependent wake time as the start of the grace. Host teardown is
asserted to occur only after the second sidecar has stopped.

Run it with:

```sh
./scripts/dev cargo run --locked -p kokage --example sidecar
```

## API friction and improvement opportunities

- The issue inventory referred to a separately usable `kokage-supervisor`
  package and the draft names `SupervisorBuilder`, `ChildSpec`, `ChildContext`,
  `ShutdownMode`, and `ShutdownPolicy`. The current workspace instead exposes
  task supervision through the public `kokage` facade as `Tree`, `TaskSpec`,
  `TaskContext`, and the combined `Shutdown` policy. This example therefore
  treats the package-level goal as evolved: it proves task-first behavior
  through the supported facade, not a separately versioned or actor-free
  dependency, because no such package currently exists. If a package split is
  still desired, that remains uncovered. Within the current facade, the
  actor-oriented `Tree` name also makes standalone/task-first discovery less
  obvious; a task-first guide or discoverability alias could help without
  reopening the private supervisor implementation.
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
