# Next Steps

You have taken the print shop from one actor to a supervised, observable,
dynamically growing system. Here is where to go from the end of the
tutorial.

## Read the examples

Every feature in this book has a runnable counterpart under
[`crates/kokage/examples/`](https://github.com/ralexstokes/kokage/tree/main/crates/kokage/examples)
— run any of them with `cargo run -p kokage --example <name>`:

- `supervised_actors`, `individual_actor_policies`, `graph_failures` —
  restart policies and strategies in action.
- `mailbox_backpressure`, `send_vs_try_send`, `drain_policy` — mailboxes,
  send flavors, and shutdown draining.
- `supervision`, `ref_rebind`, `builder_validation` — cyclic wiring, refs
  riding through restarts, spawn-time validation.
- `dynamic_actors`, `directory` — runtime membership and a userland name
  directory.
- `blocking_work`, `blocking_lifecycle` — cooperative and detached blocking
  work.
- `task_*` — the task-supervision family: strategies, nesting, dynamic
  children, restart intensity, snapshots, lifecycle event streams.
- `actor_metrics`, `actor_tracing`, `task_metrics`, `task_tracing`,
  `supervisor_snapshot_trace` — observability patterns ready to adapt.
- `json_edge` — decoding a byte-oriented edge into typed messages.

Four larger examples put everything together the way this book did, and are
kept compiling and running in CI: **`trading_engine`** (feeds, venues, a
reconciler, telemetry), **`assistant_control_plane`** (an LLM-agent control
plane with offloaded model calls), and **`build_farm`** (a finite dependency
build over restarting service tasks and dynamic one-shot workers). The first
two run with `--features metrics,derive`; `build_farm` runs with
`--features serde` so it can validate and round-trip its declaration outline
before spawn. **`sidecar`** is the task-first embedding case: a host-owned
process starts and stops plain supervised services, mixes in one actor subtree,
rolls back failed startup, and re-embeds supervision without surrendering
`main` or the Tokio runtime. It runs with the default feature set.

## Watch a tree live

The experimental `kokage-console` crate serves a local web dashboard over a
running supervision tree — spawn your tree, point your browser at it, and
watch restarts and mailboxes in real time:

```sh
cargo run -p kokage-console --example console
```

## Reference material

- The [API documentation](https://stokes.io/kokage/api/) covers the full
  surface, including corners this tutorial only brushed — the `raw` module,
  the `observe` module's view types, and every builder option.
- The crate-level rustdoc for `kokage` doubles as a dense architectural
  summary: delivery contracts, lifecycle model, and the reasoning behind
  them.

## A word on maturity

Kokage is early-stage and evolving: the crates are not yet published to
crates.io (use a git dependency), and APIs may change. The delivery
contract, ownership model, and supervision semantics described in this book
are the stable core of the design — and since this book is compiled and run
against the sources on every change, it will keep telling you the truth as
the edges move.

Found something unclear, or a failure story the supervisor handled badly?
Issues and discussions are welcome on
[GitHub](https://github.com/ralexstokes/kokage).
