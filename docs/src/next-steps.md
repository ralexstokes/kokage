# Where to go next

## API documentation

The rustdocs are the reference companion to this tutorial — the crate-level
docs in particular are worth reading in full:

```sh
just doc   # cargo doc --workspace --no-deps --open
```

## Runnable examples

Every feature covered here (and a few that weren't) has a focused, runnable
example. Run any of them with:

```sh
cargo run -p <crate> --example <name>
```

### `kokage-supervisor`

| Example | Shows |
|---------|-------|
| `one_for_one_restart` | Basic restart behaviour. |
| `one_for_all_pipeline` | Interdependent children with `OneForAll`. |
| `nested_supervisor` | Supervision trees. |
| `dynamic_children` | Adding and removing children at runtime. |
| `per_child_restart_intensity` | Per-child intensity overrides. |
| `shutdown_with_cancellation_token` | Graceful shutdown driven by a signal. |
| `watch_lifecycle_recursive` | Reacting to lifecycle events across a tree. |
| `subscribe_to_snapshots` | Polling supervisor state. |
| `tracing` | Structured logging output. |
| `metrics` | Prometheus metrics (needs `--features metrics`). |

### `kokage`

| Example | Shows |
|---------|-------|
| `supervised_actors` | Per-actor supervision with default policies. |
| `supervision` | A typed cycle wired explicitly with `ActorSlot`. |
| `individual_actor_policies` | Per-actor restart/shutdown overrides. |
| `dynamic_actors` | Adding and removing actors at runtime, refs distributed by message. |
| `directory` | A typed, userland name-directory actor (registry replacement pattern). |
| `drain_policy` | Draining queued actor messages within one shutdown bound. |
| `ref_rebind` | Stable typed actor refs across supervised restarts. |
| `send_vs_try_send` | Waiting `send` vs fail-fast `try_send` across a restart window. |
| `mailbox_backpressure` | Bounded mailbox back-pressure. |
| `graph_failures` | Supervisor policy around actor failures. |
| `builder_validation` | Tree validation errors reported at spawn. |
| `blocking_work` | Awaiting cooperative blocking work with `run_blocking`. |
| `blocking_lifecycle` | Detached blocking work returning results as actor messages. |
| `actor_tracing` | Structured actor and message tracing. |
| `supervisor_snapshot_trace` | Observing runtime state in detail. |
| `actor_metrics` | Pull-based actor stats and user-owned export sampling. |
| `console` | The experimental live web console (`cargo run -p kokage-console --example console`). |

## Design notes

Things this tutorial glossed over that matter in production:

- **Messages are ordinary owned Rust values.** Use enums to model protocol
  variants and put `Reply<T>` in a variant when callers need a response.
- **Messages in a failed actor's mailbox are lost** when it restarts. If an
  order must survive a press jam, persist it outside the actor and re-inject
  it — the same discipline OTP asks of you.
- **Restart budgets are your circuit breakers.** Tune `Restart` so a
  persistent fault escalates to something (a parent supervisor, your process
  manager, an alert) instead of looping forever.
- **Application-level breakers must not infer restarts from event pairs.** A
  lag marker means transition detail was dropped. Drive a breaker from
  `watch_lifecycle()` and the cumulative counters on its event envelope, or
  from snapshots — see the observability chapter.
- **Blocking work needs cancellation checks.** Cooperative shutdown is only as
  graceful as your `token.is_cancelled()` checks are frequent.
