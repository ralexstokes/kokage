# Introduction

`tokio-otp` is a small family of crates that bring Erlang/OTP-style fault
tolerance to the [`tokio`](https://tokio.rs) ecosystem. The core idea is the
same one that has kept telecom switches running for decades: **let it crash**.
Instead of writing defensive code that tries to recover from every possible
failure in place, you organize your program into small, isolated tasks and let
a *supervisor* restart the ones that fail.

## The crates

`tokio-otp` is the product: depend on it alone, import everything through
`tokio_otp::prelude`, wire actors in a `Graph`, and compose them with a
`SupervisionTree`. `Runtime::builder()` is the concise convenience when one
graph occupies one ordered scope. The crate contains both the typed actor
layer and the runtime that supervises it, built on one deliberately
independent crate:

| Crate | Role |
|-------|------|
| [`tokio-otp`](https://stokes.io/tokio-otp/api/tokio_otp/index.html) | Static graphs of communicating actors — typed mailboxes, restart-stable `ActorRef<M>` handles, request/reply, cooperative blocking work — with each actor running as its own supervised child under one integrated `Runtime`. |
| [`tokio-supervisor`](https://stokes.io/tokio-otp/api/tokio_supervisor/index.html) | Structured supervision of async tasks: restart policies, restart intensity limits, graceful shutdown, and supervision trees. |
| [`tokio-otp-console`](https://stokes.io/tokio-otp/api/tokio_otp_console/index.html) | An experimental, git-only web console for watching a running supervision tree. It is separate from the published product crate. |

`tokio-supervisor` knows nothing about actors — it supervises any async task,
and is useful on its own if that is all you need. `tokio-otp` builds on it:
actors are the unit of execution, and what an actor's exit *means* — restart,
final completion, escalation — is always supervisor policy, never the actor's
own concern.

## The mental model

If you have used Erlang/OTP or Elixir, the mapping is direct:

| OTP concept | tokio-otp equivalent |
|-------------|----------------------|
| Supervisor + child specs | [`SupervisionTree`] + [`ActorSpec`] / [`ChildSpec`] |
| `one_for_one` / `one_for_all` / `rest_for_one` | `Strategy::OneForOne` / `Strategy::OneForAll` / `Strategy::RestForOne` |
| `permanent` / `transient` / `temporary` | `RestartPolicy::Always` / `RestartPolicy::OnFailure` / `RestartPolicy::Never` |
| Restart intensity (`MaxR`/`MaxT`) | `RestartIntensity::new(max_restarts, within)` |
| GenServer-ish process with a mailbox | An actor with an [`ActorContext`] |
| Registered process name | A typed `ActorRef<M>`, minted at wiring time and passed around (labels are display names, not addresses) |

If you have not: don't worry. This tutorial builds everything up from scratch.

## The running example

Throughout the tutorial we build a tiny **print shop** service:

```text
                 ┌────────────┐      ┌───────┐      ┌──────────┐
  orders ref ──▶ │ front-desk │ ───▶ │ press │ ───▶ │ shipping │
                 └────────────┘      └───────┘      └──────────┘
```

- Customers submit print orders through a typed `ActorRef<Order>` (the stable
  entry point into the graph).
- The **front-desk** actor validates orders and forwards them.
- The **press** actor does the actual printing — and occasionally jams.
- The **shipping** actor records finished jobs.

The press jamming is the interesting part: we want the rest of the shop to
keep running while a supervisor replaces the press, and we want in-flight
senders to transparently reconnect to the new press. That is exactly what
these crates are for.

## How to read this tutorial

Each chapter is a complete, runnable program. You can paste any of them into a
binary crate and run it, or explore the closely related examples that ship in
each crate's `examples/` directory (listed in [Where to go
next](next-steps.md)).

[`SupervisionTree`]: https://stokes.io/tokio-otp/api/tokio_otp/enum.SupervisionTree.html
[`ActorSpec`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorSpec.html
[`ChildSpec`]: https://stokes.io/tokio-otp/api/tokio_supervisor/struct.ChildSpec.html
[`ActorContext`]: https://stokes.io/tokio-otp/api/tokio_otp/struct.ActorContext.html
