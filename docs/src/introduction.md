# Introduction

Kokage brings OTP-style supervision and typed actors to an async Rust
scheduler. Actors exchange typed messages through restart-stable
`ActorRef<M>` values, while supervision trees decide which children restart
together and how shutdown proceeds.

The guiding idea is **let it crash**: isolate fallible work in small children,
keep restart decisions in their supervisor, and rebuild fresh state instead of
trying to recover every failure in place.

## The crates

Most applications depend only on `kokage`. Its prelude contains the common
actor and tree surface; advanced configuration remains at the crate root,
low-level hosting lives under `kokage::host`, and observation types live
under `kokage::observe`.

| Crate | Role |
|---|---|
| [`kokage`](https://stokes.io/kokage/api/kokage/index.html) | Typed actors placed directly in ordered or dynamic supervision trees. |
| [`kokage-supervisor`](https://stokes.io/kokage/api/kokage_supervisor/index.html) | Actor-independent structured supervision for async tasks. |
| `kokage-derive` | The optional `ActorFactory` derive, re-exported by `kokage`. |
| `kokage-console` | An experimental live view over snapshots. |

## The mental model

If you know Erlang/OTP or Elixir, the concepts map directly:

| OTP concept | kokage equivalent |
|---|---|
| Supervisor + child specs | `OrderedTree` / `DynamicTree` + `ActorSpec` / `host::ChildSpec` |
| `one_for_one` / `one_for_all` / `rest_for_one` | `Strategy::OneForOne` / `Strategy::OneForAll` / `Strategy::RestForOne` |
| `permanent` / `transient` / `temporary` | `RestartPolicy::Always` / `RestartPolicy::OnFailure` / `RestartPolicy::Never` |
| Restart intensity (`MaxR`/`MaxT`) | `RestartConfig::new(max_restarts, within)` |
| GenServer-like process with a mailbox | An `Actor` with stage-specific lifecycle contexts |
| Registered process name | A typed `ActorRef<M>` passed during wiring; ids name scope-local children rather than global addresses |

If you do not know OTP, the tutorial builds these pieces from scratch.

An `ActorSpec<M>` declares one logical actor: its scope-local id, mailbox
policy, restart policy, shutdown policy, and incarnation factory. Calling
`actor_ref()` before placement yields its typed, restart-stable sender.

An `OrderedTree` owns static declarations. A `DynamicTree` owns a scope whose
membership can change at runtime. Moving a declaration into a tree establishes
exactly one owner, and `spawn()` validates the complete tree before starting
it.

A `Runtime` owns the spawned root. Keep it alive for the application's
lifetime; clone `RuntimeHandle` values for non-owning control and
observation.

## The running example

The tutorial grows a small print shop:

```text
  orders ref ──▶ ┌────────────┐      ┌───────┐
                 │ front-desk │ ───▶ │ press │
                 └────────────┘      └───────┘
```

- a front desk accepts orders;
- a press performs work and may fail;
- typed refs connect them;
- the tree determines their restart relationships.

Actors keep transient state inside each incarnation. Data that must survive a
restart belongs in a durable factory capture, another actor, or external
storage.

## How to read this tutorial

Start with [Getting started](getting-started.md), then use
[Actor wiring](actor-graphs.md) for slots and cyclic references. Continue with
[Supervised actors](supervised-actors.md) and
[Inspectable supervision trees](supervision-trees.md) before adding runtime
membership from [Dynamic actors](dynamic-actors.md).
