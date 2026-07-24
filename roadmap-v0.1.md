# Roadmap to v0.1

## Goal

Ship a credible, publishable v0.1 of `tokio-supervisor`,
`tokio-otp-derive`, and `tokio-otp` whose existing semantics are explicit,
tested, and usable for a demanding single-process application.

The proving application is a multi-venue trading engine that:

- reconciles market and account data from several venues;
- issues, cancels, and reconciles orders;
- survives isolated venue failures without losing control of the whole engine;
- starts and stops in a controlled way; and
- exposes enough state for health and performance monitoring.

The objective is not feature parity with Erlang/OTP. The core already has the
necessary breadth for v0.1. The API-freeze work, bounded-request contract, and
end-to-end proof are implemented. The remaining work is to finish aligning the
written contract and complete publication mechanics.

## Implementation status

Status as of 2026-07-23:

| Milestone | Status | Summary |
| --- | --- | --- |
| 1. Freeze the public API shape | Complete | Public extensibility, bounded calls, console decoupling, codec removal, and dependency-boundary work are implemented and tested. Actor construction has since become factory-first (`ActorFactory`), removing the `Clone`-based restart model from the public contract. |
| 2. Trading-engine proof | Complete | The deterministic trading-engine example covers the required failure, reconciliation, control, observability, and shutdown behavior and is smoke-run by CI. It has since been hardened: pipelined router calls avoid head-of-line blocking, order keys are restart-stable, and the restart breaker consumes reliable counters. |
| 3. Written contract | In progress | Core safety rustdoc, book coverage, and behavioral tests have landed, now including factory restarts, persistent-watch semantics, reliable restart counting versus lossy events, and handler head-of-line blocking. Cross-surface alignment, the compatibility policy, API stability labels, and `CHANGELOG.md` remain. |
| 4. Publication readiness | Not started | Release metadata, MSRV verification, package-content checks, and publication rehearsal remain. |

Since the 2026-07-12 snapshot, using the trading engine as the proving
application surfaced friction that was resolved in the runtime rather than
hidden in example scaffolding, per the Milestone 2 rule:

- **Factory-first actor construction.** `ActorFactory::build` constructs each
  incarnation; factory fields are durable configuration, and actors no longer
  need to implement `Clone`. This replaces the wiring-time-clone restart model.
- **Persistent watches.** The incarnation-scoped monitor was replaced with a
  persistent `ctx.watch` that spans incarnations, emitting `Up`/`Down` events
  with bounded per-watch buffering and an explicit `Overflowed` signal.
- **Cumulative restart counters and `RestartWatch`.** `watch_restarts` on
  runtime and supervisor handles provides a reliable (non-lossy) count of
  restarts across a subtree — the supported primitive for application-owned
  correlated-failure detection.
- **Recursive runtime subtrees.** `RuntimeBuilder::subtree` composes nested
  runtime graphs with reconciled recursive actor stats. Static subtrees and
  runtime-added `RuntimeHandle::add_subtree` memberships now share the same
  actor-registry mechanism.
- **Lifecycle-bound cross-actor timers.** `send_after_to` and `interval_to`
  schedule messages to other actors with timers bound to the scheduling
  incarnation's lifecycle.

## Decisions for v0.1

These decisions keep the release small and prevent the roadmap from becoming
an open-ended OTP reimplementation.

### Delivery remains at-most-once

An accepted message can be lost if its actor incarnation exits before handling
it. Mailboxes do not survive restarts, and each restart constructs a fresh
actor from its `ActorFactory` — incarnation-local state is lost; only factory
fields and closure captures persist as durable configuration.

For order traffic, `send` or `call` success must not be described as proof that
an order was placed. Durable intent, idempotency keys, venue acknowledgements,
and post-restart reconciliation are application protocol responsibilities.

### Mailbox modes remain purpose-specific

- FIFO mailboxes are required for orders, cancellations, fills, account
  updates, reconciliation commands, and other events where every accepted
  message matters.
- Latest-wins and keyed conflation remain supported for market snapshots and
  similar replaceable state.
- The documentation and proving example must warn against using conflation for
  request/reply or order protocols.

### Restart intensity remains per child

`SupervisorBuilder::restart_intensity` supplies the default policy for each
child's independent restart tracker; a child override replaces that default.
This differs from Erlang/OTP's aggregate supervisor restart budget.

For v0.1, preserve the implemented behavior and document the divergence
precisely. Do not add an aggregate budget before release. An aggregate
supervisor budget can be added later without changing the per-child policy.
The runtime now ships `watch_restarts` cumulative counters as the supported
detection primitive: they let an application observe correlated restarts
across a subtree reliably, but they are detection, not a budget, and do not
change this decision.

### Shutdown remains concurrent

Sibling shutdown remains concurrent under a shared deadline rather than OTP's
reverse-order sequential shutdown. Applications with dependencies must stage
shutdown explicitly: stop new work, drain the order path, then stop venue and
supporting actors.

The proving example must demonstrate staged shutdown rather than implying that
`DrainPolicy::Drain` alone orders sibling teardown.

### Urgent control traffic uses a separate path

Priority mailboxes are not part of v0.1. Kill switches, emergency cancellation,
and other bounded-latency control traffic must not share a saturated market-data
mailbox. Model them as separate actors/mailboxes or an application-owned control
channel. Document this architecture in the proving example.

### One flagship composition path

`Runtime::builder()` is the primary documented entry point. Raw `ChildSpec`
composition and hand-driven `RunnableActor` remain supported escape hatches,
but are documented as advanced APIs rather than peer starting points.

## Milestone 1: freeze the public API shape — complete

Complete breaking or subtractive changes before publication.

All five workstreams in this milestone are implemented. The release crates
enable `missing_docs`; evolving public types are non-exhaustive where
appropriate; option types have constructors and setters; and the prelude has
been narrowed to the primary composition path.

### 1.1 Make public types extensible

- Audit every public enum and data-bearing struct in the three release crates.
- Add `#[non_exhaustive]` where variants or fields may grow, especially error,
  event, snapshot, options, and policy types.
- Give non-exhaustive option structs constructors and builder-style setters so
  downstream code does not depend on struct literals. In particular, settle
  the construction story for `DynamicActorOptions`.
- Audit the roughly sixty names re-exported by `tokio_otp::prelude`; retain only
  intentional, stable conveniences.
- Add `#![warn(missing_docs)]` to the release crates and document all public
  items until the existing `-D warnings` checks pass.

Acceptance criteria:

- Adding a future error or event variant will not require a downstream
  exhaustive-match break for types intended to evolve.
- Common options remain constructible without public struct literals.
- Clippy, rustdoc, and doctests are warning-free with all features.

### 1.2 Document and pin the bounded request/reply pattern

**Status: complete.** The contract is documented on `ActorRef::call` and in
the request/reply book chapter. `tests/bounded_call.rs` covers pre-binding
timeout, FIFO backpressure, post-acceptance timeout with a discarded late
reply, and success across a restart window.

Deadlines are the caller's responsibility, uniformly. Do not add
`call_timeout` or `send_timeout` APIs for v0.1. The documented bounded
choices are `try_send` for fail-fast delivery, or `tokio::time::timeout`
around `send` or `call`, which bounds both waiting for a live mailbox and
waiting for the reply.

This works because cancelling the `call` future is already safe: before
acceptance it drops the request; after acceptance it drops the reply
receiver, so a late reply is discarded harmlessly. The pattern must be
documented as a first-class contract, not left implicit:

- timeout cancels the caller's wait, not necessarily work already accepted
  by the actor;
- a timeout after acceptance leaves the external side effect unknown and
  therefore requires idempotency and reconciliation for order placement; and
- the pattern behaves predictably across mailbox backpressure and restart
  windows.

The unknown-outcome window after acceptance is the sharpest footgun in the
API for the trading use case, so the composed pattern gets the same rustdoc
and book prominence a dedicated API would have received. `CallError` is
`#[non_exhaustive]` after 1.1, so a `CallError::Timeout` variant and a
`call_timeout` convenience can be added post-v0.1 without breakage if the
nested `Result` proves annoying in practice.

Acceptance criteria:

- Tests pin the composed `tokio::time::timeout(d, call(...))` pattern:
  timeout before mailbox binding, timeout under FIFO backpressure, timeout
  after request acceptance, a late reply, and success across a short restart
  window.
- Rustdoc on `call` and the book describe the timeout pattern and the
  unknown-outcome window after acceptance.

### 1.3 Remove product-to-console coupling

**Status: complete.** `tokio-otp-console` is an unpublished workspace crate
built exclusively on the product crates' public observation surfaces. The
`tokio-otp` crate has no console feature, dependency, runtime method, or
re-export.

- Remove the `console` feature and optional `tokio-otp-console` dependency from
  the published `tokio-otp` crate.
- Remove `RuntimeHandle::console` and console re-exports from the product crate.
- Keep `tokio-otp-console` in the repository as experimental, git-only tooling
  with `publish = false`.
- Wire the console through public snapshot, event, and actor-stat surfaces so
  the core release does not depend on its web stack or security surface.

The console is useful during development, but it is not part of the stable
v0.1 contract or a production health endpoint.

### 1.4 Remove the thin JSON codec surface

**Status: complete.** The public codec module is gone and the `json_edge`
example owns the direct `serde_json` pattern.

- Remove the public `codec` module and its helpers.
- Move the useful pattern into the `json_edge` example using `serde_json`
  directly.
- Retain `serde` only where it provides meaningful topology or observability
  serialization.

### 1.5 Contain dependency coupling

**Status: complete.** `ActorContext::try_recv` now returns the crate-owned
`TryRecvError`; the cancellation-token coupling and nested actor/supervisor
shutdown deadlines are documented; and the deadline interaction is covered by
runtime tests.

- Replace the direct `TryRecvError` re-export with a crate-owned error or a
  method-specific result that can evolve independently of Tokio.
- Document deliberate public coupling to `tokio_util::sync::CancellationToken`
  where it remains in signatures.
- Document how `GraphBuilder::actor_shutdown_timeout` and supervisor
  `ShutdownPolicy` interact. In supervised hosting, state which deadline can
  abort which layer and what completion guarantees remain.

Do not redesign shutdown timeouts unless documenting and testing the current
behavior exposes an actual correctness defect.

## Milestone 2: prove the design with a trading-engine example — complete

The deterministic `trading_engine` example implements the full proving
scenario below. Its phases assert isolated panic/restart and monitor recovery,
state-timeout staleness, bounded unknown outcomes, idempotent reconciliation,
urgent-control responsiveness under conflated-feed saturation, an
application-owned aggregate restart breaker, staged shutdown, and telemetry.
It is run by both `just smoke` and the authoritative Nix CI checks, along with
`supervised_actors`, `ref_rebind`, and `drain_policy`.

Since completion the example has been hardened as the proving application:

- the order router pipelines deadline-bounded gateway calls off its handler
  loop (spawned tasks resolve the reply and report back via a message), so one
  stalled venue cannot head-of-line block the others — the hazard and pattern
  are documented in the request/reply book chapter;
- order keys are restart-stable so pipelined calls correlate correctly across
  router restarts, with same-incarnation ordering documented;
- the aggregate restart breaker consumes the runtime's `watch_restarts`
  cumulative counter instead of lossy restart events; and
- the module carries explanatory architecture documentation.

Add one self-contained integration example or book chapter using fake venues.
It must run deterministically without credentials or network access.

The example should contain:

- two venue-feed actors using keyed or latest-wins conflation for replaceable
  market snapshots;
- FIFO actors for order intent, acknowledgements, fills, cancellations, and
  reconciliation;
- a reconciler that monitors venue actors and marks data stale with state
  timeouts;
- an order router that uses `tokio::time::timeout` around `call`, idempotency
  keys, and explicit unknown-outcome reconciliation;
- readiness-gated startup so venue sessions initialize before dependent actors;
- a user-side initialization timeout so readiness cannot stall forever;
- isolated venue failure and restart, including a forced panic;
- a separate urgent-control path for kill-switch or emergency cancellation;
- an application-owned aggregate restart breaker: a health actor fed by a
  `watch_restarts` counter watch over the venue subtree that trips venue
  failover or the kill switch when restarts in a sliding window exceed a
  threshold, since per-child restart intensity cannot detect correlated
  failures on its own;
- staged shutdown that first stops new order intake, drains or reconciles
  in-flight intent, and only then stops venue actors;
- supervisor snapshots, actor stats, tracing, and metrics sampling; and
- explicit application-owned handler and queue-latency measurement, since the
  runtime does not provide those measurements.

The example is a design test, not a production trading system. If it exposes
friction that cannot be expressed safely with the existing primitives, resolve
that friction before v0.1 rather than hiding it in example scaffolding.

Acceptance criteria:

- The example starts, processes deterministic data, places a simulated order,
  and shuts down cleanly.
- A venue panic is restarted without restarting an unrelated venue.
- The reconciler observes the venue's `Down` watch event and its subsequent
  `Up` on recovery.
- A stuck order request times out rather than hanging the engine.
- An accepted-but-unacknowledged order is reconciled without duplicate effect.
- Emergency control remains responsive while the market-data path is
  saturated.
- CI runs the example as a smoke test rather than merely compiling it.

Also smoke-run the existing `supervised_actors`, `ref_rebind`, and
`drain_policy` examples, or explicitly replace their coverage with equivalent
integration tests.

## Milestone 3: make the written contract accurate — in progress

### 3.1 Establish the sources of truth

**Status: in progress.** The sources of truth are established below, and the
book and rustdoc already cover the bounded-call contract, startup/readiness,
significant children, message-size metrics, and stable versus experimental
crate split. The correlated-failure blind spot and its `watch_restarts`
remedy are now covered in the book's observability chapter and in rustdoc. A
full README/rustdoc/book alignment pass is still required, including that
blind spot and staged-shutdown guidance on every promised surface.

There is no separate normative specification. Keep the contract close to the
code and avoid maintaining a second exhaustive description that can drift.

- Rustdoc is authoritative for public API behavior and safety contracts.
- The book is authoritative for application architecture, operational
  guidance, and worked examples.
- The README defines project scope, status, features, and stability.
- This roadmap defines the v0.1 release requirements and stops being a live
  specification after the release.

The three maintained documentation surfaces must agree on:

- concurrent and sequential readiness-gated startup;
- significant children and automatic shutdown;
- actor message-size metrics;
- per-child restart-intensity scope and its difference from OTP, stated as a
  concrete blind spot rather than a scope note: a shared-cause failure that
  crash-loops many children can stay under every per-child budget and never
  escalate, so applications own correlated-failure detection until an
  aggregate budget exists — via the `watch_restarts` cumulative counters, not
  the lossy event stream (the book's observability chapter now draws this
  distinction);
- concurrent sibling shutdown and staged-shutdown guidance;
- the bounded-call contract; and
- the stable versus experimental crate surface.

### 3.2 Strengthen safety documentation

**Status: in progress.** Rustdoc and the book now document at-most-once
delivery, restart loss, the factory-based restart lifecycle, bounded calls and
their unknown-outcome window, readiness ownership, nested shutdown deadlines,
persistent-watch semantics (including overflow), reliable restart counting
versus lossy events, and the head-of-line blocking hazard of calls inside
handlers. Tests pin restart-state reconstruction, restart message loss,
timeout/drop behavior, non-`Clone` actors, and shutdown behavior. The
remaining work is a complete two-surface audit of every topic below, including
mailbox-selection, urgent-control, staged teardown, and application-owned
latency guidance.

The crate rustdoc and book must both cover:

- at-most-once delivery and restart loss windows;
- actor state being rebuilt by its `ActorFactory` on restart, with factory
  fields as the only durable configuration;
- head-of-line blocking from awaiting `call` inside a handler, and the
  pipelining pattern that avoids it;
- durable intent and idempotency for external side effects;
- timeout after acceptance as an unknown outcome;
- FIFO versus conflating mailbox selection;
- readiness timeouts owned by the child/application;
- concurrent shutdown and staged teardown;
- separate urgent-control paths; and
- which performance metrics are runtime-provided versus application-owned.

Keep intra-doc links warning-free; the previously broken `on_start` link in
the `DrainPolicy` documentation has been resolved.

### 3.3 Define compatibility expectations

**Status: not started.** There is no `CHANGELOG.md`; the README still gives
only the pre-release "APIs may change" status and does not define a v0.1.x
compatibility policy or consistently label flagship versus advanced APIs.

- Add `CHANGELOG.md`.
- State the v0.1.x compatibility policy in the README. The project may reserve
  the right to make breaking changes before 1.0, but changes must be recorded
  and intentional.
- Clearly identify the flagship, advanced, and experimental APIs.

Acceptance criteria:

- `cargo doc --workspace --no-deps --all-features` is warning-free.
- Each safety contract above appears in both rustdoc and the book.
- Tests pin restart-state reset, restart message loss, timeout/drop behavior,
  and shutdown behavior.

## Milestone 4: publication readiness — not started

The three intended crates are already versioned `0.1.0`, and the console is
correctly marked `publish = false`, but the publication work below has not
landed. In particular, the published manifests do not yet declare
`rust-version`, repository/documentation/docs.rs metadata, or versioned
internal path dependencies, and CI has no MSRV or package-content checks.

### 4.1 Complete manifest metadata

For each published crate:

- give every internal path dependency an exact compatible version requirement
  as well as its path;
- add `repository`, `documentation`, and appropriate homepage metadata;
- set and document `rust-version` for edition 2024;
- add docs.rs metadata so the intended feature surface is rendered; and
- verify package contents and README/license paths.

The expected publication set is:

1. `tokio-supervisor`;
2. `tokio-otp-derive`;
3. `tokio-otp`.

`tokio-otp-console` is not in the v0.1 publication set.

### 4.2 Verify the MSRV

- Pin the declared minimum supported Rust version explicitly.
- Add an MSRV build/check job for the published feature combinations.
- Keep nightly limited to formatting and Clippy where currently required.

### 4.3 Rehearse packaging and publication

- Make `cargo package` succeed for every published crate.
- Add package-content checks to CI.
- Use `cargo publish --dry-run` in dependency order when registry availability
  permits it. For the first release, `tokio-otp` cannot complete a normal
  registry-backed dry run until its unpublished internal dependencies exist in
  the registry; do not paper over that constraint with path-only manifests.
- After publishing the baseline, add `cargo-semver-checks` against the latest
  released version.

Acceptance criteria:

- `just ci` and `just ci-nix` pass from a clean tree.
- Packaging succeeds with only the intended files.
- Published crates build with default features, no default features, and all
  features on the declared MSRV and current toolchain.
- docs.rs will render the intended public feature surface.

## Explicit non-goals for v0.1

The following are useful possible follow-ups, not release blockers:

- distributed or remote actors and cross-node references;
- durable mailboxes, exactly-once delivery, or a built-in order journal;
- a built-in process registry or process groups;
- aggregate supervisor-wide restart budgets (the `watch_restarts` detection
  primitive exists, but enforcement remains application-owned);
- explicit terminate-while-retaining-spec or `restart_child` controls;
- reverse-order sequential shutdown;
- priority mailboxes or selective receive;
- handler-latency or queue-latency instrumentation;
- built-in HTTP health or Prometheus endpoints;
- full `gen_statem` parity;
- hot code reload or OTP release handling; and
- publishing or stabilizing the web console.

These can be added later without invalidating the v0.1 core, provided the v0.1
contracts remain intact.

## Future example coverage

The goal of the example library is that every load-bearing surface appears in
at least one realistic application, not only in unit examples. The trading
engine covers static topology, conflation, bounded calls, watches, an
aggregate restart breaker, and staged shutdown. The planned second proving
application — an LLM agent control plane (`agent-control-sketch.md`) — covers
dynamic actor lifecycles under a subtree, transient (`RestartPolicy::Never`)
children, long-running handlers as state machines, factory-fresh rehydration,
continuations, cooperative blocking work, custom `RawActor` loops, and the
`Topology` derive.

Big shapes that neither application covers, roughly in priority order:

1. **Run-to-completion workloads.** Both applications are always-on services
   that end via staged shutdown. Nothing substantial exercises completion
   semantics: `AutoShutdown::AnySignificant`/`AllSignificant`, the
   clean-exit-is-not-restarted half of `RestartPolicy::OnFailure` (the
   default policy), `Strategy::OneForAll` fate-sharing in a real pipeline,
   `StartMode::Concurrent` (also the default — both applications start
   `Sequential`), and finishing via `handle.wait()` rather than
   `shutdown_and_wait()`.
2. **Supervision without actors.** `tokio-supervisor` is independently
   usable, but both applications go through `Runtime` + actors everywhere.
   Raw `SupervisorBuilder`/`ChildSpec`/`ChildContext` over plain tokio
   tasks, mixed trees of raw children and actor subtrees, and meaningful use
   of `ShutdownMode` variants (`Abort`, `CooperativeStrict`) and
   `ShutdownPolicy` grace tuning have no real-application proof.
3. **Restart backoff as a load-bearing design.** `BackoffPolicy::Fixed` /
   `Exponential` / `JitteredExponential` appear only in unit examples. A
   flaky-external-dependency scenario should pin how backoff, the restart
   intensity window, and `RestartWatch` counters interact.
4. **Overload where conflation does not apply.** Both applications absorb
   overload with keyed conflation, which only works for replaceable state.
   Bounded FIFO mailboxes under sustained pressure — `try_send` +
   `SendError::Full` as deliberate load shedding, backpressure propagating
   through a multi-stage pipeline, and reading `ActorStats` correctly while
   it happens — is only covered by small unit examples.
5. **The real-I/O boundary.** Both applications are deliberately in-process
   and deterministic (`ExchangeSim`, `ChatSim`). No example shows the
   supervisor owning real listener/connection lifecycles, the
   `json_edge`-style byte boundary on live sockets, or the experimental
   console attached to a nontrivial application tree.

Two candidate applications would close most of this: a **batch data
pipeline / job orchestrator** (gaps 1, 3, and 4 compose naturally:
concurrent start, one-for-all stages, backoff against a flaky source,
bounded-mailbox shedding, completion via significant children) and a
**supervised web service** (gaps 2 and 5: raw children beside actor
subtrees, real sockets, console integration). Neither is a v0.1 blocker;
like the first two, each should be a deterministic, assertion-driven design
test whose friction is resolved in the runtime rather than hidden in
scaffolding.

## Features retained for v0.1

The roadmap does not call for removing working features that support the target
application or materially strengthen the runtime:

- all three supervision strategies;
- nested and dynamic supervisors, plus recursive runtime subtrees;
- significant children and automatic shutdown;
- readiness-gated sequential startup;
- fixed, exponential, and jittered restart backoff;
- factory-first actor construction (`ActorFactory`; actors need not be
  `Clone`);
- persistent actor watches, timers (including lifecycle-bound cross-actor
  timers), state timeouts, and continuations;
- FIFO, latest-wins, and keyed-conflating mailboxes;
- restart-stable typed actor references;
- topology derivation and metadata;
- restart monitoring, cumulative restart counters, and `RestartWatch`;
- snapshots, events, tracing, actor stats, and message-size metrics; and
- raw/advanced composition escape hatches.

Retaining a feature does not make every part of it equally prominent or equally
stable. Tooling-oriented serialization and low-level channel access should be
documented as advanced surfaces where appropriate.

## Definition of done

v0.1 is ready to tag when:

- [x] all Milestone 1 API changes are complete;
- [x] the trading-engine proving example satisfies its behavioral checks;
- [ ] rustdoc, the book, the README, and the implementation agree;
- [ ] the three release crates package successfully with complete metadata;
- [ ] formatting, Clippy, builds, tests, doctests, example smoke tests, MSRV checks,
  and `just ci-nix` are green from a clean tree;
- [x] no published crate depends on the unpublished console;
- [x] every public item is documented and rustdoc warnings are denied by CI; and
- [ ] the README and changelog describe the v0.1 compatibility and delivery
  contracts.
