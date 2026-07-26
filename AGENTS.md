# tokio-otp

Erlang/OTP-like functionality for the `tokio` ecosystem, organized as a Cargo workspace.

## Structure

* `crates/tokio-supervisor` — structured task supervision (supervision trees, restart strategies)
* `crates/tokio-otp` — the product crate: typed actor graphs (private `actor/` module tree, API re-exported flat) plus the OTP-style runtime that supervises them
* `crates/tokio-otp-derive` — `#[derive(Topology)]` proc macro, re-exported by `tokio-otp` under the default `derive` feature
* `crates/tokio-otp-console` — [experimental] web-based dashboard for supervisor trees (axum + WebSocket)
* `docs/` — mdBook tutorial (`just build-book` / `just serve-book`)
* `nix/crane-checks.nix` + `flake.nix` — authoritative CI check definitions

## Development workflow

* A development shell is provided via the Nix flake's `nix develop`.

* During development, use `just ci` — the pull-request CI fast lane, which reuses the cargo cache. It runs fmt, clippy, build, tests (including doctests), nixfmt, and the book build.

* Ordinary pull requests run `nix flake check --no-build` followed by `just ci` inside the development shell. Pushes to `main` and pull requests that change Nix or Rust toolchain definitions run the full clean Nix lane instead.

* If having trouble with the full GitHub CI lane, use `just ci-nix`. It runs `nix flake check`, building with `--locked` from a clean source tree, so `just ci` passing does not guarantee `just ci-nix` passes.

* The `just ci` recipes mirror the check definitions in `nix/crane-checks.nix` — if you change flags in one, update the other.

* If you add new files that would be covered by `nix flake check`, stage them first (`git add`). Nix builds from the git tree and cannot see untracked files.

## Conventions

* Formatting and linting use the nightly toolchain: `cargo +nightly fmt` and `cargo +nightly clippy`. Clippy runs with `-D warnings` — new code must be warning-free.

* Builds and tests run with `--workspace --all-targets --all-features`; keep feature-gated code (`metrics`, `serde`) compiling under those flags.

* Nix files are formatted with `nixfmt` (checked by `just nixfmt-check`).

* Type names are suffixed by what the value carries, not by what it configures:

  * `*Factory` — construction only, one `build()`-shaped method, no identity and no policy (`ActorFactory`). Its fields are the durable configuration that survives restarts.
  * `*Spec` — a complete child declaration a supervisor can insert: identity + payload + supervision policy (`ChildSpec`, `SupervisorSpec`, `ActorSpec`). Where the identity comes from — an explicit argument or the payload's own label — does not matter.
  * `*Options` — tuning with no payload and no identity, passed alongside the thing it modifies (`ActorOptions`, `DynamicActorOptions`).
  * `*Builder` — assembles one of the above from parts (`GraphBuilder`, `SupervisorBuilder`, `RunnableActorBuilder`).
