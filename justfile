# The recipes used by `ci` mirror the checks defined in nix/crane-checks.nix
# and flake.nix (the authoritative CI definitions) — keep them in sync.

nightly_cargo := env_var_or_default("NIGHTLY_CARGO", "cargo +nightly")

default:
    @just --list

fmt:
    {{nightly_cargo}} fmt --all --check

lint:
    {{nightly_cargo}} clippy --locked --workspace --all-targets --all-features -- -D warnings

check:
    cargo check --locked --workspace --all-targets --all-features

build:
    cargo build --locked --workspace --all-targets --all-features

test:
    cargo test --locked --workspace --all-targets --all-features
    cargo test --locked --workspace --doc --all-features

doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

smoke:
    cargo run --locked -p tokio-otp --example trading_engine --features metrics
    cargo run --locked -p tokio-otp --example agent_control --features metrics
    cargo run --locked -p tokio-otp --example supervised_actors
    cargo run --locked -p tokio-otp --example ref_rebind
    cargo run --locked -p tokio-otp --example drain_policy

nixfmt-check:
    nixfmt --check flake.nix nix/crane-checks.nix

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
ci: fmt lint build test smoke doc-check nixfmt-check build-book

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open

build-book:
    mdbook build docs

serve-book:
    mdbook serve docs
