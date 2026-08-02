# The recipes used by `ci` mirror the checks defined in nix/crane-checks.nix
# and flake.nix (the authoritative CI definitions) — keep them in sync.

default:
    @just --list

fmt:
    cargo +nightly fmt --all --check

lint:
    cargo +nightly clippy --locked --workspace --all-targets --all-features -- -D warnings

check:
    cargo check --locked --workspace --all-targets --all-features

build:
    cargo build --locked --workspace --all-targets --all-features

test:
    cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
    # Custom benchmark harnesses are not compatible with nextest's test listing.
    cargo test --locked --workspace --all-features --bench '*'
    cargo test --locked --workspace --doc --all-features

doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

public-api:
    bash scripts/check-public-api.sh

smoke:
    cargo run --locked -p kokage --example trading_engine --features metrics,derive
    cargo run --locked -p kokage --example assistant_control_plane --features metrics,derive
    cargo run --locked -p kokage --example build_farm --features serde
    cargo test --locked -p kokage --example shard_store --features serde
    cargo run --locked -p kokage --example shard_store --features serde

all-examples:
    bash scripts/run-all-examples.sh

nixfmt-check:
    nixfmt --check flake.nix nix/crane-checks.nix

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
# The clean Nix lane retains the explicit all-target build for non-test codegen coverage.
ci: fmt lint test smoke doc-check public-api nixfmt-check test-docs

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open

test-docs:
    bash scripts/test-docs.sh

build-book:
    mdbook build docs

serve-book:
    mdbook serve docs
