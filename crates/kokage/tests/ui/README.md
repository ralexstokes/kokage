# Compile-fail UI tests

`actor-factory/` holds [trybuild](https://github.com/dtolnay/trybuild)
compile-fail cases run by `tests/derive_ui.rs`. They cover the
`#[derive(ActorFactory)]` shape and attribute contract. Each `.rs` case has a
checked-in `.stderr` snapshot of the exact compiler output, spans included.
The corresponding `actor-factory-pass/` directory contains compile-success
probes not already exercised by the integration tests.

## Updating snapshots on a toolchain bump

The snapshots are coupled to rustc's error rendering, so bumping the pinned
toolchain in `rust-toolchain.toml` may break them even though nothing is
wrong. Regenerate locally and review the diff:

```sh
./scripts/dev env TRYBUILD=overwrite cargo test -p kokage --test derive_ui
git diff crates/kokage/tests/ui
```

Commit the regenerated `.stderr` files together with the toolchain bump.
This must happen locally: `just ci-nix` runs in a read-only sandbox and can
only report the mismatch, not overwrite the snapshots.
