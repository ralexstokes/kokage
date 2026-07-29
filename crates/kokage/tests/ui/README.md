# Compile-fail UI tests

`actor-factory/`, `supervision/`, and `lifecycle-stages/` hold
[trybuild](https://github.com/dtolnay/trybuild) compile-fail cases run by
`tests/derive_ui.rs`. The former covers the `#[derive(ActorFactory)]` shape
and attribute contract. The latter covers the `#[derive(Supervision)]` contract,
typed factory return/completeness checks, plus the two token-API guarantees
(wrong-type `define` is E0271, reusing a consumed `ActorSlot` is E0382). Each
`.rs` case has a checked-in `.stderr` snapshot of the exact compiler output,
spans included. The corresponding `*-pass/` directories cover supported
derive attributes, visibility, constructor functions, and capturing
factories.

`lifecycle-stages/` covers the guarantees bought by splitting `ActorContext`
into per-stage views: `on_start` cannot await its own readiness through
`RestrictedScope`, `on_stop` cannot await a scope lifecycle through the same
restricted type, queue a continuation that would be dropped, or start an
actor-owned scope wait after the receive loop has ended; a handler cannot read
the mailbox the provided loop owns, and a `RawActor` cannot queue a continuation
nothing drains. Each was legal code — deadlocking or silently doing nothing —
when all four hooks shared one context type.

Restricted scope handles must also stay restricted under navigation, so
`on_start_subtree_wait_started.rs` pins that `subtree()` returns another
restricted handle rather than handing the withheld waits back.

## Updating snapshots on a toolchain bump

The snapshots are coupled to rustc's error rendering, so bumping the pinned
toolchain in `rust-toolchain.toml` may break them even though nothing is
wrong. Regenerate locally and review the diff:

```sh
TRYBUILD=overwrite cargo test -p kokage --test derive_ui
git diff crates/kokage/tests/ui
```

Commit the regenerated `.stderr` files together with the toolchain bump.
This must happen locally: `just ci-nix` runs in a read-only sandbox and can
only report the mismatch, not overwrite the snapshots.
