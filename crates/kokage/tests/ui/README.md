# Compile-fail UI tests

`actor-factory/`, `single-use-tree/`, `public-api/`, and `lifecycle-stages/`
hold [trybuild](https://github.com/dtolnay/trybuild) compile-fail cases run by
`tests/derive_ui.rs`. `actor-factory/` covers the `#[derive(ActorFactory)]`
shape and attribute contract. The `single-use-tree/` fixtures pin linear
placement, single-definition actor slots, and owner/reference boundaries;
`public-api/` pins the documented export tiers and explicit public policy
arguments. Each
`.rs` case has a checked-in `.stderr` snapshot of the exact compiler output,
spans included. The corresponding `*-pass/` directories cover supported
derive attributes and visibility.

`running_tree_scope_methods.rs` pins that `RunningTree` exposes only owner
lifecycle operations and requires explicit `.scope()` access for control or
observation. `scope_type_names_removed.rs` keeps the retired `Runtime`,
`DynamicRuntime`, `RuntimeHandle`, `DynamicRuntimeHandle`, `Scope`,
`RestrictedScope`, `RestrictedScopeRef`, and `DynamicRestrictedScope` root
names absent.

`root_export_tier.rs`, `raw_export_tier.rs`, and
`prelude_export_tier.rs` keep root task types, raw actor execution types, and
the common prelude vocabulary in their declared tiers. `removed_host_module.rs`
pins the replacement of the former `host` module by `raw` and root exports.
`send_error_types_removed.rs` keeps the superseded `TrySendError` and
`SendTimeoutError` carriers absent. `actor_slot_configuration_removed.rs`
pins that cyclic slots do not duplicate the configuration vocabulary of the
`ActorSpec` returned by `define`. `add_task_returns_unit.rs` pins that task
insertion reports only success, keeping the lineage an internal identity read
back through snapshots and lifecycle events.

`declaration-unsealed/` contains compile-pass probes proving declarations can
still be configured after one or more `actor_ref()` calls. For a slot, the
configuration is applied to the spec returned by `define`.

`lifecycle-stages/` covers the capabilities withheld from actor callbacks.
`StopContext` cannot queue a continuation after the receive loop has ended, a
handler cannot read the mailbox the provided loop owns, and a `RawActor` cannot
queue a continuation nothing drains.

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
