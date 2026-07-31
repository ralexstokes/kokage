//! Compile-fail and compile-pass coverage for actor-factory derive guarantees,
//! lifecycle-stage restrictions, and single-use construction tokens.

#[test]
fn derive_ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/actor-factory/*.rs");
    t.pass("tests/ui/actor-factory-pass/*.rs");

    // The lifecycle-stage contexts keep stage-specific no-op operations out of
    // the API, such as receiving through a handler context or queuing work from
    // a shutdown hook after the receive loop has ended.
    t.compile_fail("tests/ui/lifecycle-stages/*.rs");

    // Trees own live pre-spawn identities. The type system prevents both
    // cloning that ownership and reaching through an opaque tree to duplicate
    // a nested scope declaration.
    t.compile_fail("tests/ui/single-use-tree/*.rs");

    // Minting a stable ref borrows the declaration. A spec stays configurable
    // until placement; a slot carries no options, so the same probe configures
    // the spec `define` returns.
    t.pass("tests/ui/declaration-unsealed/*.rs");

    // Public API tiers are intentionally disjoint: task declarations and
    // their shared error surface live at the root, raw actor hosting and
    // observation have named modules, and neither escape hatch is in the
    // prelude. The low-level supervisor layer remains private, and ActorSlot
    // exposes only cyclic-ref construction and definition.
    t.compile_fail("tests/ui/public-api/*.rs");
}
