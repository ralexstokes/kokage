//! Compile-fail and compile-pass coverage for actor-factory derive guarantees,
//! lifecycle-stage restrictions, and single-use construction tokens.

#[test]
fn derive_ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/actor-factory/*.rs");
    t.pass("tests/ui/actor-factory-pass/*.rs");

    // The lifecycle-stage contexts exist to turn documented hazards into
    // compile errors. Each case here was legal — and either deadlocking or a
    // silent no-op — when every hook shared one `ActorContext`.
    t.compile_fail("tests/ui/lifecycle-stages/*.rs");

    // Trees own live pre-spawn identities. The type system prevents both
    // cloning that ownership and reaching through an opaque tree to duplicate
    // a nested scope declaration.
    t.compile_fail("tests/ui/single-use-tree/*.rs");

    // Minting a stable ref seals mailbox configuration so options that shape
    // the binding cannot fail later through an ordering panic.
    t.compile_fail("tests/ui/declaration-sealing/*.rs");

    // Public API tiers are intentionally disjoint: observation and raw-hosting
    // types live outside the crate root and day-one prelude, and the supervisor
    // attachment bridge remains hidden behind `__private`.
    t.compile_fail("tests/ui/public-api/*.rs");
}
