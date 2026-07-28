//! Compile-fail and compile-pass coverage for the derive guarantees: actor
//! factory and supervision shapes and attributes, plus the token-API errors
//! the type system promises (wrong message type for a slot and reusing a
//! consumed slot token).

#[test]
fn actor_factory_derive_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/actor-factory/*.rs");
    t.pass("tests/ui/actor-factory-pass/*.rs");
}

/// The lifecycle-stage contexts exist to turn documented hazards into compile
/// errors. Each case here was legal — and either deadlocking or a silent
/// no-op — when every hook shared one `ActorContext`.
#[test]
fn lifecycle_stage_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/lifecycle-stages/*.rs");
}

/// A reserved tree owns live pre-spawn identities. The type system prevents
/// both cloning that ownership and reaching through it to clone nested scope
/// declarations that carry reservation markers.
#[test]
fn reserved_supervision_tree_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/reserved-supervision-tree/*.rs");
}

#[test]
fn supervision_derive_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/supervision/*.rs");
    t.pass("tests/ui/supervision-pass/*.rs");
}
