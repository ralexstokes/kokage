//! Compile-fail and compile-pass coverage for the derive guarantees: actor
//! factory and topology shapes and attributes, plus the topology token-API
//! errors the type system promises (wrong message type for a slot and reusing
//! a consumed slot token).

#[test]
fn actor_factory_derive_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/actor-factory/*.rs");
    t.pass("tests/ui/actor-factory-pass/*.rs");
}

#[test]
fn topology_derive_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/topology/*.rs");
    t.pass("tests/ui/topology-pass/*.rs");
}
