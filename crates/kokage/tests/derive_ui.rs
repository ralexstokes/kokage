//! Compile-fail and compile-pass coverage for derive guarantees.

#![cfg(feature = "derive")]

#[test]
fn derive_ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/actor-factory/*.rs");
    t.pass("tests/ui/actor-factory-pass/*.rs");
}
