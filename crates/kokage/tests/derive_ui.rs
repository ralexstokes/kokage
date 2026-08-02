//! Compile-fail coverage for derive guarantees.

#![cfg(feature = "derive")]

#[test]
fn derive_ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/actor-factory/*.rs");
}
