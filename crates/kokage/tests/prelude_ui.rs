//! Compile-fail coverage for the intentionally narrow prelude surface.

#[test]
fn specialized_types_require_explicit_imports() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/prelude/*.rs");
}
