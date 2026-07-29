#[test]
fn scope_kind_builders_reject_invalid_sequences() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/builder/*.rs");
}
