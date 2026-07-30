use kokage::ScopeRef;

fn removed_dynamic_capability_accessor(scope: &ScopeRef) {
    let _ = scope.dynamic();
}

fn main() {}
