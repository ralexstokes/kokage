use kokage::ScopeRef;

fn removed_completion_and_capability_methods(scope: &ScopeRef) {
    let _ = scope.wait_completed(["future"]);
    let _ = scope.shutdown_on_completion(["future"]);
    let _ = scope.wait_completed_dynamic(["future"]);
    let _ = scope.shutdown_on_dynamic_completion(["future"]);
    let _ = scope.dynamic();
}

fn main() {}
