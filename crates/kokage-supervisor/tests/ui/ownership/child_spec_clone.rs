use kokage_supervisor::ChildSpec;

fn clone_spec(spec: ChildSpec) {
    let _copy = spec.clone();
}

fn main() {}
