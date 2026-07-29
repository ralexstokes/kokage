use kokage::Supervision;

struct Worker;

#[derive(Supervision)]
#[supervision(restart_intensity = ())]
struct ScopeOption {
    worker: Worker,
}

#[derive(Supervision)]
struct FieldOption {
    #[supervision(restart_intensity = ())]
    worker: Worker,
}

fn main() {}
