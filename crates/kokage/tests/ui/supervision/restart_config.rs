use kokage::Supervision;

#[derive(Supervision)]
#[supervision(strategy = kokage::Strategy::OneForAll)]
struct Application {
    worker: fn() -> Worker,
}

struct Worker;

fn main() {}
