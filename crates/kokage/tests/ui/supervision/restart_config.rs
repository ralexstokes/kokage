use kokage::Supervision;

#[derive(Supervision)]
#[supervision(strategy = kokage::Strategy::OneForAll)]
struct Application {
    worker: Worker,
}

struct Worker;

fn main() {}
