use kokage::Supervision;

#[derive(Clone)]
struct NotActor;

#[derive(Supervision)]
struct BadScope {
    worker: NotActor,
}

fn main() {}
