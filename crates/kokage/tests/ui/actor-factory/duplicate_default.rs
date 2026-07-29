#[derive(kokage::ActorFactory)]
struct Worker {
    #[factory(default, default)]
    local: usize,
}

fn main() {}
