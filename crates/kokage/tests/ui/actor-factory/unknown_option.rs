#[derive(kokage::ActorFactory)]
struct Worker {
    #[factory(reset)]
    local: usize,
}

fn main() {}
