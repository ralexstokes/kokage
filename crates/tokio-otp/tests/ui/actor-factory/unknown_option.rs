#[derive(tokio_otp::ActorFactory)]
struct Worker {
    #[factory(reset)]
    local: usize,
}

fn main() {}
