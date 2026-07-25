#[derive(tokio_otp::ActorFactory)]
struct Worker {
    #[factory(default, default)]
    local: usize,
}

fn main() {}
