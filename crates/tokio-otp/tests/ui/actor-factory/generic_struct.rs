#[derive(tokio_otp::ActorFactory)]
struct Worker<T> {
    value: T,
}

fn main() {}
