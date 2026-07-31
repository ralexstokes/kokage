#[derive(kokage::Supervision)]
struct Generic<T> {
    actor: T,
}

fn main() {}
