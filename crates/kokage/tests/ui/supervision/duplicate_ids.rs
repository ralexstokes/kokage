#[derive(kokage::Supervision)]
struct App {
    first: Actor,
    #[supervision(id = "first")]
    second: Actor,
}

struct Actor;

fn main() {}
