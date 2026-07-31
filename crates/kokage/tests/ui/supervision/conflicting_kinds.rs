#[derive(kokage::Supervision)]
struct App {
    #[supervision(scope, dynamic)]
    child: Child,
}

struct Child;

fn main() {}
