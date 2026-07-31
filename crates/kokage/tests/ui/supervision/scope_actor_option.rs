#[derive(kokage::Supervision)]
struct App {
    #[supervision(scope, mailbox = kokage::Mailbox::queue(8))]
    child: Child,
}

struct Child;

fn main() {}
