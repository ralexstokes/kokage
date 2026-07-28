use tokio_otp::{ActorSpec, ChildSpec, Strategy, SupervisionTree};

fn actor() -> ActorSpec {
    loop {}
}

fn task() -> ChildSpec {
    loop {}
}

fn main() {
    let _ = SupervisionTree::dynamic().strategy(Strategy::OneForAll);
    let _ = SupervisionTree::dynamic().actor(actor());
    let _ = SupervisionTree::dynamic().task(task());
    let _ = SupervisionTree::dynamic().subtree("nested", SupervisionTree::new());
}
