use tokio_otp::{ActorSpec, ChildSpec, DynamicTree, OrderedTree, Strategy};

fn actor() -> ActorSpec {
    loop {}
}

fn task() -> ChildSpec {
    loop {}
}

fn main() {
    let _ = DynamicTree::new().strategy(Strategy::OneForAll);
    let _ = DynamicTree::new().actor(actor());
    let _ = DynamicTree::new().task(task());
    let _ = DynamicTree::new().subtree("nested", OrderedTree::new());
}
