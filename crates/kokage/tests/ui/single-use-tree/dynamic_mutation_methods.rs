use kokage::{ActorSpec, DynamicTree, OrderedTree, Strategy, TaskSpec};

fn actor() -> ActorSpec<()> {
    loop {}
}

fn task() -> TaskSpec {
    loop {}
}

fn main() {
    let _ = DynamicTree::new().strategy(Strategy::OneForAll);
    DynamicTree::new().add_actor(actor());
    DynamicTree::new().add_task(task());
    DynamicTree::new().add_subtree("nested", OrderedTree::new());
}
