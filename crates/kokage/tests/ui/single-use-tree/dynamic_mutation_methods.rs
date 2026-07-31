use kokage::{ActorSpec, DynamicTree, Tree, Strategy, TaskSpec};

fn actor() -> ActorSpec<()> {
    loop {}
}

fn task() -> TaskSpec {
    loop {}
}

fn main() {
    let _ = DynamicTree::new().strategy(Strategy::OneForAll);
    DynamicTree::new().add_actor_spec(actor());
    DynamicTree::new().add_task_spec(task());
    DynamicTree::new().add_subtree("nested", Tree::new());
}
