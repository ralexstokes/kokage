use kokage::{OrderedTree, TaskSpec};

fn main() {
    let spec = TaskSpec::new("task", |_| async { Ok(()) });
    let mut tree = OrderedTree::new();
    tree.add_task(spec);
    tree.add_task(spec);
}
