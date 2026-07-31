use kokage::{Tree, TaskSpec};

fn main() {
    let spec = TaskSpec::new("task", |_| async { Ok(()) });
    let mut tree = Tree::new();
    tree.add_task_spec(spec);
    tree.add_task_spec(spec);
}
