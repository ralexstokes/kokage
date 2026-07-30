use kokage::{OrderedTree, host::TaskSpec};

fn main() {
    let spec = TaskSpec::new("task", |_| async { Ok(()) });
    let _tree = OrderedTree::new().task(spec).task(spec);
}
