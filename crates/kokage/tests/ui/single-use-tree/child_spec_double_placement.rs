use kokage::{OrderedTree, host::ChildSpec};

fn main() {
    let spec = ChildSpec::task("task", |_| async { Ok(()) });
    let _tree = OrderedTree::new().task(spec).task(spec);
}
