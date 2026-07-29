use kokage::{DynamicTree, OrderedTree};

fn main() {
    let nested = DynamicTree::new();
    let tree = OrderedTree::new().subtree("nested", nested);

    let _nested_copy = tree.children()[0].clone();
}
