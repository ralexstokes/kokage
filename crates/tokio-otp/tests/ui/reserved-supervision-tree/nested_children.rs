use tokio_otp::SupervisionTree;

fn main() {
    let nested = SupervisionTree::dynamic().reserve();
    let tree = SupervisionTree::new()
        .reserve()
        .reserved_subtree("nested", nested);

    let _nested_copy = tree.children()[0].clone();
}
