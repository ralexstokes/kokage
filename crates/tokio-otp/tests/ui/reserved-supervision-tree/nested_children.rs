use tokio_otp::SupervisionTree;

fn main() {
    let nested = SupervisionTree::dynamic().reserve().unwrap();
    let tree = SupervisionTree::new()
        .reserve()
        .unwrap()
        .reserved_subtree("nested", nested);

    let _nested_copy = tree.children()[0].clone();
}
