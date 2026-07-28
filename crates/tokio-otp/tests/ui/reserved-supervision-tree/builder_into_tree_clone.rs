use tokio_otp::SupervisionTree;

fn main() {
    let tree = SupervisionTree::dynamic().reserve();
    let _copy = tree.clone();
}
