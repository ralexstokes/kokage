use tokio_otp::SupervisionTree;

fn main() {
    let tree = SupervisionTree::new().reserve();
    let _copy = tree.clone();
}
