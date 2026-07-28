use tokio_otp::SupervisionTree;

fn main() {
    let tree = SupervisionTree::new().reserve().unwrap();
    let _copy = tree.clone();
}
