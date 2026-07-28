use tokio_otp::OrderedTree;

fn main() {
    let tree = OrderedTree::new();
    let _copy = tree.clone();
}
