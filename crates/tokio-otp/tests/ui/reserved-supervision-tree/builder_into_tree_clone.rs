use tokio_otp::Runtime;

fn main() {
    let tree = Runtime::builder().into_tree();
    let _copy = tree.clone();
}
