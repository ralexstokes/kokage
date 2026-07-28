use tokio_otp::SupervisionTree;

fn main() {
    let _ = SupervisionTree::<true>::Dynamic;
    let _ = SupervisionTree::<false>::Ordered;
}
