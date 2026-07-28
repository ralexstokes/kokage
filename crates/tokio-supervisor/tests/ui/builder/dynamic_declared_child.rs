use tokio_supervisor::{ChildSpec, Supervisor};

fn main() {
    let _ = Supervisor::dynamic().child(ChildSpec::task("worker", |_| async { Ok(()) }));
}
