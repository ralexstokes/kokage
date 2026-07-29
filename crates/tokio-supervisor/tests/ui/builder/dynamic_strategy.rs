use tokio_supervisor::{Strategy, Supervisor};

fn main() {
    let _ = Supervisor::dynamic().strategy(Strategy::RestForOne);
}
