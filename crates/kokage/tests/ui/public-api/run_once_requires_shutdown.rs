use std::{future::pending, time::Duration};

use kokage::raw::ActorHost;

fn host(actor: ActorHost) {
    let _run = actor.run_once(pending::<()>(), Duration::from_secs(1));
}

fn main() {}
