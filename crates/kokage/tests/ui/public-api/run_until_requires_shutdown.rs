use std::{future::pending, time::Duration};

use kokage::{Restart, host::RunnableActor};

fn host(actor: &RunnableActor) {
    let _run = actor.run_until(pending::<()>(), Restart::never(), Duration::from_secs(1));
}

fn main() {}
