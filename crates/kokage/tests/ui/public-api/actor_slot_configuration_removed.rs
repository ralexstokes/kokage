use std::time::Duration;

use kokage::{ActorSlot, MailboxMode, RestartMode, Shutdown};

fn message_size(message: &String) -> usize {
    message.len()
}

fn main() {
    let _ = ActorSlot::<String>::new("capacity").mailbox_capacity(1);
    let _ = ActorSlot::<String>::new("mailbox").mailbox(MailboxMode::conflate());
    let _ = ActorSlot::<String>::new("size").message_size(message_size);
    let _ = ActorSlot::<String>::new("restart").restart(RestartMode::Always);
    let _ = ActorSlot::<String>::new("shutdown")
        .shutdown(Shutdown::graceful_for(Duration::from_secs(1)));
}
