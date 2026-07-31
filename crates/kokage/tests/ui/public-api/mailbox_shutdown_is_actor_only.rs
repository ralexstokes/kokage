use kokage::{DynamicTree, MailboxShutdown, SubtreeSpec, TaskSpec};

fn main() {
    let _ = TaskSpec::new("task", |_| async { Ok(()) })
        .mailbox_shutdown(MailboxShutdown::Discard);
    let _ = SubtreeSpec::from(DynamicTree::new()).mailbox_shutdown(MailboxShutdown::Drain);
}
