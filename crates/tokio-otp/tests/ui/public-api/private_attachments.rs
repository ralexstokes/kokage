use tokio_supervisor::{AttachedChild, AttachedChildIdentity, ChildSpec, SupervisorHandle};

fn old_attachment_bridges(spec: ChildSpec, handle: &SupervisorHandle) {
    let _ = spec.attachment(());
    let _ = handle.attached_children::<()>();
    let _: Option<AttachedChild<()>> = None;
    let _: Option<AttachedChildIdentity> = None;
}

fn main() {}
