use kokage::{
    AttachedChild, AttachedChildIdentity,
    host::ChildSpec,
};

fn main() {
    let _ = ChildSpec::attachment::<()>;
}
