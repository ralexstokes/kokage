//! Supervisor scope kinds.

pub use crate::builder::ScopeKind;

impl std::fmt::Display for ScopeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Ordered => "ordered",
            Self::Dynamic => "dynamic",
        };
        f.write_str(kind)
    }
}
