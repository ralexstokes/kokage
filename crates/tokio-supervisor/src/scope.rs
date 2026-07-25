//! Supervisor scope kinds and kind-gated control operations.

pub use crate::builder::ScopeKind;

/// A runtime membership operation that may be unsupported by a scope kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ControlOperation {
    /// Add a task child.
    AddChild,
    /// Add a nested supervisor child.
    AddSupervisor,
    /// Remove a child or nested supervisor.
    RemoveChild,
}

impl std::fmt::Display for ControlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operation = match self {
            Self::AddChild => "add_child",
            Self::AddSupervisor => "add_supervisor",
            Self::RemoveChild => "remove_child",
        };
        f.write_str(operation)
    }
}

impl std::fmt::Display for ScopeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Ordered => "ordered",
            Self::Dynamic => "dynamic",
        };
        f.write_str(kind)
    }
}
